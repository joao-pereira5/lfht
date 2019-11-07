#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <lfht.h>
#include <mr.h>

#if LFHT_DEBUG

#include <stdio.h>
#include <assert.h>

#define MAX_NODES 2
#define HASH_SIZE 1

#else

#define MAX_NODES 5
#define HASH_SIZE 4

#endif

enum ntype {HASH, ANS};

struct lfht_node;

struct lfht_node_hash {
	int size;
	int hash_pos;
	struct lfht_node *prev;
	struct lfht_node * _Atomic array[0];
};

struct lfht_node_ans {
	size_t hash;
	void *value;
	struct lfht_node * _Atomic next;
};

struct lfht_node {
	enum ntype type;
	union {
		struct lfht_node_hash hash;
		struct lfht_node_ans ans;
	};
};


void search_remove(
		struct lfht_node *hnode,
		size_t hash);

struct lfht_node *search_insert(
		struct lfht_node *hnode,
		size_t hash,
		void *value);

void adjust_chain_nodes(
		struct lfht_node *cnode,
		struct lfht_node *hnode);

void adjust_node(
		struct lfht_node *cnode,
		struct lfht_node *hnode);

void *search_node(
		struct lfht_node *hnode,
		size_t hash);

struct lfht_node *create_hash_node(
		int size,
		int hash_pos,
		struct lfht_node *prev);

// debug search
#if LFHT_DEBUG

void *debug_search_chain(
		struct lfht_node *cnode,
		struct lfht_node *hnode,
		size_t hash);

void *debug_search_hash(
		struct lfht_node *hnode,
		size_t hash);

#endif

//interface

struct lfht_head *init_lfht(int max_threads){
	struct lfht_head *head = malloc(sizeof(struct lfht_head));
	head->entry_hash = create_hash_node(HASH_SIZE, 0, NULL);
	head->thread_array = init_mr(max_threads);
	head->max_threads = max_threads;
	return head;
}

int lfht_init_thread(struct lfht_head *head)
{
	return mr_thread_acquire(head->thread_array, head->max_threads);
}

void lfht_end_thread(struct lfht_head *head, int thread_id)
{
	return mr_thread_release(head->thread_array, thread_id);
}

void *lfht_search(
		struct lfht_head *head,
		size_t hash,
		int thread_id)
{
	return search_node(head->entry_hash, hash);
}

struct lfht_node *lfht_insert(
		struct lfht_head *head,
		size_t hash,
		void *value,
		int thread_id)
{
	return search_insert(
			head->entry_hash,
			hash,
			value);
}

void lfht_remove(
		struct lfht_head *head,
		size_t hash,
		int thread_id)
{
	return search_remove(
			head->entry_hash,
			hash);
}

//debug interface

#if LFHT_DEBUG

void *lfht_debug_search(
		struct lfht_head *head,
		size_t hash,
		int thread_id)
{
	return debug_search_hash(head->entry_hash, hash);
}

#endif

//auxiliary

struct lfht_node *create_ans_node(
		size_t hash,
		void *value,
		struct lfht_node *next)
{
	struct lfht_node *node = malloc(sizeof(struct lfht_node));
	node->type = ANS;
	node->ans.hash = hash;
	node->ans.value = value;
	atomic_init(&(node->ans.next), next);
	return node;
}

struct lfht_node *create_hash_node(
		int size,
		int hash_pos,
		struct lfht_node *prev)
{
	struct lfht_node *node = malloc(
			sizeof(struct lfht_node) + (1<<size)*sizeof(struct lfht_node *));
	node->type = HASH;
	node->hash.size = size;
	node->hash.hash_pos = hash_pos;
	node->hash.prev = prev;
	for(int i=0; i < 1<<size; i++){
		atomic_init(&(node->hash.array[i]), node);
	}
	return node;
}

int get_bucket(
		size_t hash,
		int hash_pos,
		int size)
{
	return (hash >> hash_pos) & ((1 << size) - 1);
}

struct lfht_node *valid_ptr(struct lfht_node *next)
{
	return (struct lfht_node *) ((uintptr_t) next & ~1);
}

unsigned get_flag(struct lfht_node *ptr)
{
	return (uintptr_t) ptr & 1;
}

int mark_invalid(struct lfht_node *cnode)
{
	struct lfht_node *expect = valid_ptr(atomic_load_explicit(
				&(cnode->ans.next),
				memory_order_consume));
	while(!atomic_compare_exchange_weak_explicit(
				&(cnode->ans.next),
				&expect,
				(struct lfht_node *)((uintptr_t) expect | 1),
				memory_order_acq_rel,
				memory_order_consume)){
		if((uintptr_t) expect & 1)
			return 0;
	}
	return 1;
}

int force_cas(struct lfht_node *node, struct lfht_node *hash)
{
	struct lfht_node *expect = atomic_load_explicit(
			&(node->ans.next),
			memory_order_consume);
	do{
		if(((uintptr_t) expect & 1) == 1)
			return 0;
	}while(!atomic_compare_exchange_weak_explicit(
				&(node->ans.next),
				&expect,
				hash,
				memory_order_acq_rel,
				memory_order_consume));
	return 1;
}

int find_node(
		size_t hash,
		struct lfht_node **hnode,
		struct lfht_node **nodeptr,
		struct lfht_node * _Atomic **last_valid,
		int *count)
{
	struct lfht_node *iter;
	int pos = get_bucket(
			hash,
			(*hnode)->hash.hash_pos,
			(*hnode)->hash.size);
	if(last_valid){
		*last_valid = &((*hnode)->hash.array[pos]);
		*nodeptr = atomic_load_explicit(
				*last_valid,
				memory_order_consume);
		iter = *nodeptr;
	}
	else{
		iter = atomic_load_explicit(
				&((*hnode)->hash.array[pos]),
				memory_order_consume);
	}
	if(count)
		*count = 0;
	while(iter != *hnode){
		if(iter->type == HASH){
			while(iter->hash.prev != *hnode)
				iter = iter->hash.prev;
			*hnode = iter;
			return find_node(
					hash,
					hnode,
					nodeptr,
					last_valid,
					count);
		}
		struct lfht_node *tmp = atomic_load_explicit(
				&(iter->ans.next),
				memory_order_consume);
		if(!get_flag(tmp)){
			if(iter->ans.hash == hash){
				*nodeptr = iter;
				return 1;
			}
			if(last_valid){
				*last_valid = &(iter->ans.next);
				*nodeptr = valid_ptr(tmp);
				iter = *nodeptr;
			}
			else{
				iter = valid_ptr(tmp);
			}
			if(count)
				(*count)++;
		}
		else{
			iter = valid_ptr(tmp);
		}
	}
	return 0;
}

void make_invisible(struct lfht_node *cnode, struct lfht_node *hnode)
{
	struct lfht_node *iter,
			*valid_after = valid_ptr(atomic_load_explicit(
						&(cnode->ans.next),
						memory_order_consume));
	while(valid_after->type == ANS){
		iter = atomic_load_explicit(
				&(valid_after->ans.next),
				memory_order_consume);
		if(!get_flag(iter))
			break;
		valid_after = valid_ptr(iter);
	}
	iter = valid_after;
	while(iter->type != HASH)
		iter = valid_ptr(atomic_load_explicit(
					&(iter->ans.next),
					memory_order_consume));
	if(iter == hnode){
		int pos = get_bucket(
				cnode->ans.hash,
				hnode->hash.hash_pos,
				hnode->hash.size);
		struct lfht_node * _Atomic *valid_before = &(hnode->hash.array[pos]),
				*valid_before_next = atomic_load_explicit(
						valid_before,
						memory_order_consume);
		iter = valid_before_next;
		while(iter !=cnode && iter->type == ANS){
			struct lfht_node *tmp = atomic_load_explicit(
					&(iter->ans.next),
					memory_order_consume);
			if(!get_flag(tmp)){
				valid_before = &(iter->ans.next);
				valid_before_next = valid_ptr(tmp);
				iter = valid_before_next;
			}
			else{
				iter = valid_ptr(tmp);
			}
		}
		if(iter == cnode){
			if(atomic_compare_exchange_strong_explicit(
						valid_before,
						&valid_before_next,
						valid_after,
						memory_order_acq_rel,
						memory_order_consume))
				return;
			else
				return make_invisible(cnode, hnode);
		}
		else if(iter == hnode){
			return;
		}
	}
	else if(iter->hash.hash_pos < hnode->hash.hash_pos){
		return;
	}
	return make_invisible(cnode, iter);
}

//remove functions

void search_remove(
		struct lfht_node *hnode,
		size_t hash)
{
	struct lfht_node *cnode;
	if(find_node(hash, &hnode, &cnode, NULL, NULL)){
		if(mark_invalid(cnode))
			make_invisible(cnode, hnode);
	}
	return;
}

//insertion functions

struct lfht_node *search_insert(
		struct lfht_node *hnode,
		size_t hash,
		void *value)
{
	struct lfht_node *cnode,
			* _Atomic *last_valid;
	int count;
	if(find_node(hash, &hnode, &cnode, &last_valid, &count))
		return cnode;
	if(count >= MAX_NODES){
		struct lfht_node *new_hash = create_hash_node(
						HASH_SIZE,
						hnode->hash.hash_pos + hnode->hash.size,
						hnode);
		if(atomic_compare_exchange_strong_explicit(
					last_valid,
					&cnode,
					new_hash,
					memory_order_acq_rel,
					memory_order_consume)){
			int pos = get_bucket(
					hash,
					hnode->hash.hash_pos,
					hnode->hash.size);
			adjust_chain_nodes(
					atomic_load_explicit(
						&(hnode->hash.array[pos]),
						memory_order_consume),
					new_hash);
			atomic_store_explicit(
					&(hnode->hash.array[pos]),
					new_hash,
					memory_order_release);
			return search_insert(
					new_hash,
					hash,
					value);
		}
		else{
			free(new_hash);
		}
	}
	else{
		struct lfht_node *new_node = create_ans_node(
				hash,
				value,
				hnode);
		if(atomic_compare_exchange_strong_explicit(
					last_valid,
					&cnode,
					new_node,
					memory_order_acq_rel,
					memory_order_consume))
			return new_node;
		else
			free(new_node);
	}
	return search_insert(
			hnode,
			hash,
			value);
}

//expansion functions

void adjust_chain_nodes(struct lfht_node *cnode, struct lfht_node *hnode)
{
	struct lfht_node *tmp = atomic_load_explicit(
			&(cnode->ans.next),
			memory_order_consume),
			*next = valid_ptr(tmp);
	if(next != hnode)
		adjust_chain_nodes(next, hnode);
	if(!get_flag(tmp)){
		adjust_node(cnode, hnode);
	}
	return;
}

void adjust_node(
		struct lfht_node *cnode,
		struct lfht_node *hnode)
{
	int counter = 0;
	int pos = get_bucket(
			cnode->ans.hash,
			hnode->hash.hash_pos,
			hnode->hash.size);
	struct lfht_node * _Atomic *current_valid = &(hnode->hash.array[pos]),
			*expected_value = valid_ptr(atomic_load_explicit(
					current_valid,
					memory_order_consume)),
			*iter = expected_value;
	while(iter->type == ANS){
		struct lfht_node *tmp = atomic_load_explicit(
				&(iter->ans.next),
				memory_order_consume);
		if(!get_flag(tmp)){
			current_valid = &(iter->ans.next);
			expected_value = valid_ptr(tmp);
			iter = expected_value;
			counter++;
		}
		else{
			iter = valid_ptr(tmp);
		}
	}
	if(iter == hnode){
		if(counter >=MAX_NODES){
			struct lfht_node *new_hash = create_hash_node(
					HASH_SIZE,
					hnode->hash.hash_pos + hnode->hash.size,
					hnode);
			if(atomic_compare_exchange_strong_explicit(
						current_valid,
						&expected_value,
						new_hash,
						memory_order_acq_rel,
						memory_order_consume)){
				adjust_chain_nodes(
						atomic_load_explicit(
							&(hnode->hash.array[pos]),
							memory_order_consume),
						new_hash);
				atomic_store_explicit(
						&(hnode->hash.array[pos]),
						new_hash,
						memory_order_release);
				return adjust_node(
						cnode,
						new_hash);
			}
			else{
				free(new_hash);
			}
		}
		else{
			if(!force_cas(cnode, hnode))
				return;
			if(atomic_compare_exchange_strong_explicit(
						current_valid,
						&expected_value,
						cnode,
						memory_order_acq_rel,
						memory_order_consume)){
				if(get_flag(atomic_load_explicit(
								&(cnode->ans.next),
								memory_order_seq_cst)))
					make_invisible(cnode, hnode);
				return;
			}
		}
		return adjust_node(cnode, hnode);
	}
	while(iter->hash.prev != hnode){
		iter = iter->hash.prev;
	}
	return adjust_node(cnode, iter);
}

// searching functions

void *search_node(
		struct lfht_node *hnode,
		size_t hash)
{
	struct lfht_node *cnode;
	if(find_node(hash, &hnode, &cnode, NULL, NULL))
		return cnode->ans.value;
	else
		return NULL;
}

// debug searching

#if LFHT_DEBUG

void *debug_search_hash(
		struct lfht_node *hnode,
		size_t hash)
{
	int pos = get_bucket(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);
	struct lfht_node *next_node = atomic_load_explicit(
			&(hnode->hash.array[pos]),
			memory_order_seq_cst);
	if(next_node == hnode)
		return NULL;
	else if(next_node->type == HASH)
		return debug_search_hash(next_node, hash);
	else
		return debug_search_chain(next_node, hnode, hash);
}

void *debug_search_chain(
		struct lfht_node *cnode,
		struct lfht_node *hnode,
		size_t hash)
{
	if(cnode->ans.hash == hash){
		if(get_flag(atomic_load_explicit(
						&(cnode->ans.next),
						memory_order_seq_cst)))
			fprintf(stderr, "Invalid node found: %p\n", cnode->ans.value);
		else
			return cnode->ans.value;
	}
	struct lfht_node *next_node = valid_ptr(atomic_load_explicit(
				&(cnode->ans.next),
				memory_order_seq_cst));
	if(next_node == hnode)
		return NULL;
	else if(next_node->type == ANS)
		return debug_search_chain(next_node, hnode, hash);
	while(next_node->hash.prev != hnode)
		next_node = next_node->hash.prev;
	return debug_search_hash(next_node, hash);
}

#endif

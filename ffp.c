#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <ffp.h>
#include <mr.h>

#if FFP_DEBUG

#include <stdio.h>
#include <assert.h>

#define MAX_NODES 2
#define HASH_SIZE 1

#else

#define MAX_NODES 5
#define HASH_SIZE 4

#endif

enum ntype {HASH, ANS};

struct ffp_node;

struct ffp_node_hash {
	int size;
	int hash_pos;
	struct ffp_node *prev;
	struct ffp_node * _Atomic array[0];
};

struct ffp_node_ans {
	unsigned long long hash;
	void *value;
	struct ffp_node * _Atomic next;
};

struct ffp_node {
	enum ntype type;
	union {
		struct ffp_node_hash hash;
		struct ffp_node_ans ans;
	};
};


void search_remove_hash(
		struct ffp_node *hnode,
		unsigned long long hash);

void search_remove_chain(
		struct ffp_node *hnode,
		unsigned long long hash);

struct ffp_node *search_insert_hash(
		struct ffp_node *hnode,
		unsigned long long hash,
		void *value);

struct ffp_node *search_insert_chain(
		struct ffp_node *hnode,
		unsigned long long hash,
		void *value,
		struct ffp_node *cnode,
		struct ffp_node * _Atomic *current_valid,
		struct ffp_node *expected_value,
		int counter);

void adjust_chain_nodes(
		struct ffp_node *cnode,
		struct ffp_node *hnode);

void adjust_node(
		struct ffp_node *cnode,
		struct ffp_node *hnode);

void *search_chain(
		struct ffp_node *cnode,
		struct ffp_node *hnode,
		unsigned long long hash);

void *search_hash(
		struct ffp_node *hnode,
		unsigned long long hash);

struct ffp_node *create_hash_node(
		int size,
		int hash_pos,
		struct ffp_node *prev);

// debug search
#if FFP_DEBUG

void *debug_search_chain(
		struct ffp_node *cnode,
		struct ffp_node *hnode,
		unsigned long long hash);

void *debug_search_hash(
		struct ffp_node *hnode,
		unsigned long long hash);

#endif

//interface

struct ffp_head init_ffp(int max_threads){
	struct ffp_head head;
	head.entry_hash = create_hash_node(HASH_SIZE, 0, NULL);
	head.thread_array = init_mr(max_threads);
	head.max_threads = max_threads;
	return head;
}

int ffp_init_thread(struct ffp_head head)
{
	return mr_thread_acquire(head.thread_array, head.max_threads);
}

void ffp_end_thread(struct ffp_head head, int thread_id)
{
	return mr_thread_release(head.thread_array, thread_id);
}

void *ffp_search(
		struct ffp_head head,
		unsigned long long hash,
		int thread_id)
{
	return search_hash(head.entry_hash, hash);
}

struct ffp_node *ffp_insert(
		struct ffp_head head,
		unsigned long long hash,
		void *value,
		int thread_id)
{
	return search_insert_hash(
			head.entry_hash,
			hash,
			value);
}

void ffp_remove(
		struct ffp_head head,
		unsigned long long hash,
		int thread_id)
{
	return search_remove_hash(
			head.entry_hash,
			hash);
}

//debug interface

#if FFP_DEBUG

void *ffp_debug_search(
		struct ffp_head head,
		unsigned long long hash,
		int thread_id)
{
	return debug_search_hash(head.entry_hash, hash);
}

#endif

//auxiliary

void *ffp_malloc(size_t size)
{
	return malloc(size);
}

void ffp_free(void *ptr)
{
	free(ptr);
}

struct ffp_node *create_ans_node(
		unsigned long long hash,
		void *value,
		struct ffp_node *next)
{
	struct ffp_node *node = ffp_malloc(sizeof(struct ffp_node));
	node->type = ANS;
	node->ans.hash = hash;
	node->ans.value = value;
	atomic_init(&(node->ans.next), next);
	return node;
}

struct ffp_node *create_hash_node(
		int size,
		int hash_pos,
		struct ffp_node *prev)
{
	struct ffp_node *node = ffp_malloc(
			sizeof(struct ffp_node) + (1<<size)*sizeof(struct ffp_node *));
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
		unsigned long long hash,
		int hash_pos,
		int size)
{
	return (hash >> hash_pos) & ((1 << size) - 1);
}

struct ffp_node *valid_ptr(struct ffp_node *next)
{
	return (struct ffp_node *) ((uintptr_t) next & ~1);
}

int mark_invalid(struct ffp_node *cnode)
{
	struct ffp_node *expect = valid_ptr(atomic_load_explicit(
				&(cnode->ans.next),
				memory_order_relaxed));
	while(!atomic_compare_exchange_weak(
				&(cnode->ans.next),
				&expect,
				(struct ffp_node *)((uintptr_t) expect | 1))){
		if((uintptr_t) expect & 1)
			return 0;
	}
	return 1;
}

int is_valid(struct ffp_node *cnode)
{
	return !((uintptr_t) atomic_load_explicit(
				&(cnode->ans.next),
				memory_order_relaxed) & 1);
}

void force_cas(struct ffp_node *node, struct ffp_node *hash)
{
	struct ffp_node *expect = atomic_load_explicit(
			&(node->ans.next),
			memory_order_relaxed);
	do{
		hash = (struct ffp_node*)(((uintptr_t) expect & 1) | (uintptr_t) hash);
	}while(!atomic_compare_exchange_weak(
				&(node->ans.next),
				&expect,
				hash));
}

struct ffp_node *get_next_valid(struct ffp_node *node)
{
	node = valid_ptr(atomic_load_explicit(
				&(node->ans.next),
				memory_order_relaxed));
	if(node->type == HASH || is_valid(node))
		return node;
	else
		return get_next_valid(node);
}

void make_invisible(struct ffp_node *cnode, struct ffp_node *hnode)
{
	struct ffp_node *valid_after = get_next_valid(cnode);
	struct ffp_node *iter = valid_after;
	while(iter->type != HASH)
		iter = valid_ptr(atomic_load_explicit(
					&(iter->ans.next),
					memory_order_relaxed));
	if(iter == hnode){
		int pos = get_bucket(
				cnode->ans.hash,
				hnode->hash.hash_pos,
				hnode->hash.size);
		iter = atomic_load_explicit(
				&(hnode->hash.array[pos]),
				memory_order_relaxed);
		struct ffp_node *valid_before = NULL;
		struct ffp_node *valid_before_next = iter;
		while(iter !=cnode && iter->type == ANS){
			if(is_valid(iter)){
				valid_before = iter;
				valid_before_next = valid_ptr(atomic_load_explicit(
							&(iter->ans.next),
							memory_order_relaxed));
				iter = valid_before_next;
			}
			else{
				iter = valid_ptr(atomic_load_explicit(
							&(iter->ans.next),
							memory_order_relaxed));
			}
		}
		if(iter == cnode){
			if(valid_before == NULL){
				if(atomic_compare_exchange_strong(
							&(hnode->hash.array[pos]),
							&valid_before_next,
							valid_after))
					return;
			}
			else{
				if(atomic_compare_exchange_strong(
							&(valid_before->ans.next),
							&valid_before_next,
							valid_after))
					return;
			}
			return make_invisible(cnode, hnode);
		}
		else if(iter == hnode){
			return;
		}
	}
	return make_invisible(cnode, iter);
}

//remove functions

void search_remove_chain(
		struct ffp_node *hnode,
		unsigned long long hash)
{
	int pos = get_bucket(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);
	struct ffp_node *iter = atomic_load_explicit(
			&(hnode->hash.array[pos]),
			memory_order_relaxed);
	while(iter->type == ANS){
		if(is_valid(iter)){
			if(hash == iter->ans.hash){
				if(mark_invalid(iter)){
					make_invisible(iter, hnode);
				}
				return;
			}
		}
		iter = valid_ptr(atomic_load_explicit(
					&(iter->ans.next),
					memory_order_relaxed));
	}
	if(iter == hnode)
		return;
	while(iter->hash.prev != hnode){
		iter = iter->hash.prev;
	}
	search_remove_hash(iter, hash);
}

void search_remove_hash(
		struct ffp_node *hnode,
		unsigned long long hash)
{
	int pos = get_bucket(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);
	struct ffp_node *entry_node = atomic_load_explicit(
			&(hnode->hash.array[pos]),
			memory_order_relaxed);
	if(entry_node != hnode){
		if(entry_node->type == ANS)
			search_remove_chain(hnode, hash);
		else
			search_remove_hash(entry_node, hash);
	}
}

//insertion functions

struct ffp_node *search_insert_hash(
		struct ffp_node *hnode,
		unsigned long long hash,
		void *value)
{
	int pos = get_bucket(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);
	struct ffp_node *tmp = atomic_load_explicit(
			&(hnode->hash.array[pos]),
			memory_order_relaxed);
	if(hnode == tmp){
		struct ffp_node *hcopy = hnode,
				*new_node = create_ans_node(
						hash,
						value,
						hnode);
		if(atomic_compare_exchange_strong(
					&(hnode->hash.array[pos]),
					&hcopy,
					new_node))
			return new_node;
		else
			ffp_free(new_node);
	}
	if(tmp->type==ANS)
		return search_insert_chain(
				hnode,
				hash,
				value,
				tmp,
				&(hnode->hash.array[pos]),
				tmp,
				0);
	else
		return search_insert_hash(
				tmp,
				hash,
				value);
}

struct ffp_node *search_insert_chain(
		struct ffp_node *hnode,
		unsigned long long hash,
		void *value,
		struct ffp_node *cnode,
		struct ffp_node * _Atomic *current_valid,
		struct ffp_node *expected_value,
		int counter)
{
	if(is_valid(cnode)){
		if(hash == cnode->ans.hash)
			return cnode;
		counter++;
		current_valid = &(cnode->ans.next);
		expected_value = valid_ptr(atomic_load_explicit(
					current_valid,
					memory_order_relaxed));
		cnode = expected_value;
	}
	else{
		cnode = valid_ptr(atomic_load_explicit(
					&(cnode->ans.next),
					memory_order_relaxed));
	}
	if(cnode == hnode){
		if(counter >= MAX_NODES){
			struct ffp_node *new_hash = create_hash_node(
							HASH_SIZE,
							hnode->hash.hash_pos + hnode->hash.size,
							hnode);
			if(atomic_compare_exchange_strong(
						current_valid,
						&expected_value,
						new_hash)){
				int pos = get_bucket(
						hash,
						hnode->hash.hash_pos,
						hnode->hash.size);
				adjust_chain_nodes(
						atomic_load_explicit(
							&(hnode->hash.array[pos]),
							memory_order_relaxed),
						new_hash);
				atomic_store_explicit(
						&(hnode->hash.array[pos]),
						new_hash,
						memory_order_relaxed);
				return search_insert_hash(
						new_hash,
						hash,
						value);
			}
			else{
				ffp_free(new_hash);
			}
		}
		else{
			struct ffp_node *new_node = create_ans_node(
							hash,
							value,
							hnode);
			if(atomic_compare_exchange_strong(
						current_valid,
						&expected_value,
						new_node))
				return new_node;
			else
				ffp_free(new_node);
		}
		return search_insert_hash(
				hnode,
				hash,
				value);
	}
	if(cnode->type==ANS){
		return search_insert_chain(
				hnode,
				hash,
				value,
				cnode,
				current_valid,
				expected_value,
				counter);
	}
	while(cnode->hash.prev != hnode)
		cnode = cnode->hash.prev;
	return search_insert_hash(
			cnode,
			hash,
			value);
}

//expansion functions

void adjust_chain_nodes(struct ffp_node *cnode, struct ffp_node *hnode)
{
	struct ffp_node *next = valid_ptr(atomic_load_explicit(
				&(cnode->ans.next),
				memory_order_relaxed));
	if(next != hnode)
		adjust_chain_nodes(next, hnode);
	if(is_valid(cnode)){
		force_cas(cnode, hnode);
		adjust_node(cnode, hnode);
	}
	return;
}

void adjust_node(
		struct ffp_node *cnode,
		struct ffp_node *hnode)
{
	int counter = 0;
	int pos = get_bucket(
			cnode->ans.hash,
			hnode->hash.hash_pos,
			hnode->hash.size);
	struct ffp_node *current_valid = NULL,
			*expected_value = atomic_load_explicit(
					&(hnode->hash.array[pos]),
					memory_order_relaxed),
			*iter = expected_value;
	while(iter->type == ANS){
		if(is_valid(iter)){
			current_valid = iter;
			expected_value = valid_ptr(atomic_load_explicit(
						&(current_valid->ans.next),
						memory_order_relaxed));
			iter = expected_value;
			counter++;
		}
		else{
			iter = valid_ptr(atomic_load_explicit(
						&(iter->ans.next),
						memory_order_relaxed));
		}
	}
	if(!is_valid(cnode))
		return;
	if(iter == hnode){
		if(current_valid == NULL || counter == 0){
			if(atomic_compare_exchange_strong(
						&(hnode->hash.array[pos]),
						&expected_value,
						cnode)){
				if(!is_valid(cnode))
					make_invisible(cnode, hnode);
				return;
			}
		}
		else if(counter >=MAX_NODES){
			struct ffp_node *new_hash = create_hash_node(
					HASH_SIZE,
					hnode->hash.hash_pos + hnode->hash.size,
					hnode);
			if(atomic_compare_exchange_strong(
						&(current_valid->ans.next),
						&(expected_value),
						new_hash)){
				adjust_chain_nodes(
						atomic_load_explicit(
							&(hnode->hash.array[pos]),
							memory_order_relaxed),
						new_hash);
				atomic_store_explicit(
						&(hnode->hash.array[pos]),
						new_hash,
						memory_order_relaxed);
				return adjust_node(
						cnode,
						new_hash);
			}
			else{
				ffp_free(new_hash);
			}
		}
		else if(atomic_compare_exchange_strong(
					&(current_valid->ans.next),
					&expected_value,
					cnode)){
			if(!is_valid(cnode))
				make_invisible(cnode, hnode);
			return;
		}
		return adjust_node(cnode, hnode);
	}
	while(iter->hash.prev != hnode){
		iter = iter->hash.prev;
	}
	return adjust_node(cnode, iter);
}

// searching functions

void *search_hash(
		struct ffp_node *hnode,
		unsigned long long hash)
{
	int pos = get_bucket(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);
	struct ffp_node *next_node = atomic_load_explicit(
			&(hnode->hash.array[pos]),
			memory_order_relaxed);
	if(next_node == hnode)
		return NULL;
	else if(next_node->type == HASH)
		return search_hash(next_node, hash);
	else
		return search_chain(next_node, hnode, hash);
}

void *search_chain(
		struct ffp_node *cnode,
		struct ffp_node *hnode,
		unsigned long long hash)
{
	if(cnode->ans.hash == hash && is_valid(cnode))
		return cnode->ans.value;
	struct ffp_node *next_node = valid_ptr(atomic_load_explicit(
				&(cnode->ans.next),
				memory_order_relaxed));
	if(next_node == hnode)
		return NULL;
	else if(next_node->type == ANS)
		return search_chain(next_node, hnode, hash);
	while(next_node->hash.prev != hnode)
		next_node = next_node->hash.prev;
	return search_hash(next_node, hash);
}

// debug searching

#if FFP_DEBUG

void *debug_search_hash(
		struct ffp_node *hnode,
		unsigned long long hash)
{
	int pos = get_bucket(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);
	struct ffp_node *next_node = atomic_load_explicit(
			&(hnode->hash.array[pos]),
			memory_order_relaxed);
	if(next_node == hnode)
		return NULL;
	else if(next_node->type == HASH)
		return debug_search_hash(next_node, hash);
	else
		return debug_search_chain(next_node, hnode, hash);
}

void *debug_search_chain(
		struct ffp_node *cnode,
		struct ffp_node *hnode,
		unsigned long long hash)
{
	if(cnode->ans.hash == hash){
		if(!is_valid(cnode))
			fprintf(stderr, "Invalid node found: %p\n", cnode->ans.value);
		else
			return cnode->ans.value;
	}
	struct ffp_node *next_node = valid_ptr(atomic_load_explicit(
				&(cnode->ans.next),
				memory_order_relaxed));
	if(next_node == hnode)
		return NULL;
	else if(next_node->type == ANS)
		return debug_search_chain(next_node, hnode, hash);
	while(next_node->hash.prev != hnode)
		next_node = next_node->hash.prev;
	return debug_search_hash(next_node, hash);
}

#endif

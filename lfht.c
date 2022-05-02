#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <hp.h>
#include <lfht.h>

enum ntype {HASH, LEAF, FREEZE, UNFREEZE};

struct lfht_node;

static HpDomain* dom;

// a node of the trie
// "size" = chunk size
// on level hash_pos/size of the tree
// hash_pos is incremented in chunks (see: get_bucket_index())
// 2^size = length of "array" of buckets
struct lfht_node_hash {
	int size;
	int hash_pos;
	_Atomic(struct lfht_node *) prev;
	_Atomic(struct lfht_node *) array[0];
};

// key-value pair node
struct lfht_node_leaf {
	size_t hash;
	void *value;
	_Atomic(struct lfht_node *) next;
};

struct lfht_node {
	enum ntype type;
	union {
		struct lfht_node_hash hash;
		struct lfht_node_leaf leaf;
	};
};

// private functions

void search_remove(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash);

void search_insert(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash,
		void *value);

int compress(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node **target,
		size_t hash);

int unfreeze(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *target,
		size_t hash);

void abort_compress(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *target,
		struct lfht_node *freeze,
		struct lfht_node *head,
		_Atomic(struct lfht_node *) *atomic_bucket);

int help_expansion(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		struct lfht_node *target,
		size_t hash);

int expand(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node **new_hash,
		struct lfht_node *hnode,
		size_t hash,
		_Atomic(struct lfht_node *) *tail_nxt_ptr);

int adjust_chain_nodes(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		struct lfht_node *head);

void adjust_node(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *cnode,
		struct lfht_node *nxt,
		struct lfht_node *hnode);

void *search_node(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash);

struct lfht_node *create_hash_node(
		int size,
		int hash_pos,
		struct lfht_node *prev);

struct lfht_node *get_next(
		struct lfht_node *node);

struct lfht_node *get_prev(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *node);

struct lfht_node *valid_ptr(struct lfht_node *next);

struct lfht_node *invalid_ptr(struct lfht_node *next);

unsigned is_invalid(struct lfht_node *ptr);

unsigned is_compression_node(struct lfht_node *node);

unsigned is_compressed(struct lfht_node *node);

unsigned is_empty(struct lfht_node *hnode);

// public functions
// defined by the header API

struct lfht_head *init_lfht(int max_threads) {
	return init_lfht_explicit(
			max_threads,
			ROOT_HASH_SIZE,
			HASH_SIZE,
			MAX_NODES);
}

struct lfht_head *init_lfht_explicit(
		int max_threads,
		int root_hash_size,
		int hash_size,
		int max_chain_nodes) {
	struct lfht_head *lfht = malloc(sizeof(struct lfht_head));

	int k = max_chain_nodes + 3;
	dom = hp_create(k, 10000);
	//dom = hp_create(k, max_threads * k + max_threads * k / 2);
	//dom = hp_create(k, 0);

	lfht->entry_hash = create_hash_node(root_hash_size, 0, NULL);
	lfht->max_threads = max_threads;
	lfht->root_hash_size = root_hash_size;
	lfht->hash_size = hash_size;
	lfht->max_chain_nodes = max_chain_nodes;
#if LFHT_STATS
	lfht->stats = (_Atomic(struct lfht_stats*) *)
		malloc(max_threads*sizeof(_Atomic(struct lfht_stats*)));

	for(int i = 0; i < lfht->max_threads; i++) {
		atomic_init(&(lfht->stats[i]), NULL);
	}

	lfht->hazard_pointers = (HpRecord**)malloc(lfht->max_threads * sizeof(HpRecord*));
	for(int i = 0; i < lfht->max_threads; i++) {
		lfht->hazard_pointers[i] = NULL;
	}

	if(lfht->max_threads <= 1) {
		lfht_init_thread(lfht, 0);
	}
#endif
	return lfht;
}

void free_lfht(struct lfht_head *lfht) {
	hp_destroy();
	free(lfht->hazard_pointers);
	lfht->hazard_pointers = NULL;

#if LFHT_STATS
	for(int i = 0; i < lfht->max_threads; i++) {
		free(lfht->stats[i]);
	}
	free(lfht->stats);
#endif
}

int lfht_init_thread(struct lfht_head *lfht, int thread_id)
{
	if(!lfht->hazard_pointers[thread_id]) {
		lfht->hazard_pointers[thread_id] = hp_alloc(dom, thread_id);
	}

#if LFHT_STATS
	size_t stats_size = CACHE_SIZE * ((sizeof(struct lfht_stats) / CACHE_SIZE) + 1);
	struct lfht_stats *s = (struct lfht_stats *) aligned_alloc(CACHE_SIZE, stats_size);
	s->compression_counter = 0;
	s->compression_rollback_counter = 0;
	s->expansion_counter = 0;
	s->unfreeze_counter = 0;
	s->freeze_counter = 0;
	s->max_retry_counter = 0;
	s->operations = 0;
	s->inserts = 0;
	s->removes = 0;
	s->searches = 0;
	s->api_calls = 0;
	s->max_depth = 0;
	s->paths = 0;
	s->lookups = 0;

	for(int i = 0; i < lfht->max_threads; i++) {
		struct lfht_stats *expect = NULL;
		if (atomic_compare_exchange_weak_explicit(
				&(lfht->stats[i]),
				&expect,
				s,
				memory_order_acq_rel,
				memory_order_consume)) {
			return i;
		}
	}

	free(s);
	return -1;
#else
	return 0;
#endif
}

void lfht_end_thread(struct lfht_head *lfht, int thread_id)
{
	hp_release(dom, lfht->hazard_pointers[thread_id]);
	lfht->hazard_pointers[thread_id] = NULL;

#if LFHT_STATS
	struct lfht_stats *s = lfht->stats[thread_id];
	clock_gettime(CLOCK_MONOTONIC_RAW, &(s->term));
#endif
}

void *lfht_search(
		struct lfht_head *lfht,
		size_t hash,
		int thread_id)
{
#if LFHT_STATS
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->api_calls++;
	stats->searches++;
#endif
	return search_node(
			lfht,
			thread_id,
			lfht->entry_hash,
			hash);
}

void lfht_insert(
		struct lfht_head *lfht,
		size_t hash,
		void *value,
		int thread_id)
{
#if LFHT_STATS
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->api_calls++;
	stats->inserts++;
#endif
	search_insert(
			lfht,
			thread_id,
			lfht->entry_hash,
			hash,
			value);
}

void lfht_remove(
		struct lfht_head *lfht,
		size_t hash,
		int thread_id)
{
#if LFHT_STATS
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->api_calls++;
	stats->removes++;
#endif
	search_remove(
			lfht,
			thread_id,
			lfht->entry_hash,
			hash);
}

// auxiliary functions

struct lfht_node *create_freeze_node(
		struct lfht_node *next)
{
	struct lfht_node *node = malloc(sizeof(struct lfht_node));
	node->type = FREEZE;
	node->leaf.hash = 0;
	node->leaf.value = NULL;

	atomic_init(&(node->leaf.next), next);

	return node;
}

struct lfht_node *create_unfreeze_node(
		struct lfht_node *next)
{
	struct lfht_node *node = malloc(sizeof(struct lfht_node));
	node->type = UNFREEZE;
	node->leaf.hash = 0;
	node->leaf.value = NULL;

	atomic_init(&(node->leaf.next), next);

	return node;
}

struct lfht_node *create_leaf_node(
		size_t hash,
		void *value,
		struct lfht_node *next)
{
	struct lfht_node *node = malloc(sizeof(struct lfht_node));
	node->type = LEAF;
	node->leaf.hash = hash;
	node->leaf.value = value;

	atomic_init(&(node->leaf.next), next);

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
	for(int i=0; i < 1<<size; i++) {
		atomic_init(&(node->hash.array[i]), node);
	}
	return node;
}

// for the current hash level, return the chunk of the
// hash which indexes the bucket array (of size 2^w)
int get_bucket_index(
		size_t hash,
		int hash_pos,
		int size)
{
	// hash_pos does not increment 1 by 1
	// it increments with the chunk size
	return (hash >> hash_pos) & ((1 << size) - 1);
}

_Atomic(struct lfht_node *) *get_atomic_bucket(
		size_t hash,
		struct lfht_node *hnode)
{
	int pos = get_bucket_index(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);

	return &(hnode->hash.array[pos]);
}

// release-consume order get next
struct lfht_node *get_next(
		struct lfht_node *node)
{
	//return atomic_load_explicit(
	//		&(node->leaf.next),
	//		memory_order_consume);

	// avoiding hazards during compress operation WIP
	struct lfht_node* nxt = atomic_load_explicit(
			&(node->leaf.next),
			memory_order_seq_cst);

	return nxt;
}

struct lfht_node *get_prev(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode)
{
	struct lfht_node* prev = atomic_load_explicit(
			&(hnode->hash.prev),
			memory_order_seq_cst);

	if(!prev) {
		return lfht->entry_hash;
	}

	HpRecord* hp = lfht->hazard_pointers[thread_id];

	hp_protect(dom, hp, prev);
	if(prev != atomic_load_explicit(
			&(hnode->hash.prev),
			memory_order_seq_cst)) {
		// hazard pointer not safe
		return lfht->entry_hash;
	}

	// prev protected successfully
	return prev;
}

// we do not use an "is_valid" field.
// to verify if a node is valid, the least significant bit
// of the *next field is used (the address, not the node it
// points to).
//
// validating a node using the mask ~1:
// 0xFFFF & ~1 === 0xFFFF & 0xFFFE = 0xFFFE
//
// invalidating a node:
// 0xFFFE | 1 === 0xFFFE | 0x0001 = 0xFFFF
//
// arg: the *next address of the node we want to invalidate
// return: pointer masked with "valid"
struct lfht_node *valid_ptr(struct lfht_node *next)
{
	return (struct lfht_node *) ((uintptr_t) next & ~1);
}

struct lfht_node *invalid_ptr(struct lfht_node *next)
{
	return (struct lfht_node *) ((uintptr_t) next | 1);
}

// mask address to get the valid/invalid bit flag
// 
// arg: the *next address of the node
// return: the least significant bit of *next
unsigned is_invalid(struct lfht_node *ptr)
{
	return (uintptr_t) ptr & 1;
}

unsigned is_compressed(struct lfht_node *hnode)
{
	_Atomic(struct lfht_node *) *prev =
		&(hnode->hash.prev);

	struct lfht_node *parent = atomic_load_explicit(
			prev,
			memory_order_consume);

	return !parent;
}

unsigned is_compression_node(struct lfht_node *node)
{
	return node->type == FREEZE || node->type == UNFREEZE;
}

unsigned is_empty(struct lfht_node *hnode)
{
	for(int i = 0; i < (1<<hnode->hash.size); i++) {
		struct lfht_node *head;
		_Atomic(struct lfht_node *) *nxt_atomic_bucket =
			&(hnode->hash.array[i]);

		head = atomic_load_explicit(
				nxt_atomic_bucket,
				memory_order_consume);

		if(head != hnode) {
			// bucket not empty
			return 0;
		}
	}

	return 1;
}

// retries CAS until it succeeds
int force_cas(struct lfht_node *node, struct lfht_node *replace)
{
	struct lfht_node *expect = get_next(node);

	if(is_invalid(expect)) {
		return 0;
	}

	if(expect == replace) {
		// value already in place
		return 1;
	}

	while(!atomic_compare_exchange_weak_explicit(
				&(node->leaf.next),
				&expect,
				replace,
				memory_order_acq_rel,
				memory_order_consume)) {
		if(is_invalid(expect)) {
			return 0;
		}
	}
	return 1;
}

// hnode -> parent hash node of lnode
// lnode -> will point to the target node, if it exists
//
// for use w/ insertion():
//   tail -> tail of the chain
//   count -> length of the collision chain
//
// returns: 0/1 success
int lookup(
		struct lfht_head *lfht,
		int thread_id,
		size_t hash,
		struct lfht_node **hnode,
		struct lfht_node **lnode,
		_Atomic(struct lfht_node *) **tail,
		unsigned int *count)
{
#if LFHT_STATS
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->lookups++;
	stats->operations++;
#endif

	HpRecord* hp = lfht->hazard_pointers[thread_id];

start: ;
	// HP.protect uses a ring buffer to protect references
	// the following protection prevents the *hnode reference from
	// getting overwritten from its HP after a few retrials
	if (*hnode != lfht->entry_hash) {
		hp_protect(dom, hp, *hnode);
	}

#if LFHT_STATS
	stats->max_retry_counter++;
#endif

traversal: ;

	// load bucket entry

	_Atomic(struct lfht_node *) *bucket =
		get_atomic_bucket(hash, *hnode);

	_Atomic(struct lfht_node *) *prev = bucket;

	struct lfht_node *iter = atomic_load_explicit(
			prev,
			memory_order_consume);

	hp_protect(dom, hp, iter);
	// is hazard pointer safe?
	if(iter != atomic_load_explicit(
				prev,
				memory_order_consume)) {
		// hazard pointer unprotected, retry
		goto start;
	}

	struct lfht_node *head = iter;
	*lnode = head;

	if(is_compression_node(head)) {
		// skip compression node
		struct lfht_node *nxt = get_next(head);

		if(nxt == *hnode) {
			// hash node compressed completely
			*hnode = get_prev(lfht, thread_id, *hnode);
			goto traversal;
		}

		hp_protect(dom, hp, nxt);
		if(get_next(head) != nxt) {
			goto start;
		}

		*hnode = nxt;
		compress(lfht, thread_id, hnode, hash);
		goto start;
	}

	if(tail) {
		*tail = prev;
	}

	if(count) {
		*count = iter->type == LEAF ? 1 : 0;
	}

	// traverse chain (tail points back to hash node)

	while(iter != *hnode) {
#if LFHT_STATS
		stats->paths++;
#endif

		if(iter->type == HASH) {
			// onto next tree level
			*hnode = iter;
			goto traversal;
		}

		// traverse collision chain
		struct lfht_node *nxt_iter = get_next(iter);
		struct lfht_node* nxt = valid_ptr(nxt_iter);

		if(is_invalid(nxt_iter)) {
			// remove iter

			struct lfht_node *expect = iter;
			if(!atomic_compare_exchange_strong_explicit(
						prev,
						&expect,
						nxt,
						memory_order_acq_rel,
						memory_order_consume)) {
				// failed to detach, retry
				goto start;
			}

			hp_retire(dom, thread_id, hp, iter);

			// check if we should compress

			if(prev == bucket && nxt == *hnode) {
				hp_protect(dom, hp, *hnode);

				// bucket was left empty
				if(compress(lfht, thread_id, hnode, hash)) {
					goto start;
				}
			}

			if(iter->leaf.hash == hash) {
				return 0;
			}

		} else {
			// iter is a valid node

			if(iter->leaf.hash == hash) {
				*lnode = iter;
				return 1;
			}
			*lnode = nxt_iter;

			prev = &(iter->leaf.next);
			if(tail) {
				*tail = prev;
			}

			if(count) {
				(*count)++;
			}
		}

		hp_protect(dom, hp, nxt);
		if(nxt != atomic_load_explicit(
					prev,
					memory_order_seq_cst)) {
			goto start;
		}

		if(nxt->type == HASH && nxt != *hnode) {
			// expansion detected
			help_expansion(
					lfht,
					thread_id,
					*hnode,
					nxt,
					hash);

			goto start;
		}

		// advance chain
		iter = nxt;
	}

	return 0;
}

// remove functions

void search_remove(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash)
{

	HpRecord* hp = lfht->hazard_pointers[thread_id];

start: ;
	// start from root
	// both nodes protected by HPs from the lookup function
	struct lfht_node *cnode;
	if(!lookup(lfht, thread_id, hash, &hnode, &cnode, NULL, NULL)) {
		return;
	}

	struct lfht_node *nxt = get_next(cnode);

	if(is_invalid(nxt)) {
		return;
	}

	hp_protect(dom, hp, nxt);
	if(nxt != get_next(cnode)) {
		goto start;
	}

	if(nxt->type == HASH && nxt != hnode) {
		// expansion detected
		help_expansion(
				lfht,
				thread_id,
				hnode,
				nxt,
				hash);

		goto start;
	}

	// mark invalid
	if(!atomic_compare_exchange_weak_explicit(
				&(cnode->leaf.next),
				&nxt,
				invalid_ptr(nxt),
				memory_order_acq_rel,
				memory_order_consume)
			&& !is_invalid(nxt)) {
		// new node inserted in front of cnode
		// retry
		goto start;
	}

	// this will detach any invalid nodes
	lookup(lfht, thread_id, hash, &hnode, &cnode, NULL, NULL);
}

// insertion functions

void search_insert(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash,
		void *value)
{
#if LFHT_STATS
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->operations++;
#endif
	HpRecord* hp = lfht->hazard_pointers[thread_id];

start: ;
#if LFHT_STATS
	stats->max_retry_counter++;
#endif

	struct lfht_node *cnode;
	 _Atomic(struct lfht_node*) *tail;
	unsigned int count;

	if(lookup(lfht, thread_id, hash, &hnode, &cnode, &tail, &count)) {
		// node already inserted
		return;
	}

	//if(cnode->type == FREEZE &&
	//		!unfreeze(lfht, thread_id, hnode, hash)) {
	if(cnode->type == FREEZE) {
		goto start;
	}

	// expand hash level
	if(count >= lfht->max_chain_nodes) {
		struct lfht_node *new_hash;
		// add new level to tail of chain
		if(expand(lfht, thread_id, &new_hash, hnode, hash, tail)) {
			// level added
			hnode = new_hash;
		}

		goto start;
	}

	// insert new node in current bucket
	struct lfht_node *new_node = create_leaf_node(
			hash,
			value,
			hnode);
	if(atomic_compare_exchange_strong_explicit(
				tail,
				&cnode,
				new_node,
				memory_order_acq_rel,
				memory_order_consume)) {
		return;
	}

	free(new_node);
	goto start;
}

// compression functions

int compress(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node **hnode,
		size_t hash)
{
	HpRecord* hp = lfht->hazard_pointers[thread_id];

#if LFHT_STATS
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->operations++;
#endif

start: ;

#if LFHT_STATS
	stats->max_retry_counter++;
#endif

	struct lfht_node *target = *hnode;

	struct lfht_node *prev_hash = atomic_load_explicit(
			&(target->hash.prev),
			memory_order_consume);

	if(prev_hash == NULL || !is_empty(target)) {
		// we cannot compress the root hash or a non empty hash
		return 0;
	}

	struct lfht_node *freeze = create_freeze_node(target);
	struct lfht_node *expect;

	// get_prev() tries to protect the previous hash node
	prev_hash = get_prev(lfht, thread_id, target);

	_Atomic(struct lfht_node *) *atomic_bucket =
		get_atomic_bucket(hash, prev_hash);

	// try to place freeze node in front of bucket,
	// pointing to target hash
	expect = target;
	if(!atomic_compare_exchange_strong_explicit(
				atomic_bucket,
				&expect,
				freeze,
				memory_order_acq_rel,
				memory_order_consume)) {
		free(freeze);

		hp_protect(dom, hp, expect);
		if(expect != atomic_load_explicit(
					atomic_bucket,
					memory_order_consume)) {
			goto start;
		}

		if(!is_compression_node(expect)) {
			return 1;
		}

		// aid compression
		freeze = expect;
	}
#if LFHT_STATS
	else {
		stats->freeze_counter++;
	}
#endif

	// freeze empty buckets
	for(int i = 0; i < (1<<target->hash.size); i++) {
		_Atomic(struct lfht_node *) *nxt_atomic_bucket =
			&(target->hash.array[i]);

		expect = target;
		if(!atomic_compare_exchange_strong_explicit(
					nxt_atomic_bucket,
					&expect,
					freeze,
					memory_order_acq_rel,
					memory_order_consume) && expect != freeze) {
			// bucket not empty
			abort_compress(lfht, thread_id, target, freeze, freeze, atomic_bucket);
			goto start;
		}
	}

	// this prevents the parent hash node from being referenced
	// after it has been reclaimed
	atomic_store_explicit(
			&(target->hash.prev),
			NULL,
			memory_order_seq_cst);

	// removing hash from the trie (commit)
	expect = freeze;
	if(atomic_compare_exchange_strong_explicit(
				atomic_bucket,
				&expect,
				prev_hash,
				memory_order_acq_rel,
				memory_order_consume)) {
		// this thread was the one to commit compression
		// it is responsible for freeing memory
		hp_retire(dom, thread_id, hp, freeze);
		hp_retire(dom, thread_id, hp, target);

#if LFHT_STATS
		// compressed level
		stats->compression_counter++;
#endif
	}

	// try to compress previous level
	*hnode = prev_hash;
	goto start;
}

// notify compressing thread to abort compression
int unfreeze(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *target,
		size_t hash)
{
	HpRecord* hp = lfht->hazard_pointers[thread_id];

	_Atomic(struct lfht_node *) *atomic_bucket =
		get_atomic_bucket(hash, target->hash.prev);

	struct lfht_node *head = atomic_load_explicit(
			atomic_bucket,
			memory_order_consume);

	if(head == target) {
		// compression rolled back successfully
		return 1;
	}

	if(head->type == UNFREEZE) {
		return get_next(head) == target;
	}

	if(head->type != FREEZE || get_next(head) != target) {
		// level already removed
		return 0;
	}

	// head is freeze node

	struct lfht_node *unfreeze = create_unfreeze_node(target);
	hp_protect(dom, hp, unfreeze);

	// try to place unfreeze node in front of bucket,
	// pointing to freeze node
	if(!atomic_compare_exchange_strong_explicit(
				atomic_bucket,
				&head,
				unfreeze,
				memory_order_acq_rel,
				memory_order_consume)) {
		// already compressed, unfrozen or removed
		free(unfreeze);

		if(head == target) {
			// compression rolled back successfully
			return 1;
		}

		if(head->type == UNFREEZE) {
			return get_next(head) == target;
		}

		return 0;
	}

	// we removed the freeze node, so we should retire it
	hp_retire(dom, thread_id, hp, head);

#if LFHT_STATS
	struct lfht_stats* stats = atomic_load_explicit(&(lfht->stats[thread_id]), memory_order_relaxed);
	stats->unfreeze_counter++;
#endif
	return 1;
}

// target -> hash level we wanted to compress
// atomic_bucket -> atomic object of the previous level
//   bucket pointing to our target
void abort_compress(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *target,
		struct lfht_node *freeze,
		struct lfht_node *head,
		_Atomic(struct lfht_node *) *atomic_bucket)
{
	HpRecord* hp = lfht->hazard_pointers[thread_id];

	struct lfht_node *expect;

	// point all buckets to target
	for(int i = 0; i < (1<<target->hash.size); i++) {
		_Atomic(struct lfht_node *) *nxt_atomic_bucket = &(target->hash.array[i]);
		expect = freeze;

		// ignoring CAS failure
		// just making sure all buckets point to target
		// if it fails, they already do point to target or have a chain
		atomic_compare_exchange_strong_explicit(
				nxt_atomic_bucket,
				&expect,
				target,
				memory_order_acq_rel,
				memory_order_consume);
	}

	// commit level by removing compression bridge node
	if(atomic_compare_exchange_strong_explicit(
			atomic_bucket,
			&head,
			target,
			memory_order_acq_rel,
			memory_order_consume)) {
		// retire compression node
		hp_retire(dom, thread_id, hp, head);
	}

#if LFHT_STATS
	struct lfht_stats* stats = atomic_load_explicit(&(lfht->stats[thread_id]), memory_order_relaxed);
	stats->compression_rollback_counter++;
#endif
}

// expansion functions

int help_expansion(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		struct lfht_node *target,
		size_t hash)
{
	if(target->type != HASH || target == hnode) {
		return 0;
	}

	HpRecord* hp = lfht->hazard_pointers[thread_id];

	// recursive helping
	int pos = get_bucket_index(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);

	// move all nodes of chain to new level
	_Atomic(struct lfht_node *) *parent_bucket =
		&(hnode->hash.array[pos]);

	struct lfht_node *head = atomic_load_explicit(
			parent_bucket,
			memory_order_consume);

	// check if expansion has terminated
	if(head == target) {
		return 1;
	}

	// check if new hash node has been compressed
	if(is_compressed(target)) {
		return 0;
	}

	adjust_chain_nodes(
			lfht,
			thread_id,
			target,
			head);

	int res = atomic_compare_exchange_strong_explicit(
			parent_bucket,
			&head,
			target,
			memory_order_acq_rel,
			memory_order_consume);

#if LFHT_STATS
	if(res) {
		struct lfht_stats* stats = atomic_load_explicit(&(lfht->stats[thread_id]), memory_order_relaxed);
		stats->expansion_counter++;
		int level = target->hash.hash_pos / target->hash.size;
		if(stats->max_depth < level) {
			stats->max_depth = level;
		}
	}
#endif

	return res;
}

int expand(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node **new_hash,
		struct lfht_node *hnode,
		size_t hash,
		_Atomic(struct lfht_node *) *tail_nxt_ptr)
{
	HpRecord* hp = lfht->hazard_pointers[thread_id];

	struct lfht_node *exp = hnode;

	*new_hash = create_hash_node(
			lfht->hash_size,
			hnode->hash.hash_pos + hnode->hash.size,
			hnode);

	hp_protect(dom, hp, *new_hash);

	// add new hash level to tail of chain
	if(atomic_compare_exchange_strong_explicit(
				tail_nxt_ptr,
				&exp,
				*new_hash,
				memory_order_acq_rel,
				memory_order_consume)) {
		return help_expansion(
				lfht,
				thread_id,
				hnode,
				*new_hash,
				hash);
	}

	// failed
	free(*new_hash);

	// protect new hash node
	hp_protect(dom, hp, exp);
	struct lfht_node* tail = atomic_load_explicit(
			tail_nxt_ptr,
			memory_order_consume);

	if(tail != exp || exp->type != HASH || exp == hnode) {
		return 0;
	}

	*new_hash = exp;

	return help_expansion(
			lfht,
			thread_id,
			hnode,
			exp,
			hash);
}

int adjust_chain_nodes(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		struct lfht_node *iter)
{
	HpRecord* hp = lfht->hazard_pointers[thread_id];

	if(iter->type != LEAF) {
		// reached tail
		return iter == hnode;
	}

	struct lfht_node *nxt_iter = get_next(iter);
	struct lfht_node *nxt = valid_ptr(nxt_iter);

	if(!adjust_chain_nodes(
			lfht,
			thread_id,
			hnode,
			nxt)) {
		// not the hash we wished to expand
		return 0;
	}

	if(is_invalid(nxt_iter)) {
		// skip invalid nodes
		return 1;
	}

	// iter is valid
	adjust_node(lfht, thread_id, iter, nxt_iter, hnode);
	return 1;
}

void adjust_node(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *cnode,
		struct lfht_node *nxt,
		struct lfht_node *hnode)
{
#if LFHT_STATS
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->operations++;
#endif

start: ;

#if LFHT_STATS
	stats->max_retry_counter++;
#endif

	unsigned int count = 0;
	size_t hash = cnode->leaf.hash;

	_Atomic(struct lfht_node *) *current_valid =
		get_atomic_bucket(hash, hnode);

	struct lfht_node *expect = valid_ptr(atomic_load_explicit(
				current_valid,
				memory_order_consume));

	struct lfht_node *iter = expect;

	// find tail of target bucket on new hash level
	while(iter->type == LEAF) {
		struct lfht_node *nxt_ptr = get_next(iter);

		if(is_invalid(nxt_ptr)) {
			// skip invalid node
			iter = valid_ptr(nxt_ptr);
			continue;
		}

		if(cnode == iter) {
			// already inserted
			return;
		}

		current_valid = &(iter->leaf.next);
		expect = valid_ptr(nxt_ptr);
		iter = expect;
		count++;
	}

	if(iter != hnode) {
		hnode = iter;
		goto start;
	}

	// point node to newer level
	if(!atomic_compare_exchange_strong_explicit(
				&(cnode->leaf.next),
				&nxt,
				hnode,
				memory_order_acq_rel,
				memory_order_consume) && is_invalid()) {
		return;
	}

	// inserting node in chain of the newer level
	if(!atomic_compare_exchange_strong_explicit(
				current_valid,
				&expect,
				cnode,
				memory_order_acq_rel,
				memory_order_consume)) {
		// insertion failed
		goto start;
	}
}

// searching functions

void *search_node(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash)
{
	struct lfht_node *cnode;
	HpRecord* hp = lfht->hazard_pointers[thread_id];
	int found = lookup(lfht, thread_id, hash, &hnode, &cnode, NULL, NULL);
	void* result = cnode->leaf.value;

	if(found) {
		return result;
	}
	return NULL;
}

#if HP_STATS
#include <stdio.h>

void print_hp_stats() {
	HpStats* stats = hp_gather_stats();
	printf("HP API calls: %lu\n", stats->api_calls);
	printf("HP reclaimed nodes: %lu\n", stats->reclaimed);
	printf("HP retired nodes: %lu\n", stats->retired);
}
#endif


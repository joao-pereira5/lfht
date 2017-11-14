struct ffp_node *init_ffp();

void search_remove_hash(
		struct ffp_node *hnode,
		unsigned long long hash);


struct ffp_node *search_insert_hash(
		struct ffp_node *hnode,
		unsigned long long hash,
		void *value);

void *search_hash(struct ffp_node *hnode, unsigned long long hash);

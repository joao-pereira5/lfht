#include <ffp.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>

#if FFP_DEBUG

#include <assert.h>

#endif

unsigned long long limit_sf,
                   limit_r,
                   limit_i;

int test_size,
    n_threads;

struct ffp_head head;

void *prepare_worker(void *entry_point)
{
	int thread_id = ffp_init_thread(head);
	for(int i=0; i<test_size/n_threads; i++){
		unsigned long long value = nrand48(entry_point);
		if(value < limit_r)
			ffp_insert(head, value, (void*)value, thread_id);
	}
	ffp_end_thread(head, thread_id);
	return NULL;
}

void *bench_worker(void *entry_point)
{
	int thread_id = ffp_init_thread(head);
	int thread_limit = test_size/n_threads;
	for(int i=0; i<thread_limit; i++){
		unsigned long long value = nrand48(entry_point);
		if(value < limit_sf){
#if FFP_DEBUG
			assert((unsigned long long)ffp_search(head, value, thread_id)==value);
#else
			if((unsigned long long)ffp_search(head, value, thread_id)!=value)
				fprintf(stderr, "failiure: item not match %lld\n", value);
#endif
		}
		else if(value < limit_r){
			ffp_remove(head, value, thread_id);
		}
		else if(value < limit_i){
			ffp_insert(head, value, (void*)value, thread_id);
		}
		else{
#if FFP_DEBUG
			assert(ffp_search(head, value, thread_id) == NULL);
#else
			if(ffp_search(head, value, thread_id)!=NULL)
				fprintf(stderr, "failiure: found item %lld\n", value);
#endif
		}
	}
	ffp_end_thread(head, thread_id);
	return NULL;
}
#if FFP_DEBUG
void *test_worker(void *entry_point)
{
	int thread_id = ffp_init_thread(head);
	for(int i=0; i<test_size/n_threads; i++){
		unsigned long long value = nrand48(entry_point);
		if(value < limit_sf){
			assert((unsigned long long)ffp_debug_search(head, value, thread_id)==value);
		}
		else if(value < limit_r){
			assert(ffp_debug_search(head, value, thread_id)==NULL);
		}
		else if(value < limit_i){
			assert((unsigned long long)ffp_debug_search(head, value, thread_id)==value);
		}
		else{
			assert(ffp_debug_search(head, value, thread_id)==NULL);
		}
	}
	ffp_end_thread(head, thread_id);
	return NULL;
}
#endif

int main(int argc, char **argv)
{
	if(argc < 7){
		printf("usage: bench <threads> <nodes> <inserts> <removes> <searches found> <searches not found>\nAdd 't' at the end to verify integrity\n");
		return -1;
	}
	printf("preparing data.\n");
	n_threads = atoi(argv[1]);
	test_size = atoi(argv[2]);
	unsigned long long inserts = atoi(argv[3]),
	                   removes = atoi(argv[4]),
	                   searches_found = atoi(argv[5]),
	                   searches_not_found = atoi(argv[6]),
	                   total = inserts + removes + searches_found + searches_not_found;
	limit_sf = RAND_MAX*searches_found/total;
	limit_r = limit_sf + RAND_MAX*removes/total;
	limit_i = limit_r + RAND_MAX*inserts/total;
	struct timespec start_monoraw,
			end_monoraw,
			start_process,
			end_process;
	double time;
	pthread_t *threads = malloc(n_threads*sizeof(pthread_t));
	unsigned short **seed = malloc(n_threads*sizeof(unsigned short*));
	head = init_ffp(n_threads);
	for(int i=0; i<n_threads; i++)
		seed[i] = aligned_alloc(64, 64);
	if(limit_r!=0){
		for(int i=0; i<n_threads; i++){
			seed[i][0] = i;
			seed[i][1] = i;
			seed[i][2] = i;
			pthread_create(&threads[i], NULL, prepare_worker, seed[i]);
		}
		for(int i=0;i<n_threads; i++){
			pthread_join(threads[i], NULL);
		}
	}
	printf("starting test\n");
	clock_gettime(CLOCK_MONOTONIC_RAW, &start_monoraw);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start_process);
	for(int i=0; i<n_threads; i++){
		seed[i][0] = i;
		seed[i][1] = i;
		seed[i][2] = i;
		pthread_create(&threads[i], NULL, bench_worker, seed[i]);
	}
	for(int i=0; i<n_threads; i++)
		pthread_join(threads[i], NULL);
	clock_gettime(CLOCK_MONOTONIC_RAW, &end_monoraw);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end_process);
	time = end_monoraw.tv_sec - start_monoraw.tv_sec + ((end_monoraw.tv_nsec - start_monoraw.tv_nsec)/1000000000.0);
	printf("Real time: %lf\n", time);
	time = end_process.tv_sec - start_process.tv_sec + ((end_process.tv_nsec - start_process.tv_nsec)/1000000000.0);
	printf("Process time: %lf\n", time);

#if FFP_DEBUG
	if(argc == 8 && argv[7][0]=='t'){
		for(int i=0; i<n_threads; i++){
			seed[i][0] = i;
			seed[i][1] = i;
			seed[i][2] = i;
			pthread_create(&threads[i], NULL, test_worker, seed[i]);
		}
		for(int i=0; i<n_threads; i++){
			pthread_join(threads[i], NULL);
		}
		printf("Correct!\n");
	}
#endif
	return 0;
}

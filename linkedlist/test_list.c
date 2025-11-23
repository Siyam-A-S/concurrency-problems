#include "lock_free_list.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <string.h>

#define _POSIX_C_SOURCE 200809L

#define NUM_THREADS 8
#define OPERATIONS_PER_THREAD 12500  // 8 threads * 12500 = 100000 ops
#define TEST_DURATION_SECONDS 10      // 10 second stress test
#define INSERT_RATIO 0.5  // 50% inserts, 50% deletes

typedef struct {
    lock_free_list_t* list;
    int thread_id;
    uint64_t insert_count;
    uint64_t delete_count;
    uint64_t search_count;
    uint64_t insert_success;
    uint64_t delete_success;
    double elapsed_time;
} thread_data_t;

// Get current time in milliseconds
double get_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000.0) + (tv.tv_usec / 1000.0);
}

void* worker_thread(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    unsigned int seed = time(NULL) + data->thread_id;
    
    double start_time = get_time_ms();
    
    for (uint64_t i = 0; i < OPERATIONS_PER_THREAD; i++) {
        int value = rand_r(&seed) % 10000;  // Random value between 0-9999
        double op_type = (double)rand_r(&seed) / RAND_MAX;
        
        if (op_type < INSERT_RATIO) {
            // Insert operation
            data->insert_count++;
            if (list_insert(data->list, value)) {
                data->insert_success++;
            }
        } else if (op_type < 0.9) {
            // Delete operation
            data->delete_count++;
            if (list_delete(data->list, value)) {
                data->delete_success++;
            }
        } else {
            // Search operation
            data->search_count++;
            list_search(data->list, value);
        }
        
        // Small yield to simulate more realistic workload
        if (i % 100 == 0) {
            usleep(1);
        }
    }
    
    double end_time = get_time_ms();
    data->elapsed_time = end_time - start_time;
    
    return NULL;
}

void* stress_test_thread(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    unsigned int seed = (unsigned int)(time(NULL) + data->thread_id * 1000);
    
    time_t start_time = time(NULL);
    
    while (1) {
        time_t current_time = time(NULL);
        if (difftime(current_time, start_time) >= TEST_DURATION_SECONDS) {
            break;
        }
        
        int value = (int)(rand_r(&seed) % 50000);
        int op_type = rand_r(&seed) % 100;
        
        if (op_type < 50) {
            // Insert
            if (list_insert(data->list, value)) {
                __atomic_add_fetch(&data->insert_success, 1, __ATOMIC_RELAXED);
            }
            __atomic_add_fetch(&data->insert_count, 1, __ATOMIC_RELAXED);
        } else if (op_type < 90) {
            // Delete
            if (list_delete(data->list, value)) {
                __atomic_add_fetch(&data->delete_success, 1, __ATOMIC_RELAXED);
            }
            __atomic_add_fetch(&data->delete_count, 1, __ATOMIC_RELAXED);
        } else {
            // Search
            list_search(data->list, value);
            __atomic_add_fetch(&data->search_count, 1, __ATOMIC_RELAXED);
        }
    }
    
    return NULL;
}

void run_performance_test() {
    lock_free_list_t list;
    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];
    
    printf("\n╔════════════════════════════════════════════════════════════╗\n");
    printf("║           LOCK-FREE LINKED LIST PERFORMANCE TEST          ║\n");
    printf("╚════════════════════════════════════════════════════════════╝\n\n");
    
    // Initialize list
    if (list_init(&list) != 0) {
        fprintf(stderr, "Failed to initialize list\n");
        return;
    }
    
    printf("Configuration:\n");
    printf("  Threads: %d\n", NUM_THREADS);
    printf("  Operations per thread: %d\n", OPERATIONS_PER_THREAD);
    printf("  Total operations: %d\n", NUM_THREADS * OPERATIONS_PER_THREAD);
    printf("  Insert ratio: %.0f%%\n", INSERT_RATIO * 100);
    printf("  Delete ratio: %.0f%%\n", 40.0);
    printf("  Search ratio: %.0f%%\n\n", 10.0);
    
    printf("Starting test...\n");
    
    double total_start = get_time_ms();
    
    // Create threads
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].list = &list;
        thread_data[i].thread_id = i;
        thread_data[i].insert_count = 0;
        thread_data[i].delete_count = 0;
        thread_data[i].search_count = 0;
        thread_data[i].insert_success = 0;
        thread_data[i].delete_success = 0;
        thread_data[i].elapsed_time = 0;
        
        pthread_create(&threads[i], NULL, worker_thread, &thread_data[i]);
    }
    
    // Wait for all threads
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    double total_end = get_time_ms();
    double total_time = total_end - total_start;
    
    // Calculate statistics
    uint64_t total_inserts = 0;
    uint64_t total_deletes = 0;
    uint64_t total_searches = 0;
    uint64_t total_insert_success = 0;
    uint64_t total_delete_success = 0;
    
    printf("\n╔════════════════════════════════════════════════════════════╗\n");
    printf("║                      TEST RESULTS                          ║\n");
    printf("╚════════════════════════════════════════════════════════════╝\n\n");
    
    printf("Per-Thread Statistics:\n");
    for (int i = 0; i < NUM_THREADS; i++) {
        total_inserts += thread_data[i].insert_count;
        total_deletes += thread_data[i].delete_count;
        total_searches += thread_data[i].search_count;
        total_insert_success += thread_data[i].insert_success;
        total_delete_success += thread_data[i].delete_success;
        
        printf("  Thread %d:\n", i);
        printf("    Inserts: %lu (success: %lu, %.1f%%)\n", 
               thread_data[i].insert_count, 
               thread_data[i].insert_success,
               thread_data[i].insert_count > 0 ? 
               (100.0 * thread_data[i].insert_success / thread_data[i].insert_count) : 0);
        printf("    Deletes: %lu (success: %lu, %.1f%%)\n", 
               thread_data[i].delete_count,
               thread_data[i].delete_success,
               thread_data[i].delete_count > 0 ?
               (100.0 * thread_data[i].delete_success / thread_data[i].delete_count) : 0);
        printf("    Searches: %lu\n", thread_data[i].search_count);
        printf("    Time: %.2f ms\n", thread_data[i].elapsed_time);
        printf("\n");
    }
    
    uint64_t total_ops = total_inserts + total_deletes + total_searches;
    
    printf("Overall Statistics:\n");
    printf("  Total operations: %lu\n", total_ops);
    printf("  Total inserts: %lu (success: %lu, %.1f%%)\n", 
           total_inserts, total_insert_success,
           total_inserts > 0 ? (100.0 * total_insert_success / total_inserts) : 0);
    printf("  Total deletes: %lu (success: %lu, %.1f%%)\n", 
           total_deletes, total_delete_success,
           total_deletes > 0 ? (100.0 * total_delete_success / total_deletes) : 0);
    printf("  Total searches: %lu\n", total_searches);
    printf("  Final list size: %lu\n", list_size(&list));
    printf("  Total time: %.2f ms (%.2f seconds)\n", total_time, total_time / 1000.0);
    printf("  Throughput: %.0f ops/second\n", (total_ops * 1000.0) / total_time);
    printf("  Operations per minute: %.0f\n", (total_ops * 60000.0) / total_time);
    
    // Cleanup
    list_destroy(&list);
    
    printf("\n✓ List destroyed successfully\n\n");
}

void run_stress_test() {
    lock_free_list_t list;
    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];
    
    printf("\n╔════════════════════════════════════════════════════════════╗\n");
    printf("║         LOCK-FREE LINKED LIST STRESS TEST (%ds)           ║\n", TEST_DURATION_SECONDS);
    printf("╚════════════════════════════════════════════════════════════╝\n\n");
    
    if (list_init(&list) != 0) {
        fprintf(stderr, "Failed to initialize list\n");
        return;
    }
    
    printf("Configuration:\n");
    printf("  Threads: %d\n", NUM_THREADS);
    printf("  Duration: %d seconds\n", TEST_DURATION_SECONDS);
    printf("  Target: 100,000+ operations per minute\n\n");
    
    printf("Starting stress test...\n");
    
    time_t start = time(NULL);
    
    // Create threads
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].list = &list;
        thread_data[i].thread_id = i;
        thread_data[i].insert_count = 0;
        thread_data[i].delete_count = 0;
        thread_data[i].search_count = 0;
        thread_data[i].insert_success = 0;
        thread_data[i].delete_success = 0;
        thread_data[i].elapsed_time = 0;
        pthread_create(&threads[i], NULL, stress_test_thread, &thread_data[i]);
    }
    
    // Wait for all threads
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    time_t end = time(NULL);
    double duration = difftime(end, start);
    
    // Calculate statistics
    uint64_t total_inserts = 0;
    uint64_t total_deletes = 0;
    uint64_t total_searches = 0;
    uint64_t total_insert_success = 0;
    uint64_t total_delete_success = 0;
    
    for (int i = 0; i < NUM_THREADS; i++) {
        total_inserts += thread_data[i].insert_count;
        total_deletes += thread_data[i].delete_count;
        total_searches += thread_data[i].search_count;
        total_insert_success += thread_data[i].insert_success;
        total_delete_success += thread_data[i].delete_success;
    }
    
    uint64_t total_ops = total_inserts + total_deletes + total_searches;
    double ops_per_minute = (total_ops * 60.0) / duration;
    
    printf("\n╔════════════════════════════════════════════════════════════╗\n");
    printf("║                    STRESS TEST RESULTS                     ║\n");
    printf("╚════════════════════════════════════════════════════════════╝\n\n");
    
    printf("Statistics:\n");
    printf("  Duration: %.0f seconds\n", duration);
    printf("  Total operations: %lu\n", total_ops);
    printf("  Inserts: %lu (success: %lu)\n", total_inserts, total_insert_success);
    printf("  Deletes: %lu (success: %lu)\n", total_deletes, total_delete_success);
    printf("  Searches: %lu\n", total_searches);
    printf("  Final list size: %lu\n", list_size(&list));
    printf("\n");
    printf("Performance:\n");
    printf("  Operations per second: %.0f\n", total_ops / duration);
    printf("  Operations per minute: %.0f\n", ops_per_minute);
    printf("  Target achieved: %s\n", ops_per_minute >= 100000 ? "✓ YES" : "✗ NO");
    
    list_destroy(&list);
    printf("\n✓ Test completed successfully\n\n");
}

int main(int argc, char* argv[]) {
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════╗\n");
    printf("║      LOCK-FREE LINKED LIST WITH CUSTOM CAS OPERATIONS     ║\n");
    printf("║              High-Performance Concurrent List              ║\n");
    printf("╚════════════════════════════════════════════════════════════╝\n");
    
    if (argc > 1 && strcmp(argv[1], "stress") == 0) {
        run_stress_test();
    } else {
        run_performance_test();
        
        printf("Tip: Run './test_list stress' for 60-second stress test\n\n");
    }
    
    return 0;
}

#include "semaphore.h"
#include <stdio.h>

int semaphore_init(semaphore_t *sem, int initial_count) {
    if (sem == NULL) {
        fprintf(stderr, "Error: NULL semaphore pointer\n");
        return -1;
    }
    
    sem->count = initial_count;
    
    // Initialize mutex
    if (pthread_mutex_init(&sem->mutex, NULL) != 0) {
        fprintf(stderr, "Error: Failed to initialize mutex\n");
        return -1;
    }
    
    // Initialize condition variable
    if (pthread_cond_init(&sem->condition, NULL) != 0) {
        fprintf(stderr, "Error: Failed to initialize condition variable\n");
        pthread_mutex_destroy(&sem->mutex);
        return -1;
    }
    
    return 0;
}

void semaphore_wait(semaphore_t *sem) {
    pthread_mutex_lock(&sem->mutex);
    
    // Wait while count is 0 or negative (resource not available)
    while (sem->count <= 0) {
        pthread_cond_wait(&sem->condition, &sem->mutex);
    }
    
    // Decrement the counter (acquire resource)
    sem->count--;
    
    pthread_mutex_unlock(&sem->mutex);
}

void semaphore_signal(semaphore_t *sem) {
    pthread_mutex_lock(&sem->mutex);
    
    // Increment the counter (release resource)
    sem->count++;
    
    // Wake up one waiting thread
    pthread_cond_signal(&sem->condition);
    
    pthread_mutex_unlock(&sem->mutex);
}

int semaphore_destroy(semaphore_t *sem) {
    if (sem == NULL) {
        fprintf(stderr, "Error: NULL semaphore pointer\n");
        return -1;
    }
    
    // Destroy condition variable
    if (pthread_cond_destroy(&sem->condition) != 0) {
        fprintf(stderr, "Error: Failed to destroy condition variable\n");
        return -1;
    }
    
    // Destroy mutex
    if (pthread_mutex_destroy(&sem->mutex) != 0) {
        fprintf(stderr, "Error: Failed to destroy mutex\n");
        return -1;
    }
    
    return 0;
}

#ifndef SEMAPHORE_H
#define SEMAPHORE_H

#include <pthread.h>

/**
 * Custom semaphore implementation using mutex and condition variable
 * Inspired by OS - Three Easy Pieces (http://pages.cs.wisc.edu/~remzi/OSTEP/)
 */
typedef struct {
    int count;                      // Semaphore counter
    pthread_mutex_t mutex;          // Protects the count
    pthread_cond_t condition;       // Condition variable for waiting
} semaphore_t;

/**
 * Initialize a semaphore with an initial count
 * @param sem Pointer to the semaphore
 * @param initial_count Initial value of the semaphore counter
 * @return 0 on success, -1 on failure
 */
int semaphore_init(semaphore_t *sem, int initial_count);

/**
 * Wait (P operation / down operation)
 * Decrements the semaphore counter. If counter becomes negative, blocks.
 * @param sem Pointer to the semaphore
 */
void semaphore_wait(semaphore_t *sem);

/**
 * Signal (V operation / up operation)
 * Increments the semaphore counter and wakes up a waiting thread if any.
 * @param sem Pointer to the semaphore
 */
void semaphore_signal(semaphore_t *sem);

/**
 * Destroy a semaphore and free its resources
 * @param sem Pointer to the semaphore
 * @return 0 on success, -1 on failure
 */
int semaphore_destroy(semaphore_t *sem);

#endif // SEMAPHORE_H

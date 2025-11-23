#ifndef LOCK_FREE_LIST_H
#define LOCK_FREE_LIST_H

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>

/**
 * Node structure for lock-free linked list
 * Uses tagged pointers to handle ABA problem
 */
typedef struct node {
    int data;
    _Atomic(struct node*) next;
    _Atomic(bool) marked;  // Logical deletion flag
} node_t;

/**
 * Lock-free linked list structure
 */
typedef struct {
    _Atomic(node_t*) head;
    _Atomic(uint64_t) size;
} lock_free_list_t;

/**
 * Custom compare-and-swap wrapper for pointer types
 * Uses C11 atomic operations
 */
static inline bool cas_ptr(_Atomic(node_t*)* ptr, node_t** expected, node_t* desired) {
    return atomic_compare_exchange_strong(ptr, expected, desired);
}

/**
 * Custom compare-and-swap for boolean values
 */
static inline bool cas_bool(_Atomic(bool)* ptr, bool* expected, bool desired) {
    return atomic_compare_exchange_strong(ptr, expected, desired);
}

/**
 * Initialize a lock-free linked list
 * @param list Pointer to the list structure
 * @return 0 on success, -1 on failure
 */
int list_init(lock_free_list_t* list);

/**
 * Insert a value into the list (sorted order maintained)
 * Lock-free operation using CAS
 * @param list Pointer to the list
 * @param value Value to insert
 * @return true on success, false on failure
 */
bool list_insert(lock_free_list_t* list, int value);

/**
 * Delete a value from the list
 * Uses logical deletion followed by physical deletion
 * @param list Pointer to the list
 * @param value Value to delete
 * @return true if found and deleted, false otherwise
 */
bool list_delete(lock_free_list_t* list, int value);

/**
 * Search for a value in the list
 * @param list Pointer to the list
 * @param value Value to search for
 * @return true if found, false otherwise
 */
bool list_search(lock_free_list_t* list, int value);

/**
 * Get the current size of the list
 * @param list Pointer to the list
 * @return Number of elements in the list
 */
uint64_t list_size(lock_free_list_t* list);

/**
 * Print the list contents (for debugging)
 * @param list Pointer to the list
 */
void list_print(lock_free_list_t* list);

/**
 * Destroy the list and free all nodes
 * @param list Pointer to the list
 */
void list_destroy(lock_free_list_t* list);

/**
 * Helper function to find a node and its predecessor
 * Handles marked nodes and physical deletion
 */
bool list_find(lock_free_list_t* list, int value, node_t** pred_ptr, node_t** curr_ptr);

#endif // LOCK_FREE_LIST_H

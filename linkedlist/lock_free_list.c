#include "lock_free_list.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/**
 * Create a new node with given value
 */
static node_t* create_node(int value) {
    node_t* node = (node_t*)malloc(sizeof(node_t));
    if (node == NULL) {
        return NULL;
    }
    
    node->data = value;
    atomic_init(&node->next, NULL);
    atomic_init(&node->marked, false);
    
    return node;
}

int list_init(lock_free_list_t* list) {
    if (list == NULL) {
        return -1;
    }
    
    // Create sentinel head node with minimum value
    node_t* head = create_node(INT32_MIN);
    if (head == NULL) {
        return -1;
    }
    
    // Create sentinel tail node with maximum value
    node_t* tail = create_node(INT32_MAX);
    if (tail == NULL) {
        free(head);
        return -1;
    }
    
    atomic_store(&head->next, tail);
    atomic_init(&list->head, head);
    atomic_init(&list->size, 0);
    
    return 0;
}

bool list_find(lock_free_list_t* list, int value, node_t** pred_ptr, node_t** curr_ptr) {
    node_t* pred;
    node_t* curr;
    node_t* succ;
    bool marked;
    
try_again:
    pred = atomic_load(&list->head);
    if (pred == NULL) return false;
    
    curr = atomic_load(&pred->next);
    if (curr == NULL) return false;
    
    while (true) {
        // Read current node's next pointer and mark bit
        succ = atomic_load(&curr->next);
        if (succ == NULL) {
            *pred_ptr = pred;
            *curr_ptr = curr;
            return (curr->data == value);
        }
        marked = atomic_load(&curr->marked);
        
        // If current node is marked for deletion, physically remove it
        while (marked) {
            // Try to swing predecessor's next pointer to successor
            node_t* expected = curr;
            if (!cas_ptr(&pred->next, &expected, succ)) {
                // CAS failed, retry from beginning
                goto try_again;
            }
            
            // Successfully removed marked node, free it
            free(curr);
            
            curr = succ;
            if (curr == NULL) goto try_again;
            succ = atomic_load(&curr->next);
            if (succ == NULL) {
                *pred_ptr = pred;
                *curr_ptr = curr;
                return (curr->data == value);
            }
            marked = atomic_load(&curr->marked);
        }
        
        // Check if we've found the right position
        if (curr->data >= value) {
            *pred_ptr = pred;
            *curr_ptr = curr;
            return (curr->data == value);
        }
        
        // Move forward
        pred = curr;
        curr = succ;
    }
}

bool list_insert(lock_free_list_t* list, int value) {
    node_t* new_node = create_node(value);
    if (new_node == NULL) {
        return false;
    }
    
    while (true) {
        node_t* pred;
        node_t* curr;
        
        // Find the position to insert
        if (list_find(list, value, &pred, &curr)) {
            // Value already exists, don't insert duplicate
            free(new_node);
            return false;
        }
        
        // Set new node's next to current
        atomic_store(&new_node->next, curr);
        
        // Try to insert using CAS
        node_t* expected = curr;
        if (cas_ptr(&pred->next, &expected, new_node)) {
            // Successfully inserted
            atomic_fetch_add(&list->size, 1);
            return true;
        }
        
        // CAS failed, retry
    }
}

bool list_delete(lock_free_list_t* list, int value) {
    while (true) {
        node_t* pred;
        node_t* curr;
        
        // Find the node to delete
        if (!list_find(list, value, &pred, &curr)) {
            // Value not found
            return false;
        }
        
        node_t* succ = atomic_load(&curr->next);
        
        // Logical deletion: mark the node
        bool expected_mark = false;
        if (!cas_bool(&curr->marked, &expected_mark, true)) {
            // Already marked by another thread, retry
            continue;
        }
        
        // Physical deletion: try to swing predecessor's next pointer
        node_t* expected = curr;
        if (cas_ptr(&pred->next, &expected, succ)) {
            // Successfully removed, free the node
            free(curr);
        }
        // If CAS fails, another thread will remove it during find()
        
        atomic_fetch_sub(&list->size, 1);
        return true;
    }
}

bool list_search(lock_free_list_t* list, int value) {
    node_t* curr = atomic_load(&list->head);
    curr = atomic_load(&curr->next);
    
    while (curr->data < value) {
        curr = atomic_load(&curr->next);
    }
    
    return (curr->data == value && !atomic_load(&curr->marked));
}

uint64_t list_size(lock_free_list_t* list) {
    return atomic_load(&list->size);
}

void list_print(lock_free_list_t* list) {
    node_t* curr = atomic_load(&list->head);
    curr = atomic_load(&curr->next);
    
    printf("List contents: [");
    bool first = true;
    
    while (curr != NULL && curr->data != INT32_MAX) {
        bool marked = atomic_load(&curr->marked);
        if (!marked) {
            if (!first) printf(", ");
            printf("%d", curr->data);
            first = false;
        }
        curr = atomic_load(&curr->next);
    }
    
    printf("] (size: %lu)\n", list_size(list));
}

void list_destroy(lock_free_list_t* list) {
    node_t* curr = atomic_load(&list->head);
    
    while (curr != NULL) {
        node_t* next = atomic_load(&curr->next);
        free(curr);
        curr = next;
    }
    
    atomic_store(&list->head, NULL);
    atomic_store(&list->size, 0);
}

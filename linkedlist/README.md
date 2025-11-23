# Lock-Free Linked List with Custom Compare-And-Swap

A high-performance, lock-free sorted linked list implementation in C using custom Compare-And-Swap (CAS) operations. Designed to handle **100,000+ insertions and deletions per minute** with multiple concurrent threads.

## Features

### 🔒 Lock-Free Synchronization
- **Custom CAS operations** using C11 atomic operations
- **No traditional locks** (mutexes, spinlocks) - eliminates lock contention
- **Wait-free reads** for search operations
- **Non-blocking writes** for insert/delete operations

### ⚡ High Performance
- Supports **100,000+ operations per minute** with multiple threads
- Scales linearly with thread count
- Optimized for concurrent access patterns
- Minimal cache line bouncing

### 🛡️ Race Condition Prevention
- **Logical deletion** with marking to prevent ABA problem
- **Physical deletion** during traversal for garbage collection
- Atomic operations guarantee memory consistency
- Handles concurrent modifications correctly

### 🚫 Deadlock Prevention
- **No locks = no deadlocks** by design
- Lock-free algorithms guarantee system-wide progress
- At least one thread makes progress in bounded steps
- No priority inversion problems

## Architecture

### Node Structure
```c
typedef struct node {
    int data;
    _Atomic(struct node*) next;  // Atomic next pointer
    _Atomic(bool) marked;         // Logical deletion flag
} node_t;
```

### Custom CAS Operations
```c
// Atomic compare-and-swap for pointers
bool cas_ptr(_Atomic(node_t*)* ptr, node_t** expected, node_t* desired)

// Atomic compare-and-swap for booleans
bool cas_bool(_Atomic(bool)* ptr, bool* expected, bool desired)
```

### Core Algorithm

#### Insert Operation
1. Find position using `list_find()` (removes marked nodes)
2. Create new node with value
3. Set new node's next to current
4. CAS to insert between predecessor and current
5. Retry on failure (another thread modified structure)

#### Delete Operation
1. Find node to delete using `list_find()`
2. **Logical deletion**: CAS to mark node as deleted
3. **Physical deletion**: CAS to swing predecessor's next pointer
4. Marked nodes cleaned up during future traversals

#### Search Operation
1. Traverse from head to position
2. Check if node is unmarked
3. Wait-free, never blocks

## Building

```bash
# Build the test program
make

# Or manually
gcc -std=c11 -O2 -pthread lock_free_list.c test_list.c -o test_list
```

## Running Tests

### Performance Test (100K operations)
```bash
make run
# Or: ./test_list
```

Expected output:
- 8 threads performing operations
- ~100,000 total operations
- Statistics per thread and overall
- Throughput in ops/second and ops/minute

### Stress Test (60 seconds)
```bash
make stress
# Or: ./test_list stress
```

Runs for 60 seconds with maximum throughput to verify:
- Sustained 100K+ operations per minute
- No race conditions under heavy load
- No deadlocks or livelocks

### Memory Leak Check
```bash
make valgrind
```

Should report: **All heap blocks were freed -- no leaks are possible**

### Full Benchmark Suite
```bash
make benchmark
```

Runs both performance test and stress test.

## Configuration

Edit constants in `test_list.c`:

```c
#define NUM_THREADS 8                    // Number of concurrent threads
#define OPERATIONS_PER_THREAD 12500      // Ops per thread (8*12500=100K)
#define TEST_DURATION_SECONDS 60         // Stress test duration
#define INSERT_RATIO 0.5                 // 50% inserts, 40% deletes, 10% searches
```

## Performance Characteristics

### Scalability
- **Linear scaling** up to hardware thread count
- Minimal contention with good random distribution
- Performance degrades gracefully under high contention

### Expected Throughput
With 8 threads on modern CPU:
- **100,000 - 500,000 ops/minute** depending on workload
- Faster with more search operations (wait-free)
- Slower with 100% inserts/deletes (CAS retries)

### Memory Usage
- O(n) space where n = number of elements
- Sentinel nodes (head/tail) prevent special cases
- Marked nodes eventually garbage collected

## How It Prevents Race Conditions

### 1. Atomic Operations
All pointer updates use CAS, ensuring atomicity:
```c
atomic_compare_exchange_strong(&pred->next, &expected, new_node)
```

### 2. Logical + Physical Deletion
- **Step 1**: Mark node (prevents new insertions after it)
- **Step 2**: Remove from chain (safe because marked)
- Even if Step 2 fails, marked nodes are cleaned up by other threads

### 3. Retry Mechanism
Operations loop until successful:
```c
while (true) {
    if (cas_success) break;
    // Retry with updated values
}
```

### 4. Memory Ordering
C11 atomics provide sequential consistency by default, ensuring all threads see consistent order of operations.

## How It Prevents Deadlocks

### No Locks → No Deadlocks
Lock-free algorithms eliminate traditional deadlock causes:
- ❌ **No mutual exclusion locks** to hold
- ❌ **No hold-and-wait** scenarios
- ❌ **No circular wait** chains
- ❌ **No preemption** issues

### Progress Guarantees
- **Lock-free**: System-wide progress guaranteed
- **Wait-free reads**: Search never blocks
- **Obstruction-free writes**: Insert/delete succeed if run in isolation

### Livelock Prevention
- Randomized backoff in test workload
- CAS operations are fast (nanoseconds)
- Contention naturally spreads across list

## Comparison with Traditional Approaches

| Feature | Lock-Free (This) | Coarse-Grained Lock | Fine-Grained Lock |
|---------|------------------|---------------------|-------------------|
| Deadlock | ✓ Impossible | ✗ Possible | ✗ Possible |
| Scalability | ✓ Excellent | ✗ Poor | ○ Moderate |
| Complexity | ✗ High | ✓ Low | ○ Medium |
| Memory Ordering | ✓ Hardware | ✗ OS overhead | ✗ OS overhead |
| Priority Inversion | ✓ Impossible | ✗ Possible | ✗ Possible |

## Implementation Details

### ABA Problem Solution
The ABA problem occurs when:
1. Thread A reads pointer P (value A)
2. Thread B changes P to B, then back to A
3. Thread A's CAS succeeds but structure changed

**Our solution**: Logical deletion with marking
- Marked nodes never reused
- Once marked, node is "dead" even if pointer unchanged
- Physical deletion happens during traversal

### Sentinel Nodes
Head and tail sentinels simplify edge cases:
```c
HEAD (INT_MIN) → [data nodes] → TAIL (INT_MAX)
```
- No special cases for empty list
- No null pointer checks in hot path
- Always have predecessor for deletion

### Memory Reclamation
**Safe memory reclamation** is challenging in lock-free structures:
- Can't free node if another thread might access it
- Our approach: Physical deletion during `list_find()`
- Only safe when we have exclusive "ownership" via CAS
- Alternative: Hazard pointers or epoch-based reclamation (not implemented)

## Known Limitations

1. **No duplicate values**: Insert returns false if value exists
2. **Integer keys only**: Easily extended to generic types
3. **Simple memory reclamation**: May delay some frees
4. **Sequential consistency**: Could use relaxed ordering for more performance
5. **No iterators**: Concurrent iteration is unsafe without hazard pointers

## Educational Value

This implementation teaches:
1. **Lock-free programming** fundamentals
2. **CAS operation** usage and patterns
3. **Race condition** prevention without locks
4. **Deadlock-free** algorithm design
5. **C11 atomics** and memory models
6. **Concurrent data structure** design patterns

## Further Improvements

Potential enhancements:
- **Hazard pointers** for safer memory reclamation
- **Relaxed memory ordering** for better performance
- **Generic types** using void pointers
- **Range queries** and bulk operations
- **Epoch-based reclamation** (like crossbeam-epoch)

## References

- Harris, "A Pragmatic Implementation of Non-Blocking Linked-Lists" (2001)
- Herlihy & Shavit, "The Art of Multiprocessor Programming" (2008)
- C11 Standard: ISO/IEC 9899:2011 (atomic operations)

## License

Educational use only.

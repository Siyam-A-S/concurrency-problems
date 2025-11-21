# Dining Philosophers Problem - Educational Implementation

This project implements the classic **Dining Philosophers Problem** using a custom semaphore built from POSIX mutexes and condition variables. This is for educational purposes to understand synchronization primitives.

## Overview

The Dining Philosophers Problem is a classic synchronization problem that illustrates challenges in resource allocation and deadlock avoidance in concurrent systems.

### The Problem

- 5 philosophers sit around a circular table
- Each philosopher alternates between thinking and eating
- 5 forks are placed between philosophers (one between each pair)
- A philosopher needs **both** adjacent forks to eat
- **Challenge**: Avoid deadlock, starvation, and ensure thread safety

## Solution Strategy

This implementation uses **multiple techniques** to prevent deadlock:

1. **Room Semaphore**: Only allows N-1 philosophers (4 out of 5) to attempt picking up forks simultaneously
2. **Binary Semaphores for Forks**: Each fork is protected by a semaphore initialized to 1
3. **Custom Semaphore Implementation**: Built using mutex + condition variable for educational purposes

### Why This Works

By limiting the number of philosophers who can enter the "room" (attempt to pick up forks), we guarantee that at least one philosopher can always acquire both forks, preventing circular wait and thus deadlock.

## Custom Semaphore Implementation

### Components

```c
typedef struct {
    int count;                    // Available resources
    pthread_mutex_t mutex;        // Protects count
    pthread_cond_t condition;     // Blocks/wakes threads
} semaphore_t;
```

### Operations

- **`semaphore_wait()`** (P operation / down):
  - Decrements counter
  - Blocks if counter ≤ 0
  - Uses `pthread_cond_wait()` for blocking

- **`semaphore_signal()`** (V operation / up):
  - Increments counter
  - Wakes one waiting thread with `pthread_cond_signal()`

### Why Build Custom Semaphores?

Modern POSIX provides `sem_t` (from `<semaphore.h>`), but building our own helps understand:
- How mutexes and condition variables work together
- The relationship between semaphores and lower-level primitives
- Critical section management
- Thread signaling mechanisms

## Project Structure

```
.
├── semaphore.h              # Custom semaphore interface
├── semaphore.c              # Custom semaphore implementation
├── dining_philosophers.c    # Main program
├── Makefile                 # Build automation
└── README.md               # This file
```

## Building

```bash
# Build the program
make

# Or manually:
gcc -std=c11 -g -Wall -pthread dining_philosophers.c semaphore.c -o dining_philosophers
```

## Running

```bash
# Run directly
./dining_philosophers

# Or using make
make run
```

### Expected Output

The program displays colorful, timestamped output showing each philosopher's actions:

```
[12:34:56] Philosopher 0 (Socrates): is THINKING 🤔
[12:34:57] Philosopher 0 (Socrates): is HUNGRY 🍽️  - waiting for forks
[12:34:57] Philosopher 0 (Socrates): picking up LEFT fork
[12:34:57] Philosopher 0 (Socrates): picking up RIGHT fork
[12:34:57] Philosopher 0 (Socrates): has BOTH forks and is EATING 🍝
```

## Memory Analysis with Valgrind

```bash
# Check for memory leaks
make valgrind

# Or manually:
valgrind --leak-check=full ./dining_philosophers
```

Should report: **All heap blocks were freed -- no leaks are possible**

## Configuration

Edit constants in `dining_philosophers.c`:

```c
#define NUM_PHILOSOPHERS 5        // Number of philosophers
#define THINKING_TIME_MAX 3       // Max seconds thinking
#define EATING_TIME_MAX 2         // Max seconds eating
#define NUM_MEALS 3               // Meals per philosopher
```

## Key Concepts Demonstrated

### 1. **Race Conditions**
- Multiple threads accessing shared resources (forks)
- Protected by semaphores

### 2. **Deadlock Prevention**
- Room semaphore ensures at least one philosopher can always proceed
- Prevents circular wait condition

### 3. **Mutual Exclusion**
- Only one philosopher can hold a fork at a time
- Implemented via binary semaphores

### 4. **Condition Variables**
- Used in semaphore implementation
- `pthread_cond_wait()`: Atomically releases mutex and blocks
- `pthread_cond_signal()`: Wakes one waiting thread

### 5. **Critical Sections**
- State updates protected by `state_mutex`
- Semaphore operations protected internally

## Educational Value

This implementation teaches:

1. **Low-level synchronization primitives**
   - Mutexes vs. Semaphores
   - Condition variables
   - When to use each

2. **Classical concurrency problems**
   - Deadlock, livelock, starvation
   - Resource allocation strategies

3. **Thread programming patterns**
   - Thread creation and joining
   - Passing arguments to threads
   - Thread-safe random number generation

4. **Debugging concurrent programs**
   - Using print statements effectively
   - Valgrind for memory issues
   - Understanding thread interleavings

## Common Pitfalls (Avoided Here)

❌ **All philosophers pick up left fork first** → Deadlock  
✅ We limit room capacity

❌ **Forgetting to unlock/signal** → Deadlock  
✅ All paths properly release resources

❌ **Using sleep() instead of proper synchronization** → Race conditions  
✅ We use proper semaphore operations

❌ **Memory leaks from dynamic allocation** → Memory corruption  
✅ Valgrind confirms no leaks

## Alternative Solutions

Other approaches to solve this problem:

1. **Asymmetric solution**: Odd philosophers pick left first, even pick right first
2. **Waiter solution**: Central coordinator grants permission
3. **Token passing**: Use a token to control access
4. **Hierarchy of locks**: Number forks and always acquire in order

Our **room limiting** approach is simple and effective for educational purposes.

## Cleaning Up

```bash
make clean
```

## Further Reading

- [POSIX Threads Programming](https://computing.llnl.gov/tutorials/pthreads/)
- [The Little Book of Semaphores](http://greenteapress.com/semaphores/)
- [Dining Philosophers Problem - Wikipedia](https://en.wikipedia.org/wiki/Dining_philosophers_problem)

## License

Educational use only.

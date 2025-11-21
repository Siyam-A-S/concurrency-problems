#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include "semaphore.h"

#define NUM_PHILOSOPHERS 5
#define THINKING_TIME_MAX 3
#define EATING_TIME_MAX 2
#define NUM_MEALS 3  // Each philosopher will eat this many times

// Philosopher states
typedef enum {
    THINKING,
    HUNGRY,
    EATING
} philosopher_state_t;

// Global data structures
semaphore_t forks[NUM_PHILOSOPHERS];      // One semaphore per fork
semaphore_t room;                         // Limits philosophers in the room (prevents deadlock)
pthread_mutex_t state_mutex;              // Protects state array
philosopher_state_t state[NUM_PHILOSOPHERS];
int meals_eaten[NUM_PHILOSOPHERS];        // Track meals per philosopher

// Color codes for prettier output
#define RESET   "\033[0m"
#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define YELLOW  "\033[33m"
#define BLUE    "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN    "\033[36m"

const char* colors[] = {RED, GREEN, YELLOW, BLUE, MAGENTA};

// Function to get philosopher name
const char* get_philosopher_name(int id) {
    const char* names[] = {"Socrates", "Plato", "Aristotle", "Descartes", "Kant"};
    return names[id];
}

// Function to print philosopher state
void print_state(int id, const char* action) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    
    printf("%s[%02d:%02d:%02d] Philosopher %d (%s): %s%s\n",
           colors[id], t->tm_hour, t->tm_min, t->tm_sec,
           id, get_philosopher_name(id), action, RESET);
    fflush(stdout);
}

// Philosopher thinks
void think(int id) {
    pthread_mutex_lock(&state_mutex);
    state[id] = THINKING;
    pthread_mutex_unlock(&state_mutex);
    
    print_state(id, "is THINKING 🤔");
    sleep(rand() % THINKING_TIME_MAX + 1);
}

// Philosopher picks up forks
void pickup_forks(int id) {
    pthread_mutex_lock(&state_mutex);
    state[id] = HUNGRY;
    pthread_mutex_unlock(&state_mutex);
    
    print_state(id, "is HUNGRY 🍽️  - waiting for forks");
    
    // Enter the room (limits concurrent philosophers to prevent deadlock)
    semaphore_wait(&room);
    
    int left_fork = id;
    int right_fork = (id + 1) % NUM_PHILOSOPHERS;
    
    // Pick up left fork
    print_state(id, "picking up LEFT fork");
    semaphore_wait(&forks[left_fork]);
    
    // Pick up right fork
    print_state(id, "picking up RIGHT fork");
    semaphore_wait(&forks[right_fork]);
    
    pthread_mutex_lock(&state_mutex);
    state[id] = EATING;
    pthread_mutex_unlock(&state_mutex);
    
    print_state(id, "has BOTH forks and is EATING 🍝");
}

// Philosopher eats
void eat(int id) {
    sleep(rand() % EATING_TIME_MAX + 1);
    
    pthread_mutex_lock(&state_mutex);
    meals_eaten[id]++;
    pthread_mutex_unlock(&state_mutex);
    
    print_state(id, "finished EATING (meal completed) ✓");
}

// Philosopher puts down forks
void putdown_forks(int id) {
    int left_fork = id;
    int right_fork = (id + 1) % NUM_PHILOSOPHERS; //last philosopher (p=4) tries to grab the fork on
                                                //their right, which is fork 0 in circular table
    
    // Put down left fork
    print_state(id, "putting down LEFT fork");
    semaphore_signal(&forks[left_fork]);
    
    // Put down right fork
    print_state(id, "putting down RIGHT fork");
    semaphore_signal(&forks[right_fork]);
    
    // Leave the room
    semaphore_signal(&room);
    
    print_state(id, "released BOTH forks");
}

// Philosopher thread function
void* philosopher(void* arg) {
    int id = *(int*)arg;
    free(arg);
    
    // Seed random number generator per thread
    unsigned int seed = time(NULL) + id;
    srand(seed);
    
    print_state(id, "has joined the table");
    
    // Each philosopher eats NUM_MEALS times
    for (int meal = 0; meal < NUM_MEALS; meal++) {
        think(id);
        pickup_forks(id);
        eat(id);
        putdown_forks(id);
    }
    
    print_state(id, "is DONE and leaving the table 👋");
    
    return NULL;
}

int main() {
    pthread_t philosophers[NUM_PHILOSOPHERS];
    
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════╗\n");
    printf("║         DINING PHILOSOPHERS PROBLEM SOLUTION              ║\n");
    printf("║   Using Custom Semaphores with Locks & Condition Vars     ║\n");
    printf("╚════════════════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("Configuration:\n");
    printf("  - Philosophers: %d\n", NUM_PHILOSOPHERS);
    printf("  - Meals per philosopher: %d\n", NUM_MEALS);
    printf("  - Room capacity: %d (prevents deadlock)\n\n", NUM_PHILOSOPHERS - 1);
    
    // Initialize state mutex
    pthread_mutex_init(&state_mutex, NULL);
    
    // Initialize all fork semaphores (binary semaphores)
    for (int i = 0; i < NUM_PHILOSOPHERS; i++) {
        if (semaphore_init(&forks[i], 1) != 0) {
            fprintf(stderr, "Failed to initialize fork semaphore %d\n", i);
            return 1;
        }
        state[i] = THINKING;
        meals_eaten[i] = 0;
    }
    
    // Initialize room semaphore (allow N-1 philosophers to prevent deadlock)
    if (semaphore_init(&room, NUM_PHILOSOPHERS - 1) != 0) {
        fprintf(stderr, "Failed to initialize room semaphore\n");
        return 1;
    }
    
    printf("Starting simulation...\n\n");
    
    // Create philosopher threads
    for (int i = 0; i < NUM_PHILOSOPHERS; i++) {
        int *id = malloc(sizeof(int));
        *id = i;
        
        if (pthread_create(&philosophers[i], NULL, philosopher, id) != 0) {
            fprintf(stderr, "Failed to create philosopher thread %d\n", i);
            return 1;
        }
    }
    
    // Wait for all philosophers to finish
    for (int i = 0; i < NUM_PHILOSOPHERS; i++) {
        pthread_join(philosophers[i], NULL);
    }
    
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════╗\n");
    printf("║                    SIMULATION COMPLETE                     ║\n");
    printf("╚════════════════════════════════════════════════════════════╝\n");
    printf("\nMeals eaten by each philosopher:\n");
    
    for (int i = 0; i < NUM_PHILOSOPHERS; i++) {
        printf("  %sPhilosopher %d (%s): %d meals%s\n",
               colors[i], i, get_philosopher_name(i), meals_eaten[i], RESET);
    }
    
    // Cleanup
    for (int i = 0; i < NUM_PHILOSOPHERS; i++) {
        semaphore_destroy(&forks[i]);
    }
    semaphore_destroy(&room);
    pthread_mutex_destroy(&state_mutex);
    
    printf("\nAll resources cleaned up successfully!\n\n");
    
    return 0;
}

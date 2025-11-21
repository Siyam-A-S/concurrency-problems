# Makefile for Dining Philosophers Problem

CC = gcc
CFLAGS = -std=c11 -g -Wall -Wextra -pthread
LDFLAGS = -pthread

# Targets
TARGET = dining_philosophers
SOURCES = dining_philosophers.c semaphore.c
OBJECTS = $(SOURCES:.c=.o)
HEADERS = semaphore.h

.PHONY: all clean run valgrind

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

run: $(TARGET)
	./$(TARGET)

valgrind: $(TARGET)
	valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes ./$(TARGET)

clean:
	rm -f $(TARGET) $(OBJECTS)

# Help target
help:
	@echo "Available targets:"
	@echo "  all       - Build the dining philosophers program"
	@echo "  run       - Build and run the program"
	@echo "  valgrind  - Run with Valgrind memory checker"
	@echo "  clean     - Remove compiled files"
	@echo "  help      - Show this help message"

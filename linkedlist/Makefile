# Makefile for Lock-Free Linked List

CC = gcc
CFLAGS = -std=c11 -g -Wall -Wextra -pthread -O2
LDFLAGS = -pthread

SOURCES = lock_free_list.c test_list.c
OBJECTS = $(SOURCES:.c=.o)
TARGET = test_list
HEADERS = lock_free_list.h

.PHONY: all clean run stress valgrind benchmark

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

run: $(TARGET)
	./$(TARGET)

stress: $(TARGET)
	./$(TARGET) stress

valgrind: $(TARGET)
	valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes ./$(TARGET)

benchmark: $(TARGET)
	@echo "Running performance benchmark..."
	@./$(TARGET)
	@echo ""
	@echo "Running 60-second stress test..."
	@./$(TARGET) stress

clean:
	rm -f $(TARGET) $(OBJECTS)

help:
	@echo "Available targets:"
	@echo "  all       - Build the test program"
	@echo "  run       - Run performance test (100K operations)"
	@echo "  stress    - Run 60-second stress test"
	@echo "  valgrind  - Run with Valgrind memory checker"
	@echo "  benchmark - Run both performance and stress tests"
	@echo "  clean     - Remove compiled files"
	@echo "  help      - Show this help message"

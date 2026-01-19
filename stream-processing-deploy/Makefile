# Stream Processor Deployment Makefile

.PHONY: build run stop logs clean test

# Build the Docker image
build:
	docker-compose build

# Build without cache
build-fresh:
	docker-compose build --no-cache

# Start the server
run:
	docker-compose up -d

# Start with logs attached
run-attached:
	docker-compose up

# Start with Coinbase WebSocket bridge
run-coinbase:
	docker-compose --profile coinbase up -d

# Start with ML pipeline (paper trading mode)
run-ml:
	docker-compose --profile coinbase --profile ml-pipeline up -d

# Start ML pipeline with custom model
run-ml-onnx:
	MODEL_TYPE=onnx docker-compose --profile coinbase --profile ml-pipeline up -d

# Start ML pipeline with remote model server
run-ml-remote:
	MODEL_TYPE=remote docker-compose --profile coinbase --profile ml-pipeline up -d

# Start FULL stack: stream processor + coinbase + ml pipeline
run-full:
	docker-compose --profile coinbase --profile ml-pipeline up -d

# Stop the server
stop:
	docker-compose --profile coinbase --profile ml-pipeline down

# View logs
logs:
	docker-compose logs -f

# View ML pipeline logs
logs-ml:
	docker-compose logs -f ml-pipeline

# View all service logs
logs-all:
	docker-compose --profile coinbase --profile ml-pipeline logs -f

# Check server status
status:
	docker-compose --profile coinbase --profile ml-pipeline ps
	@echo ""
	@echo "Server Stats:"
	@docker-compose exec stream-processor sh -c "echo 'stats' | nc localhost 9092 2>/dev/null || echo 'Server running'"

# Clean up
clean:
	docker-compose down -v --rmi local
	rm -rf ./data

# Test connection
test:
	@echo "Testing connection to stream processor..."
	@nc -zv localhost 9092 && echo "✓ Connection successful!" || echo "✗ Connection failed"

# Push to registry (customize REGISTRY variable)
REGISTRY ?= your-registry
TAG ?= latest

push:
	docker tag stream-processor:latest $(REGISTRY)/stream-processor:$(TAG)
	docker push $(REGISTRY)/stream-processor:$(TAG)

# Development: build locally without Docker
build-local:
	cd ../stream-processing && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j$$(nproc)
	g++ -std=c++20 -O3 -o stream-server \
		src/main.cpp \
		../stream-processing/build/CMakeFiles/streamprocessor.dir/src/core/*.o \
		../stream-processing/build/CMakeFiles/streamprocessor.dir/src/network/*.o \
		../stream-processing/build/CMakeFiles/streamprocessor.dir/src/storage/*.o \
		-I ../stream-processing/include \
		-lpthread

# Run locally (without Docker)
run-local: build-local
	./stream-server --data-dir=./data --port=9092 --bind=0.0.0.0

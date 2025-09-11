#!/bin/bash

# Test script for TorchServe Semantic Highlighter API
# Usage: ./test_api.sh

echo "====================================="
echo "TorchServe Semantic Highlighter Tests"
echo "====================================="

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if server is running
echo -e "\n${BLUE}Checking server status...${NC}"
curl -s http://localhost:8081/models | jq . || echo "Server not running. Start with: torchserve --start --model-store model_store --models semantic_highlighter.mar"

echo -e "\n${GREEN}Test 1: Single Document Highlighting${NC}"
echo "Question: What is semantic search?"
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What is semantic search?",
    "context": "OpenSearch is a powerful search engine. It provides semantic search capabilities. Semantic search understands the meaning and context of queries."
  }' | jq .

echo -e "\n${GREEN}Test 2: Batch with Single Question, Multiple Contexts${NC}"
echo "Question: What are the features?"
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What are the features?",
    "contexts": [
      "OpenSearch has many features. It supports full-text search and analytics.",
      "The semantic highlighter identifies relevant sentences. It uses machine learning models."
    ]
  }' | jq .

echo -e "\n${GREEN}Test 3: Multiple Question-Context Pairs${NC}"
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "What is OpenSearch?",
        "context": "OpenSearch is a community-driven, open source search and analytics suite. It provides powerful search capabilities."
      },
      {
        "question": "How does highlighting work?",
        "context": "The semantic highlighter uses BERT models. It analyzes sentences and identifies the most relevant ones based on the query."
      }
    ]
  }' | jq .

echo -e "\n${GREEN}Test 4: Legacy Batch Format${NC}"
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "batch": [
      {
        "question": "What is machine learning?",
        "context": "Machine learning is a subset of artificial intelligence. It enables systems to learn from data."
      },
      {
        "question": "What are neural networks?",
        "context": "Neural networks are computing systems inspired by biological neural networks. They are used in deep learning."
      }
    ]
  }' | jq .

echo -e "\n${GREEN}Test 5: Complex Document with Multiple Sentences${NC}"
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "question": "How does OpenSearch handle large-scale data?",
    "context": "OpenSearch is designed for scalability. It can handle petabytes of data across distributed clusters. The architecture supports horizontal scaling by adding more nodes. Data is automatically sharded and replicated for high availability. This ensures both performance and reliability at scale."
  }' | jq .

echo -e "\n${BLUE}Model Information:${NC}"
curl -s http://localhost:8081/models/semantic_highlighter | jq .

echo -e "\n${BLUE}Metrics Endpoint:${NC}"
curl -s http://localhost:8082/metrics | head -20

echo -e "\n====================================="
echo "Tests completed!"
echo "====================================="
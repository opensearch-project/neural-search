#!/bin/bash

# Test script for TorchServe Semantic Highlighter

echo "===== Testing TorchServe Semantic Highlighter ====="

# Check if TorchServe is running
echo "1. Checking TorchServe health..."
curl -X GET http://localhost:8081/models

echo -e "\n\n2. Testing single document highlighting..."
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What is semantic search?",
    "context": "OpenSearch is a powerful search engine. It provides semantic search capabilities. Semantic search understands the meaning and context of queries."
  }' | python -m json.tool

echo -e "\n\n3. Testing batch highlighting with single question..."
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What are the features?",
    "contexts": [
      "OpenSearch has many features. It supports full-text search and analytics.",
      "The semantic highlighter identifies relevant sentences. It uses machine learning models."
    ]
  }' | python -m json.tool

echo -e "\n\n4. Testing batch with multiple question-context pairs..."
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
  }' | python -m json.tool

echo -e "\n\n5. Testing legacy batch format..."
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
  }' | python -m json.tool

echo -e "\n\nAll tests completed!"
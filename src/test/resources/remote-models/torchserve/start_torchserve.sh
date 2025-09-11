#!/bin/bash

# Smart TorchServe startup script with automatic model download
# This script automatically downloads models from HuggingFace if not present locally

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "===== Smart TorchServe Startup ====="

# Configuration
MODEL_NAME="semantic_highlighter"
MODEL_VERSION="1.0"
MODEL_STORE="model_store"
MODEL_FILES="model_files"
HUGGINGFACE_REPO="opensearch-project/opensearch-semantic-highlighter-v1"

# Create directories
mkdir -p "$MODEL_STORE"
mkdir -p "$MODEL_FILES"
mkdir -p logs

# Check if model archive already exists
if [ ! -f "$MODEL_STORE/${MODEL_NAME}.mar" ]; then
    echo "Model archive not found. Downloading from HuggingFace..."
    
    # Download model files from HuggingFace if not present
    if [ ! -f "$MODEL_FILES/pytorch_model.bin" ]; then
        echo "Downloading pytorch_model.bin..."
        wget -q --show-progress "https://huggingface.co/${HUGGINGFACE_REPO}/resolve/main/pytorch_model.bin" \
             -O "$MODEL_FILES/pytorch_model.bin"
    else
        echo "pytorch_model.bin already exists, skipping download"
    fi
    
    if [ ! -f "$MODEL_FILES/config.json" ]; then
        echo "Downloading config.json..."
        wget -q --show-progress "https://huggingface.co/${HUGGINGFACE_REPO}/resolve/main/config.json" \
             -O "$MODEL_FILES/config.json"
    else
        echo "config.json already exists, skipping download"
    fi
    
    # Check for tokenizer files (optional but useful)
    if [ ! -f "$MODEL_FILES/tokenizer_config.json" ]; then
        echo "Downloading tokenizer_config.json..."
        wget -q --show-progress "https://huggingface.co/${HUGGINGFACE_REPO}/resolve/main/tokenizer_config.json" \
             -O "$MODEL_FILES/tokenizer_config.json" 2>/dev/null || echo "tokenizer_config.json not found, continuing..."
    fi
    
    if [ ! -f "$MODEL_FILES/vocab.txt" ]; then
        echo "Downloading vocab.txt..."
        wget -q --show-progress "https://huggingface.co/${HUGGINGFACE_REPO}/resolve/main/vocab.txt" \
             -O "$MODEL_FILES/vocab.txt" 2>/dev/null || echo "vocab.txt not found, continuing..."
    fi
    
    # Create model archive
    echo "Creating TorchServe model archive..."
    torch-model-archiver \
        --model-name "$MODEL_NAME" \
        --version "$MODEL_VERSION" \
        --handler "handlers/torchserve_handler.py" \
        --extra-files "$MODEL_FILES/config.json,$MODEL_FILES/pytorch_model.bin" \
        --export-path "$MODEL_STORE" \
        --force
    
    echo "Model archive created successfully!"
else
    echo "Model archive already exists at $MODEL_STORE/${MODEL_NAME}.mar"
fi

# Stop any existing TorchServe instance
echo "Stopping any existing TorchServe instances..."
torchserve --stop 2>/dev/null || true
sleep 2

# Start TorchServe
echo "Starting TorchServe..."
torchserve --start \
    --ts-config configs/config.properties \
    --model-store "$MODEL_STORE" \
    --models "${MODEL_NAME}=${MODEL_NAME}.mar" \
    --ncs

# Wait for TorchServe to be ready
echo "Waiting for TorchServe to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:8082/ping | grep -q "Healthy"; then
        echo "TorchServe is ready!"
        break
    fi
    echo -n "."
    sleep 1
done

# Verify model is loaded
echo ""
echo "Verifying model is loaded..."
curl -s http://localhost:8081/models | python3 -m json.tool

echo ""
echo "===== TorchServe Started Successfully ====="
echo "Inference endpoint: http://localhost:8080"
echo "Management endpoint: http://localhost:8081"
echo "Metrics endpoint: http://localhost:8082"
echo ""
echo "Test with:"
echo "  curl -X POST http://localhost:8080/predictions/$MODEL_NAME \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"question\": \"What is OpenSearch?\", \"context\": \"OpenSearch is a search engine.\"}'"
echo ""
echo "Stop with: torchserve --stop"
#!/bin/bash

# Setup script for TorchServe Semantic Highlighter deployment
# Optimized for CPU deployment

set -e

echo "===== TorchServe Semantic Highlighter Setup ====="
echo "Note: This setup is optimized for CPU deployment"

# Check if model files exist
if [ ! -d "sagemaker-model" ]; then
    echo "Error: sagemaker-model directory not found!"
    echo "Please extract the model artifact first."
    exit 1
fi

# Create directories
echo "Creating directories..."
mkdir -p model_store
mkdir -p logs

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Download NLTK data
echo "Downloading NLTK data..."
python -c "import nltk; nltk.download('punkt'); nltk.download('punkt_tab')"

# Create model archive
echo "Creating TorchServe model archive..."
torch-model-archiver \
    --model-name semantic_highlighter \
    --version 1.0 \
    --handler torchserve_handler.py \
    --extra-files sagemaker-model/model_files/config.json,sagemaker-model/model_files/pytorch_model.bin,sagemaker-model/model_files/model.safetensors \
    --export-path model_store \
    --force

echo "Model archive created successfully!"

# Start TorchServe
echo "Starting TorchServe..."
torchserve --start --ts-config config.properties --ncs

echo ""
echo "===== TorchServe Started Successfully ====="
echo "Inference endpoint: http://localhost:8080"
echo "Management endpoint: http://localhost:8081"
echo "Metrics endpoint: http://localhost:8082"
echo ""
echo "To test the model, use test_torchserve.sh"
echo "To stop TorchServe, run: torchserve --stop"
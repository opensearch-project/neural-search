# TorchServe Semantic Highlighting Test Resources

This directory contains the TorchServe configuration and handler files for testing semantic highlighting with remote models.

## Directory Structure

```
remote-models/torchserve/
├── handlers/               # TorchServe handler implementations
│   ├── torchserve_handler.py         # Batch-enabled handler
│   └── torchserve_handler_single.py  # Single inference handler
├── configs/                # Configuration files
│   ├── config.properties              # TorchServe configuration
│   ├── ml_commons_connector_torchserve.json  # ML Commons connector config
│   └── requirements.txt              # Python dependencies
├── scripts/                # Setup and test scripts
│   ├── setup_torchserve.sh          # Setup TorchServe locally
│   ├── test_torchserve.sh           # Test TorchServe endpoints
│   └── test_api.sh                  # Test API integration
└── README.md               # This file
```

## Model Files (Not in Git)

The large model files are NOT stored in Git. They need to be downloaded separately:

- `model-1.tar.gz` (774MB) - Contains the BERT model for semantic highlighting
- `model_files/` - Extracted model files
- `sagemaker-model/` - SageMaker-compatible model format
- `model_store/` - TorchServe model archive storage

## Setup Instructions

### For Local Development

1. **Download Model Files**:
   ```bash
   # Download the model from HuggingFace
   wget https://huggingface.co/opensearch-project/opensearch-semantic-highlighter-v1/resolve/main/pytorch_model.bin
   wget https://huggingface.co/opensearch-project/opensearch-semantic-highlighter-v1/resolve/main/config.json
   ```

2. **Create Model Archive**:
   ```bash
   cd src/test/resources/remote-models/torchserve
   torch-model-archiver \
     --model-name semantic_highlighter \
     --version 1.0 \
     --handler handlers/torchserve_handler.py \
     --extra-files model_files/config.json,model_files/pytorch_model.bin \
     --export-path model_store \
     --force
   ```

3. **Start TorchServe**:
   ```bash
   torchserve --start --ts-config configs/config.properties --model-store model_store --ncs
   ```

4. **Run Tests**:
   ```bash
   ./gradlew integTest --tests "*.SemanticHighlightingIT" -Dtests.torchserve.endpoint=http://localhost:8080
   ```

### For GitHub CI

The CI pipeline automatically:
1. Uses the `pytorch/torchserve:latest-cpu` Docker image
2. Downloads model files from HuggingFace
3. Creates the model archive
4. Registers the model with TorchServe
5. Runs integration tests with `TORCHSERVE_ENDPOINT=http://localhost:8080`

## Handler Versions

- **torchserve_handler.py**: Supports batch inference for processing multiple documents
- **torchserve_handler_single.py**: Single document inference (used in CI for simplicity)

## Testing

To test TorchServe locally:
```bash
# Check if TorchServe is running
curl http://localhost:8081/models

# Test inference
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{"question": "What is OpenSearch?", "context": "OpenSearch is a search engine."}'
```

## Troubleshooting

1. **Port already in use**: Stop existing TorchServe with `torchserve --stop`
2. **Model not loading**: Check logs in `logs/` directory
3. **NLTK data missing**: Run `python -c "import nltk; nltk.download('punkt')"`

## Important Notes

- Do NOT commit model files (*.bin, *.safetensors, *.tar.gz) to Git
- Model files should be downloaded dynamically during setup
- Keep handler files lightweight and version-controlled
- Update CI workflow if handler logic changes significantly
#!/bin/bash
# Unified script for managing TorchServe lifecycle for integration tests
# Usage: ./run.sh {start|setup-model|test|stop|remove|status|lifecycle [setup|teardown]}

set -e

# Configuration
CONTAINER_NAME="${TORCHSERVE_CONTAINER_NAME:-torchserve-integ-test}"
TORCHSERVE_IMAGE="${TORCHSERVE_IMAGE:-pytorch/torchserve:0.9.0-cpu}"
INFERENCE_PORT="${INFERENCE_PORT:-8080}"
MANAGEMENT_PORT="${MANAGEMENT_PORT:-8081}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HANDLERS_DIR="${SCRIPT_DIR}/../handlers"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Logging functions
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

# Check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker to run remote model tests."
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running. Please start Docker."
        exit 1
    fi
}

# Check if container exists
container_exists() {
    docker ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"
}

# Check if container is running
container_running() {
    docker ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"
}

# Get container memory usage statistics
get_memory_stats() {
    local container_id=$(docker ps --format "{{.ID}}" --filter "name=${CONTAINER_NAME}" | head -1)
    if [ -z "$container_id" ]; then
        return 1
    fi

    # Get memory stats from docker
    local stats=$(docker stats --no-stream --format "json" ${CONTAINER_NAME} 2>/dev/null)
    if [ -z "$stats" ]; then
        return 1
    fi

    # Parse memory usage
    local mem_usage=$(echo "$stats" | python3 -c "
import json, sys
try:
    data = json.loads(sys.stdin.read())
    print(f\"Memory: {data['MemUsage']}\")
    print(f\"Memory %: {data['MemPerc']}\")
    print(f\"CPU %: {data['CPUPerc']}\")
except:
    pass
" 2>/dev/null)

    echo "$mem_usage"
}

# Log memory at key checkpoints
log_memory_checkpoint() {
    local checkpoint_name="$1"
    local mem_stats=$(get_memory_stats)
    if [ ! -z "$mem_stats" ]; then
        log_info "Memory checkpoint - ${checkpoint_name}:"
        echo "$mem_stats" | while IFS= read -r line; do
            echo "  $line"
        done
    fi
}

# Start TorchServe container
start() {
    check_docker

    if container_running; then
        log_info "TorchServe container '${CONTAINER_NAME}' is already running"
        return 0
    fi

    log_info "Starting TorchServe container..."

    if container_exists; then
        docker start ${CONTAINER_NAME}
    else
        docker run -d \
            --name ${CONTAINER_NAME} \
            -p ${INFERENCE_PORT}:8080 \
            -p ${MANAGEMENT_PORT}:8081 \
            --memory 2g \
            --cpus 1 \
            ${TORCHSERVE_IMAGE} \
            torchserve --start --model-store /home/model-server/model-store
    fi

    # Wait for container to be ready
    log_info "Waiting for TorchServe to be ready..."
    local max_attempts=30
    for i in $(seq 1 $max_attempts); do
        if curl -s http://localhost:${INFERENCE_PORT}/ping 2>/dev/null | grep -q "Healthy"; then
            log_info "✓ TorchServe is ready!"
            # Log initial memory after startup
            log_memory_checkpoint "Container started"
            return 0
        fi
        echo "  Waiting for TorchServe... ($i/$max_attempts)"
        sleep 2
    done

    log_error "TorchServe failed to start"
    exit 1
}

# Auto-discover all models from handler files
discover_models() {
    local models=()

    # Find all handler files and extract model names
    if [ -d "${HANDLERS_DIR}" ]; then
        for handler in ${HANDLERS_DIR}/*_handler.py; do
            if [ -f "$handler" ]; then
                # Extract model name from filename (e.g., semantic_highlighting_handler.py -> semantic_highlighting)
                local filename=$(basename "$handler")
                local model_name="${filename%_handler.py}"
                models+=("$model_name")
            fi
        done
    fi

    echo "${models[@]}"
}

# Setup single model in TorchServe
setup_model() {
    local model_name=${1:-semantic_highlighting}
    log_info "Setting up model: $model_name..."

    local container_id=$(docker ps --format "{{.ID}}" --filter "name=${CONTAINER_NAME}" | head -1)
    if [ -z "$container_id" ]; then
        log_error "TorchServe container not found"
        exit 1
    fi

    # Install dependencies
    log_info "Installing dependencies in container..."
    docker exec ${container_id} pip install -q --no-cache-dir \
        transformers==4.35.0 \
        datasets \
        nltk \
        safetensors

    # Download NLTK data
    docker exec ${container_id} python -c "
import nltk
try:
    nltk.download('punkt', quiet=True)
    nltk.download('punkt_tab', quiet=True)
except:
    pass
" 2>/dev/null || true

    # Check if model is already loaded
    local model_status=$(curl -s http://localhost:${MANAGEMENT_PORT}/models 2>/dev/null || echo "[]")
    if echo "$model_status" | grep -q "${model_name}"; then
        log_info "Model '${model_name}' is already loaded"
        return 0
    fi

    # Deploy model
    deploy_model "$model_name"
}

# Setup all discovered models
setup_all_models() {
    local models=($(discover_models))

    if [ ${#models[@]} -eq 0 ]; then
        log_warning "No models found in ${HANDLERS_DIR}"
        log_info "Make sure handler files follow the naming convention: <model_name>_handler.py"
        return 1
    fi

    log_info "Found ${#models[@]} model(s): ${models[*]}"

    for model in "${models[@]}"; do
        setup_model "$model"

        # Export endpoint for this model
        local env_var="$(echo ${model} | tr '[:lower:]' '[:upper:]')_ENDPOINT"
        export ${env_var}="http://localhost:${INFERENCE_PORT}/predictions/${model}"
        log_info "Exported ${env_var}=http://localhost:${INFERENCE_PORT}/predictions/${model}"

        # Log memory after loading each model
        log_memory_checkpoint "After loading ${model}"
    done

    # Log final memory after all models loaded
    log_memory_checkpoint "All models loaded"
}

# Deploy model to TorchServe
deploy_model() {
    local model_name=${1:-semantic_highlighting}
    log_info "Deploying model: $model_name..."

    local work_dir=$(mktemp -d)
    cd ${work_dir}

    mkdir -p model_store model_files

    # Copy handler - check if it exists
    local handler_path="${HANDLERS_DIR}/${model_name}_handler.py"
    if [ -f "${handler_path}" ]; then
        cp "${handler_path}" handler.py
    else
        log_info "Handler not found at ${handler_path}, using embedded handler for ${model_name}"
        # Create a basic handler inline
        cat > handler.py << 'EOF'
import json
import logging
import torch
from transformers import pipeline, AutoTokenizer, AutoModelForQuestionAnswering

logger = logging.getLogger(__name__)

class SemanticHighlightingHandler:
    def __init__(self):
        self.initialized = False
        self.model = None
        self.tokenizer = None
        self.pipeline = None

    def initialize(self, context):
        """Initialize the model"""
        try:
            logger.info("Initializing semantic highlighter model...")

            # Load model and tokenizer from extra files
            model_dir = context.system_properties.get("model_dir")

            self.tokenizer = AutoTokenizer.from_pretrained(model_dir)
            self.model = AutoModelForQuestionAnswering.from_pretrained(model_dir)

            self.pipeline = pipeline(
                "question-answering",
                model=self.model,
                tokenizer=self.tokenizer,
                device=-1  # CPU
            )

            self.initialized = True
            logger.info("Model initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize model: {str(e)}")
            raise

    def preprocess(self, data):
        """Preprocess input data"""
        try:
            # Handle both single and batch requests
            if isinstance(data, list):
                # Batch request
                inputs = []
                for item in data:
                    body = item.get("body", item.get("data", item))
                    if isinstance(body, (str, bytes)):
                        body = json.loads(body)
                    inputs.append(body)
                return inputs
            else:
                # Single request
                body = data[0].get("body", data[0].get("data", data[0]))
                if isinstance(body, (str, bytes)):
                    body = json.loads(body)

                # Check if it's already a batch format
                if "inputs" in body and isinstance(body["inputs"], list):
                    return body["inputs"]

                return [body]
        except Exception as e:
            logger.error(f"Preprocessing error: {str(e)}")
            raise

    def inference(self, data):
        """Run inference"""
        try:
            results = []

            for item in data:
                question = item.get("question", "")
                context = item.get("context", "")

                result = self.pipeline(
                    question=question,
                    context=context,
                    max_answer_len=50,
                    doc_stride=128
                )

                # Format response for semantic highlighting
                highlight_result = {
                    "highlights": [{
                        "start": result["start"],
                        "end": result["end"],
                        "score": result["score"],
                        "text": result["answer"]
                    }]
                }

                results.append(highlight_result)

            return results
        except Exception as e:
            logger.error(f"Inference error: {str(e)}")
            raise

    def postprocess(self, data):
        """Postprocess results"""
        # Return as-is for now
        return data

    def handle(self, data, context):
        """Main handler entry point"""
        if not self.initialized:
            self.initialize(context)

        input_data = self.preprocess(data)
        predictions = self.inference(input_data)
        return self.postprocess(predictions)

# Create handler instance
_service = SemanticHighlightingHandler()

def handle(data, context):
    """Entry point for TorchServe"""
    return _service.handle(data, context)
EOF
    fi

    # Download model files from HuggingFace
    log_info "Downloading model files from HuggingFace..."
    local base_url="https://huggingface.co/opensearch-project/opensearch-semantic-highlighter-v1/resolve/main"

    for file in config.json tokenizer_config.json vocab.txt; do
        if ! curl -fsSL "$base_url/$file" -o "model_files/$file"; then
            log_error "Failed to download $file"
            rm -rf ${work_dir}
            exit 1
        fi
        echo "  ✓ Downloaded $file"
    done

    # Download model weights
    log_info "Downloading model weights (this may take a moment)..."
    if ! curl -L --progress-bar "$base_url/model.safetensors" -o "model_files/model.safetensors"; then
        log_error "Failed to download model weights"
        rm -rf ${work_dir}
        exit 1
    fi
    log_info "✓ Downloaded model weights"

    # Install torch-model-archiver if needed
    if ! command -v torch-model-archiver &> /dev/null; then
        log_info "Installing torch-model-archiver..."
        pip install -q torch-model-archiver --no-cache-dir
    fi

    # Create Model Archive
    log_info "Creating model archive..."
    torch-model-archiver \
        --model-name "${model_name}" \
        --version 1.0 \
        --handler handler.py \
        --extra-files model_files/ \
        --export-path model_store \
        --force

    # Deploy to container
    local container_id=$(docker ps --format "{{.ID}}" --filter "name=${CONTAINER_NAME}" | head -1)
    docker cp "model_store/${model_name}.mar" "${container_id}:/home/model-server/model-store/"

    # Create config
    cat > config.properties << EOF
inference_address=http://0.0.0.0:8080
management_address=http://0.0.0.0:8081
model_store=/home/model-server/model-store
load_models=${model_name}.mar
default_workers_per_model=1
EOF

    docker cp config.properties "${container_id}:/home/model-server/"

    # Restart TorchServe with model
    log_info "Restarting TorchServe with model..."
    docker exec ${container_id} bash -c "torchserve --stop || true"
    sleep 3
    docker exec -d ${container_id} torchserve --start \
        --model-store /home/model-server/model-store \
        --ts-config /home/model-server/config.properties

    # Clean up temp directory
    cd - > /dev/null
    rm -rf ${work_dir}

    # Wait for model to load
    log_info "Waiting for model to be ready..."
    local max_attempts=20
    for i in $(seq 1 $max_attempts); do
        local model_status=$(curl -s http://localhost:${MANAGEMENT_PORT}/models 2>/dev/null || echo "{}")
        if echo "$model_status" | grep -q "${model_name}"; then
            log_info "✓ Model '${model_name}' is loaded"

            # Re-install dependencies for workers
            docker exec ${container_id} pip install -q --no-cache-dir \
                transformers==4.35.0 datasets nltk safetensors 2>/dev/null

            return 0
        fi
        echo "  Waiting for model... ($i/$max_attempts)"
        sleep 3
    done

    log_error "Model failed to load"
    exit 1
}

# Show memory usage summary
show_memory_summary() {
    echo ""
    echo "=== Memory Usage Summary ==="
    local mem_stats=$(get_memory_stats)
    if [ ! -z "$mem_stats" ]; then
        echo "$mem_stats"
    else
        echo "Unable to fetch memory statistics"
    fi

    # Get TorchServe metrics if available
    local metrics=$(curl -s http://localhost:8082/metrics 2>/dev/null | grep -E "memory|cpu" | head -10)
    if [ ! -z "$metrics" ]; then
        echo ""
        echo "TorchServe Metrics:"
        echo "$metrics"
    fi
    echo "==========================="
}

# List discovered models
list_models() {
    local models=($(discover_models))

    echo "Discovered models in ${HANDLERS_DIR}:"
    echo "====================================="

    if [ ${#models[@]} -eq 0 ]; then
        echo "  No models found"
        echo "  Add handler files following the pattern: <model_name>_handler.py"
    else
        for model in "${models[@]}"; do
            local handler="${HANDLERS_DIR}/${model}_handler.py"
            if [ -f "$handler" ]; then
                echo "  ✓ $model"
                echo "    Handler: $(basename $handler)"
                echo "    Endpoint: http://localhost:${INFERENCE_PORT}/predictions/${model}"
            fi
        done
    fi
}

# Test model inference
test() {
    log_info "Testing model inference with memory monitoring..."

    local models=($(discover_models))
    if [ ${#models[@]} -eq 0 ]; then
        log_warning "No models to test"
        return 1
    fi

    # Test first available model
    local model_name=${models[0]}

    # Log memory before inference
    log_memory_checkpoint "Before single inference"

    # Test single inference
    local response=$(curl -s -X POST http://localhost:${INFERENCE_PORT}/predictions/${model_name} \
        -H "Content-Type: application/json" \
        -d '{
            "question": "What is OpenSearch?",
            "context": "OpenSearch is a distributed, community-driven, Apache 2.0-licensed, open-source search and analytics suite."
        }' 2>/dev/null)

    if echo "$response" | grep -q "highlights"; then
        log_info "✓ Single inference successful!"
        echo "  Response: $response"
        # Log memory after single inference
        log_memory_checkpoint "After single inference"
    else
        log_error "Single inference failed"
        echo "  Response: $response"
        exit 1
    fi

    # Test batch inference
    log_info "Testing batch inference..."
    log_memory_checkpoint "Before batch inference"
    local batch_response=$(curl -s -X POST http://localhost:${INFERENCE_PORT}/predictions/${model_name} \
        -H "Content-Type: application/json" \
        -d '{
            "inputs": [
                {
                    "question": "What is OpenSearch?",
                    "context": "OpenSearch is a scalable, flexible, and extensible open-source software suite."
                },
                {
                    "question": "What is machine learning?",
                    "context": "Machine learning is a branch of artificial intelligence."
                }
            ]
        }' 2>/dev/null)

    if echo "$batch_response" | grep -q "highlights"; then
        log_info "✓ Batch inference successful!"
        log_memory_checkpoint "After batch inference"
    else
        log_warning "Batch inference may not be working correctly"
    fi

    # Show memory summary
    show_memory_summary
}

# Stop TorchServe container
stop() {
    if container_running; then
        log_info "Stopping TorchServe container..."
        docker stop ${CONTAINER_NAME}
        log_info "✓ Container stopped"
    else
        log_warning "Container is not running"
    fi
}

# Remove container completely
remove() {
    stop
    if container_exists; then
        log_info "Removing TorchServe container..."
        docker rm ${CONTAINER_NAME}
        log_info "✓ Container removed"
    fi
}

# Show status
status() {
    echo "========================================="
    echo "TorchServe Integration Test Status"
    echo "========================================="

    if container_running; then
        log_info "Container: Running"

        if curl -s http://localhost:${INFERENCE_PORT}/ping 2>/dev/null | grep -q "Healthy"; then
            log_info "Health: Healthy"
        else
            log_warning "Health: Not responding"
        fi

        echo ""
        echo "Endpoints:"
        echo "  Inference: http://localhost:${INFERENCE_PORT}"
        echo "  Management: http://localhost:${MANAGEMENT_PORT}"

        # Show all model endpoints
        local models=($(discover_models))
        for model in "${models[@]}"; do
            echo "  Model ($model): http://localhost:${INFERENCE_PORT}/predictions/${model}"
        done

        # Check loaded models
        echo ""
        echo "Loaded models:"
        curl -s http://localhost:${MANAGEMENT_PORT}/models 2>/dev/null | python -m json.tool 2>/dev/null || echo "  Unable to fetch models"

        # Show resource usage
        echo ""
        echo "Resource Usage:"
        get_memory_stats | while IFS= read -r line; do
            echo "  $line"
        done
    else
        log_warning "Container: Not running"
    fi
}

# Main function for lifecycle management
lifecycle() {
    case "$1" in
        setup)
            start
            setup_all_models
            test
            ;;
        teardown)
            stop
            ;;
        *)
            log_error "Unknown lifecycle command: $1"
            exit 1
            ;;
    esac
}

# Main command handler
case "${1:-}" in
    start)
        start
        ;;
    setup-model)
        if [ -n "$2" ]; then
            setup_model "$2"
        else
            log_error "Please specify a model name: $0 setup-model <model_name>"
            list_models
            exit 1
        fi
        ;;
    setup-all-models)
        setup_all_models
        ;;
    list-models)
        list_models
        ;;
    test)
        test
        ;;
    stop)
        stop
        ;;
    remove)
        remove
        ;;
    status)
        status
        ;;
    lifecycle)
        lifecycle "${2:-}"
        ;;
    *)
        echo "Usage: $0 {start|setup-model|setup-all-models|list-models|test|stop|remove|status|lifecycle [setup|teardown]}"
        echo ""
        echo "Commands:"
        echo "  start            - Start TorchServe container"
        echo "  setup-model      - Deploy specific model to running container"
        echo "  setup-all-models - Deploy all discovered models"
        echo "  list-models      - List all discovered models"
        echo "  test             - Test model inference"
        echo "  stop             - Stop container"
        echo "  remove           - Remove container completely"
        echo "  status           - Show current status"
        echo "  lifecycle        - Manage full lifecycle (setup/teardown)"
        echo ""
        echo "Lifecycle usage (for Gradle integration):"
        echo "  $0 lifecycle setup    # Start and setup everything"
        echo "  $0 lifecycle teardown # Stop everything"
        exit 1
        ;;
esac
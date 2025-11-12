import json
import torch
from transformers import AutoTokenizer, AutoModel
from ts.torch_handler.base_handler import BaseHandler

class AsymmetricTextEmbeddingHandler(BaseHandler):
    """
    TorchServe handler for asymmetric text embedding using multilingual-e5-small model.
    Supports different content types (query vs passage) for asymmetric embeddings.
    """
    
    def __init__(self):
        super().__init__()
        self.model = None
        self.tokenizer = None
        self.initialized = False

    def initialize(self, context):
        """Initialize the model and tokenizer."""
        self.manifest = context.manifest
        properties = context.system_properties
        model_dir = properties.get("model_dir")
        
        # Load model from HuggingFace Hub
        model_name = "intfloat/multilingual-e5-small"
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.model.eval()
        
        self.initialized = True

    def preprocess(self, data):
        """
        Parse input JSON and return list of texts for embedding generation.
        
        Expected input formats:
        1. Custom format: {"texts": ["text1", "text2"], "content_type": "query"}
        2. Custom with parameters: {"parameters": {"texts": ["text1"], "content_type": "passage"}}
        """
        texts = []
        
        for row in data:
            # Handle different input formats
            if isinstance(row, dict):
                if "body" in row:
                    # HTTP request format - body might be string or dict
                    body = row["body"]
                    if isinstance(body, str):
                        input_data = json.loads(body)
                    else:
                        input_data = body
                else:
                    # Direct dict format
                    input_data = row
            else:
                # String format
                input_data = json.loads(row)

            # Handle OpenSearch remote connector format
            if "parameters" in input_data:
                params = input_data["parameters"]
                texts_list = params.get("texts", [])
                content_type = params.get("content_type")
                if content_type:
                    texts_list = [f"{content_type}: {text}" for text in texts_list]
            else:
                texts_list = input_data.get("texts", [])
                content_type = input_data.get("content_type")
                if content_type:
                    texts_list = [f"{content_type}: {text}" for text in texts_list]
            
            texts.extend(texts_list)
        
        # Ensure we have at least one text
        if not texts:
            texts = [""]
            
        return texts

    def inference(self, data):
        """Generate embeddings for the input texts."""
        if not self.initialized:
            raise RuntimeError("Model not initialized")
        
        # Tokenize
        inputs = self.tokenizer(data, padding=True, truncation=True, return_tensors="pt", max_length=512)

        # Generate embeddings
        with torch.no_grad():
            outputs = self.model(**inputs)
            embeddings = outputs.last_hidden_state.mean(dim=1)

        return embeddings.cpu().numpy()

    def postprocess(self, data):
        """
        Format prediction output for OpenSearch connector compatibility.
        
        Expected output format (simple array for OpenSearch to wrap):
        Single embedding: [0.1, 0.2, 0.3, ...]
        Batch embeddings: [[0.1, 0.2, ...], [0.3, 0.4, ...]]
        """
        # Return simple array format for OpenSearch to wrap properly
        if len(data.shape) == 2:  # Batch of embeddings
            # Return array of arrays for batch
            result = [embedding.tolist() for embedding in data]
        else:  # Single embedding
            # Return single array
            result = data.tolist()

        return [result]

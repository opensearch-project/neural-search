import json
import logging
import torch
from transformers import AutoTokenizer, AutoModel
from ts.torch_handler.base_handler import BaseHandler

logger = logging.getLogger(__name__)

class SymmetricTextEmbeddingHandler(BaseHandler):
    """
    Simple TorchServe handler for symmetric text embedding
    """

    def __init__(self):
        super().__init__()
        self.model = None
        self.tokenizer = None
        self.initialized = False

    def initialize(self, context):
        """Initialize the model and tokenizer."""
        try:
            logger.info("Initializing symmetric text embedding model...")

            # Using Google's tiny BERT - Apache 2.0 licensed, same as OpenSearch!
            model_name = "google/bert_uncased_L-2_H-128_A-2"  # 4.4MB, 128-dim embeddings, Apache 2.0

            logger.info(f"Loading model: {model_name}")
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModel.from_pretrained(model_name)
            self.model.eval()

            self.initialized = True
            logger.info("Model initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize model: {str(e)}")
            raise

    def preprocess(self, data):
        """
        Parse input JSON and return list of texts for embedding generation.

        Expected input formats:
        1. Direct format: {"texts": ["text1", "text2"]}
        2. With parameters: {"parameters": {"texts": ["text1", "text2"]}}
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
            else:
                texts_list = input_data.get("texts", [])

            texts.extend(texts_list)

        # Ensure we have at least one text
        if not texts:
            texts = [""]

        logger.info(f"Processing {len(texts)} text(s)")
        return texts

    def inference(self, data):
        """Generate embeddings for the input texts."""
        try:
            if not self.initialized:
                raise RuntimeError("Model not initialized")

            logger.info(f"Running inference on {len(data)} texts")

            # Tokenize
            inputs = self.tokenizer(
                data,
                padding=True,
                truncation=True,
                return_tensors="pt",
                max_length=512
            )

            # Generate embeddings
            with torch.no_grad():
                outputs = self.model(**inputs)

                # Mean pooling
                token_embeddings = outputs.last_hidden_state
                attention_mask = inputs['attention_mask']
                input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
                embeddings = torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

                # Normalize
                embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)

            result = embeddings.cpu().numpy()
            logger.info(f"Generated embeddings with shape: {result.shape}")

            return result

        except Exception as e:
            logger.error(f"Error in inference: {str(e)}")
            raise

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
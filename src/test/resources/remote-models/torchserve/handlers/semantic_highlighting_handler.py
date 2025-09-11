"""
TorchServe Handler for Semantic Highlighter Model
"""

import os
import json
import logging
import torch
import torch.nn as nn
from torch.nn.utils.rnn import pad_sequence
from transformers import (
    AutoTokenizer,
    AutoConfig,
    BertModel
)
from transformers.models.bert.modeling_bert import BertPreTrainedModel
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass, field
from functools import partial
from datasets import Dataset
import nltk

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment configuration
MAX_BATCH_SIZE = int(os.environ.get('MAX_BATCH_SIZE', '256'))
MAX_LENGTH = 510
STRIDE = 128

# Global variables for model and tokenizer
model = None
tokenizer = None
device = None


class BertTaggerForSentenceExtractionWithBackoff(BertPreTrainedModel):
    """Sentence-level BERT classifier with confidence-backoff rule"""

    def __init__(self, config):
        super().__init__(config)
        self.num_labels = config.num_labels

        self.bert = BertModel(config, add_pooling_layer=False)
        self.dropout = nn.Dropout(config.hidden_dropout_prob)
        self.classifier = nn.Linear(config.hidden_size, self.num_labels)

    def forward(
            self,
            input_ids=None,
            attention_mask=None,
            token_type_ids=None,
            sentence_ids=None,
    ):
        outputs = self.bert(
            input_ids,
            attention_mask=attention_mask,
            token_type_ids=token_type_ids,
        )

        sequence_output = self.dropout(outputs[0])

        def _get_agg_output(ids, seq_out):
            """Aggregate token embeddings to sentence level"""
            max_sentences = torch.max(ids) + 1
            d_model = seq_out.size(-1)

            agg_out, global_offsets, num_sents = [], [], []
            for i, sen_ids in enumerate(ids):
                out, local_ids = [], sen_ids.clone()
                mask = local_ids != -100
                if not mask.any():
                    global_offsets.append(0)
                    num_sents.append(0)
                    agg_out.append(torch.zeros((max_sentences, d_model), device=seq_out.device))
                    continue

                offset = local_ids[mask].min()
                global_offsets.append(offset)
                local_ids[mask] -= offset
                n_sent = local_ids[mask].max() + 1
                num_sents.append(n_sent)

                for j in range(int(n_sent)):
                    mask_j = (local_ids == j) & mask
                    if mask_j.any():
                        out.append(seq_out[i, mask_j].mean(dim=-2, keepdim=True))
                    else:
                        out.append(torch.zeros((1, d_model), device=seq_out.device))

                if max_sentences - n_sent > 0:
                    padding = torch.zeros(
                        (int(max_sentences - n_sent), d_model), device=seq_out.device
                    )
                    out.append(padding)
                agg_out.append(torch.cat(out, dim=0))
            return torch.stack(agg_out), global_offsets, num_sents

        agg_output, offsets, num_sents_item = _get_agg_output(sentence_ids, sequence_output)

        logits = self.classifier(agg_output)
        probs = torch.softmax(logits, dim=-1)[:, :, 1]

        def _get_preds(pp, offs, num_s, threshold=0.5, alpha=0.05):
            """Apply threshold and backoff rule"""
            preds = []
            for p, off, ns in zip(pp, offs, num_s):
                if ns == 0:
                    preds.append(torch.tensor([], dtype=torch.long))
                    continue

                rel_probs = p[:ns]
                hits = (rel_probs >= threshold).int()
                if hits.sum() == 0 and rel_probs.max().item() >= alpha:
                    hits[rel_probs.argmax()] = 1
                preds.append(torch.where(hits == 1)[0] + off)
            return preds

        return tuple(_get_preds(probs, offsets, num_sents_item))


@dataclass
class DataCollatorWithPadding:
    """Data collator with padding"""
    pad_kvs: Dict[str, int] = field(default_factory=dict)

    def __call__(self, features: List[Dict[str, Any]]) -> Dict[str, Any]:
        first = features[0]
        batch = {}

        for key, pad_value in self.pad_kvs.items():
            if key in first and first[key] is not None:
                batch[key] = pad_sequence(
                    [torch.tensor(f[key]) for f in features],
                    batch_first=True,
                    padding_value=pad_value,
                )

        return batch


def prepare_input_features(
        tokenizer, examples, max_seq_length=510, stride=128, padding=False
):
    """Prepare input features for the model"""
    tokenized_examples = tokenizer(
        examples["question"],
        examples["context"],
        truncation="only_second",
        max_length=max_seq_length,
        stride=stride,
        return_overflowing_tokens=True,
        padding=padding,
        is_split_into_words=True,
    )

    sample_mapping = tokenized_examples.pop("overflow_to_sample_mapping")
    tokenized_examples["example_id"] = []
    tokenized_examples["word_ids"] = []
    tokenized_examples["sentence_ids"] = []

    for i, sample_index in enumerate(sample_mapping):
        word_ids = tokenized_examples.word_ids(i)
        word_level_sentence_ids = examples["word_level_sentence_ids"][sample_index]

        sequence_ids = tokenized_examples.sequence_ids(i)
        token_start_index = 0
        while token_start_index < len(sequence_ids) and sequence_ids[token_start_index] != 1:
            token_start_index += 1

        sentences_ids = [-100] * token_start_index
        for word_idx in word_ids[token_start_index:]:
            if word_idx is not None and word_idx < len(word_level_sentence_ids):
                sentences_ids.append(word_level_sentence_ids[word_idx])
            else:
                sentences_ids.append(-100)

        tokenized_examples["sentence_ids"].append(sentences_ids)
        tokenized_examples["example_id"].append(examples["id"][sample_index])
        tokenized_examples["word_ids"].append(word_ids)

    for key in ("input_ids", "token_type_ids", "attention_mask", "sentence_ids"):
        tokenized_examples[key] = [seq[:max_seq_length] for seq in tokenized_examples[key]]

    return tokenized_examples


def _process_single(question: str, context: str) -> Dict[str, Any]:
    """Process a single document"""
    doc_sents = nltk.sent_tokenize(context)

    if not doc_sents:
        return {"highlights": []}

    # Calculate sentence positions
    sentence_positions = []
    current_pos = 0
    for sent in doc_sents:
        start_pos = context.find(sent, current_pos)
        if start_pos == -1:
            start_pos = current_pos
        end_pos = start_pos + len(sent)
        sentence_positions.append((start_pos, end_pos))
        current_pos = end_pos

    # Prepare data
    sentence_ids = []
    words = []
    for sid, sent in enumerate(doc_sents):
        sent_words = sent.split()
        words.extend(sent_words)
        sentence_ids.extend([sid] * len(sent_words))

    example_dataset = Dataset.from_dict({
        "question": [[question]],
        "context": [words],
        "word_level_sentence_ids": [sentence_ids],
        "id": [0],
    })

    # Tokenize
    example_dataset = example_dataset.map(
        partial(
            prepare_input_features,
            tokenizer,
            max_seq_length=MAX_LENGTH,
            stride=STRIDE,
        ),
        batched=True,
        remove_columns=example_dataset.column_names,
    )

    # Collate
    example = example_dataset[0]
    features = DataCollatorWithPadding(
        pad_kvs={
            "input_ids": 0,
            "token_type_ids": 0,
            "attention_mask": 0,
            "sentence_ids": -100,
        }
    )([example])

    features = {k: v.to(device) for k, v in features.items()}

    # Inference
    with torch.no_grad():
        sentence_preds = model(**features)

    # Process results
    highlights = []
    if sentence_preds and len(sentence_preds[0]) > 0:
        for pred_idx in sentence_preds[0].cpu().tolist():
            if 0 <= pred_idx < len(doc_sents):
                start_pos, end_pos = sentence_positions[pred_idx]
                highlights.append({
                    "start": start_pos,
                    "end": end_pos
                })

    return {"highlights": highlights}


def handle(data, context):
    """
    TorchServe handler function - single entry point
    Supports both single document and batch processing
    """
    global model, tokenizer, device

    # Handle initialization call (TorchServe calls with None data during init)
    if data is None:
        data = []

    # Initialize on first call
    if model is None:
        properties = context.system_properties if hasattr(context, 'system_properties') else {}
        model_dir = properties.get("model_dir", ".")
        device = torch.device("cuda:" + str(properties.get("gpu_id"))
                              if torch.cuda.is_available() and properties.get("gpu_id") is not None
                              else "cpu")

        # Download NLTK data
        try:
            nltk.download('punkt', quiet=True)
            nltk.download('punkt_tab', quiet=True)
        except:
            logger.warning("Could not download NLTK data, assuming it's already present")

        # Load tokenizer
        try:
            tokenizer = AutoTokenizer.from_pretrained(
                "opensearch-project/opensearch-semantic-highlighter-v1",
                use_fast=True
            )
        except:
            # If can't download, try local
            tokenizer = AutoTokenizer.from_pretrained(
                os.path.join(model_dir, "model_files"),
                use_fast=True,
                local_files_only=True
            )

        # Load model
        model_path = os.path.join(model_dir, "model_files") if os.path.exists(os.path.join(model_dir, "model_files")) else model_dir

        config = AutoConfig.from_pretrained(model_path, local_files_only=True)
        config.num_labels = 2  # Binary classification
        model = BertTaggerForSentenceExtractionWithBackoff(config)

        # Load weights
        weight_file = None
        if os.path.exists(os.path.join(model_path, "pytorch_model.bin")):
            weight_file = os.path.join(model_path, "pytorch_model.bin")
            state_dict = torch.load(weight_file, map_location=device)
        elif os.path.exists(os.path.join(model_path, "model.safetensors")):
            weight_file = os.path.join(model_path, "model.safetensors")
            from safetensors.torch import load_file
            state_dict = load_file(weight_file)
        else:
            # For CI testing, just use random weights
            logger.warning("No model weights found, using random initialization for testing")
            state_dict = model.state_dict()

        model.load_state_dict(state_dict, strict=False)
        model.to(device)
        model.eval()

        logger.info(f"Model loaded successfully on {device}")

    # Process requests
    results = []
    
    # Log raw data for debugging
    logger.info(f"Raw data received: type={type(data)}, len={len(data) if data else 0}")
    if data and len(data) > 0:
        logger.info(f"First item keys: {data[0].keys() if isinstance(data[0], dict) else 'not a dict'}")

    for row in data:
        input_data = row.get("data") or row.get("body")
        if isinstance(input_data, str):
            input_data = json.loads(input_data)

        # Log the input for debugging
        logger.info(f"Processing input_data type: {type(input_data)}, content preview: {str(input_data)[:200]}")
        
        # Handle different input formats
        # Check if input_data is directly an array (test connector with "${parameters.inputs}")
        if isinstance(input_data, list):
            # Direct array format from test connector
            logger.info(f"Processing direct array with {len(input_data)} items")
            batch_results = []
            for item in input_data:
                result = _process_single(item["question"], item["context"])
                batch_results.append(result["highlights"])
            # For direct array, TorchServe expects a single response containing all results
            results.append({"highlights": batch_results})
                
        elif "inputs" in input_data:
            # Batch processing with wrapper - handle both array and JSON string formats
            inputs = input_data["inputs"]
            
            if isinstance(inputs, list):
                logger.info(f"Processing wrapped batch with {len(inputs)} items")
                # For wrapped format, return nested structure like SageMaker
                batch_results = []
                for item in inputs:
                    result = _process_single(item["question"], item["context"])
                    batch_results.append(result["highlights"])
                # Return a single result containing all highlights (SageMaker format)
                results.append({"highlights": batch_results})
            else:
                # For direct string inputs (shouldn't happen with ML Commons connector)
                results.append({"error": "Unexpected inputs format"})

        elif "question" in input_data and "context" in input_data:
            # Single document
            logger.info("Processing single document")
            result = _process_single(input_data["question"], input_data["context"])
            results.append(result)
        else:
            logger.error(f"Invalid input format. Type: {type(input_data)}, Content: {input_data}")
            results.append({"error": "Invalid input format"})

    logger.info(f"Returning {len(results)} results")
    return results
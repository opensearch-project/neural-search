"""
TorchServe Handler for Phi-3-mini-4k-instruct Model
"""

import os
import json
import logging
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment configuration
MAX_LENGTH = 4096  # Phi-3-mini-4k context window
MODEL_NAME = "Phi-3-mini-4k-instruct"

# Global variables for model and tokenizer
model = None
tokenizer = None
device = None

def format_prompt(system_prompt: str, user_prompt: str) -> str:
    """Format the prompt according to Phi-3 instruction format"""
    formatted_prompt = f"System: {system_prompt}\nHuman: {user_prompt}\nAssistant:"
    return formatted_prompt

def generate_response(system_prompt: str, user_prompt: str) -> str:
    """Generate response using Phi-3"""
    formatted_prompt = format_prompt(system_prompt, user_prompt)
    
    inputs = tokenizer(formatted_prompt, return_tensors="pt", truncation=True, max_length=MAX_LENGTH)
    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_length=MAX_LENGTH,
            temperature=0.7,
            do_sample=True,
            pad_token_id=tokenizer.eos_token_id
        )
    
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)
    # Remove the prompt from the response
    response = response[len(formatted_prompt):].strip()
    return response

def handle(data, context):
    """
    TorchServe handler function
    """
    global model, tokenizer, device

    if data is None:
        return None

    # Initialize on first call
    if model is None:
        properties = context.system_properties
        model_dir = properties.get("model_dir", ".")
        device = torch.device("cuda:" + str(properties.get("gpu_id"))
                            if torch.cuda.is_available() and properties.get("gpu_id") is not None
                            else "cpu")

        # Load tokenizer and model
        try:
            model_path = os.path.join(model_dir, "model_files") if os.path.exists(os.path.join(model_dir, "model_files")) else model_dir
            
            tokenizer = AutoTokenizer.from_pretrained(
                model_path,
                trust_remote_code=True,
                local_files_only=True
            )
            
            model = AutoModelForCausalLM.from_pretrained(
                model_path,
                torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
                trust_remote_code=True,
                local_files_only=True
            )
            
            model.to(device)
            model.eval()
            
            logger.info(f"Model loaded successfully on {device}")
        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            raise e

    results = []
    
    # Log raw data for debugging
    logger.info(f"Raw data received: type={type(data)}, len={len(data) if data else 0}")

    for row in data:
        input_data = row.get("data") or row.get("body")
        if isinstance(input_data, str):
            input_data = json.loads(input_data)

        # Log the input for debugging
        logger.info(f"Processing input: {str(input_data)[:200]}")

        try:
            # Validate input format
            if not isinstance(input_data, dict):
                raise ValueError("Input must be a dictionary")
            
            # Extract system prompt
            system = input_data.get("system", [])
            if not system or not isinstance(system, list) or len(system) == 0:
                raise ValueError("System prompt is required")
            system_prompt = system[0].get("text", "")
            
            # Extract user prompt
            messages = input_data.get("messages", [])
            if not messages or not isinstance(messages, list) or len(messages) == 0:
                raise ValueError("User message is required")
            
            user_message = next((msg for msg in messages if msg["role"] == "user"), None)
            if not user_message or not user_message.get("content"):
                raise ValueError("User message content is required")
            
            user_prompt = user_message["content"][0].get("text", "")

            if not system_prompt or not user_prompt:
                raise ValueError("Both system and user prompts are required")

            # Generate response
            response = generate_response(system_prompt, user_prompt)
            
            # Format response
            results.append({
                "model": MODEL_NAME,
                "response": response
            })

        except Exception as e:
            logger.error(f"Error processing request: {str(e)}")
            results.append({
                "error": str(e),
                "model": MODEL_NAME
            })

    logger.info(f"Returning {len(results)} results")
    return results
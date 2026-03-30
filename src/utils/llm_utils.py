import os
import config
import logging
from google import genai
from google.genai import types

from logger_config import logger

MODEL_PRICING = {
    "gemini-2.5-flash": {"input": 0.30, "output": 2.50}, 
    "gemini-2.5-flash-lite": {"input": 0.10, "output": 0.40}
}

def calculate_cost(model_name: str, prompt_tokens: int, completion_tokens: int) -> float:
    rates = MODEL_PRICING.get(model_name, {"input": 0.0, "output": 0.0})
    input_cost = (prompt_tokens / 1_000_000) * rates["input"]
    output_cost = (completion_tokens / 1_000_000) * rates["output"]
    return input_cost + output_cost

_gemini_client = None

def get_gemini_client():
    """
    Ensures the client is initialized only once during the application lifecycle.
    """

    # Without the 'global' keyword above, the assignment below ('=') 
    # would cause the compiler to treat '_gemini_client' as a new 
    # local variable scoped only to this method. The result would 
    # never be stored in the global variable declaeered at the top of the file.
    global _gemini_client
    if _gemini_client is None:
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            logger.error("❌ Critical Failure: GOOGLE_API_KEY environment variable is missing.")
            raise ValueError("GOOGLE_API_KEY is required but not found.")
        
        # Initialize the Google GenAI client
        _gemini_client = genai.Client(api_key=api_key)
        print("✅ Gemini Client successfully initialized.")
    return _gemini_client

def _execute_llm_call(model_config_name: str, system_instruction: str, user_message: str, temperature: float, caller_id: str) -> str:
    """
    Internal helper to handle the actual LLM logic.
    """
    try:
        # Resolve model name from config.py
        raw_name = getattr(config, model_config_name, "gemini-2.5-flash-lite")
        model_name = raw_name.split('/')[-1] if raw_name else "gemini-2.0-flash-lite"
        
        client = get_gemini_client()
        generate_config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=temperature
        )

        response = client.models.generate_content(
            model=model_name,
            contents=user_message,
            config=generate_config
        )

        if not response or not response.text:
            return "Error: LLM returned empty response."
        
        # ---------------------------------------------------------
        # 📊 Token Log
        # ---------------------------------------------------------
        if response.usage_metadata:
            usage = response.usage_metadata
            prompt_tokens = usage.prompt_token_count
            completion_tokens = usage.candidates_token_count
            total_tokens = usage.total_token_count
            
            cost = calculate_cost(model_name, prompt_tokens, completion_tokens)
            
            logger.info(
                f"\n{'='*20} LLM CALLING PRICING START {'='*20}\n"
                f"👤 [Caller] : {caller_id}\n"
                f"🤖 [Model]  : {model_name}\n"
                f"🔢 [Tokens] : {total_tokens} (Prompt: {prompt_tokens}, Completion: {completion_tokens})\n"
                f"💰 [Cost]   : ${cost:.6f} USD\n"
                f"{'='*21} LLM CALLING PRICING END {'='*21}\n"
            )
        else:
            logger.warning(f"⚠️ [Caller: {caller_id}] No usage metadata returned from API.")
        # ---------------------------------------------------------
        return response.text
    
    except Exception as e:
        logger.error(f"❌ LLM Call Failed ({model_config_name}): {str(e)}", exc_info=True)
        raise e

def call_planning_llm(system_instruction: str, user_message: str, temperature: float = 0.3, caller_id: str = "Planning_Agent") -> str:
    """
    Uses the High-Intelligence Planning Model (e.g., Gemini 2.5 Flash).
    Best for: Complex reasoning, multi-step plans, and data synthesis.
    """
    print("DEBUG: 🧠 Invoking Planning LLM...")
    return _execute_llm_call('PLANNING_MODEL_NAME', system_instruction, user_message, temperature, caller_id)

def call_general_llm(system_instruction: str, user_message: str, temperature: float = 0.5, caller_id: str = "General_Agent") -> str:
    """
    Uses the Lightweight General Model (e.g., Gemini 2.5 Flash Lite).
    Best for: Simple tasks, translations, or classification.
    """
    print("DEBUG: 🧠 Invoking General LLM...")
    return _execute_llm_call('GENERAL_MODEL_NAME', system_instruction, user_message, temperature, caller_id)
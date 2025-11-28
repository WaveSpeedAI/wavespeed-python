"""WaveSpeed | Test Worker | handler.py.

Simple test handler for validating the wavespeed serverless SDK.
"""

import time

import wavespeed.serverless as serverless
from wavespeed.serverless.utils import validate

INPUT_SCHEMA = {
    "prompt": {"type": str, "required": True},
    "delay": {
        "type": float,
        "required": False,
        "default": 0.0,
        "constraints": lambda x: 0 <= x <= 10,
    },
    "uppercase": {"type": bool, "required": False, "default": False},
    "repeat": {
        "type": int,
        "required": False,
        "default": 1,
        "constraints": lambda x: 1 <= x <= 10,
    },
}


def handler(job):
    """Process a prompt with optional transformations.

    Input:
        - prompt: str - The prompt to process
        - delay: float - Optional delay in seconds (0-10)
        - uppercase: bool - Whether to convert to uppercase
        - repeat: int - Number of times to repeat the prompt (1-10)

    Output:
        - result: str - The processed prompt
        - processing_time: float - Time taken to process
    """
    job_input = job["input"]

    # Validate input
    validated = validate(job_input, INPUT_SCHEMA)
    if "errors" in validated:
        return {"error": f"Validation errors: {validated['errors']}"}

    valid_input = validated["validated_input"]
    prompt = valid_input["prompt"]
    delay = valid_input["delay"]
    uppercase = valid_input["uppercase"]
    repeat = valid_input["repeat"]

    start_time = time.time()

    # Simulate processing delay
    if delay > 0:
        time.sleep(delay)

    # Process the prompt
    if uppercase:
        prompt = prompt.upper()

    result = " ".join([prompt] * repeat)

    processing_time = time.time() - start_time

    return {
        "result": result,
        "processing_time": round(processing_time, 3),
        "job_id": job["id"],
    }


def generator_handler(job):
    """Yield progress updates for a streaming job.

    Input:
        - prompt: str - The prompt to process
        - chunks: int - Number of chunks to yield (1-10)
        - delay: float - Delay between chunks (0-5)
    """
    job_input = job["input"]

    prompt = job_input.get("prompt", "Hello")
    chunks = min(10, max(1, job_input.get("chunks", 3)))
    delay = min(5, max(0, job_input.get("delay", 0.5)))

    for i in range(chunks):
        if delay > 0:
            time.sleep(delay)
        yield {
            "chunk": i + 1,
            "total": chunks,
            "partial": f"{prompt} (part {i + 1}/{chunks})",
        }


if __name__ == "__main__":
    serverless.start({"handler": handler})

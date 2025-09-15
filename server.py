import torch
from flask import Flask, request, jsonify
from transformers import AutoModelForCausalLM, AutoTokenizer, GenerationConfig

MODEL_ID = "Qwen/Qwen2.5-3B-Instruct"  # можно поменять на 7B при мощном ПК
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# Подгружаем токенайзер и модель
tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, use_fast=True)
# CPU: float32 по умолчанию; GPU: bf16/16bit, 4bit при наличии bitsandbytes
kwargs = {}
if DEVICE == "cuda":
    try:
        import bitsandbytes  # noqa

        kwargs = dict(load_in_4bit=True, device_map="auto")
    except Exception:
        kwargs = dict(torch_dtype=torch.bfloat16, device_map="auto")
else:
    kwargs = dict(device_map="auto")  # на CPU автоматически разместит на CPU

model = AutoModelForCausalLM.from_pretrained(MODEL_ID, **kwargs)

gen_cfg = GenerationConfig(
    max_new_tokens=512,
    temperature=0.7,
    top_p=0.9,
)

app = Flask(__name__)


def chat(messages):
    """
    messages = [
      {"role": "system", "content": "..."},
      {"role": "user", "content": "..."},
      {"role": "assistant", "content": "..."},
      ...
    ]
    """
    # применяем чат-шаблон модели
    prompt = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True
    )
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    with torch.no_grad():
        output_ids = model.generate(
            **inputs,
            max_new_tokens=gen_cfg.max_new_tokens,
            temperature=gen_cfg.temperature,
            top_p=gen_cfg.top_p,
            do_sample=True,
            eos_token_id=tokenizer.eos_token_id,
        )
    # отрезаем префикс запроса
    generated_ids = output_ids[0][inputs["input_ids"].shape[1]:]
    text = tokenizer.decode(generated_ids, skip_special_tokens=True)
    return text.strip()


@app.route("/v1/chat/completions", methods=["POST"])
def chat_completions():
    payload = request.get_json(force=True)
    messages = payload.get("messages", [])
    reply = chat(messages)

    # Возвращаем ответ в стиле OpenAI
    return jsonify({
        "id": "chatcmpl-local-1",
        "object": "chat.completion",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": reply},
            "finish_reason": "stop"
        }],
        "model": MODEL_ID
    })


if __name__ == "__main__":
    # Запуск: python server.py  -> http://localhost:8000
    app.run(host="0.0.0.0", port=8000)

# 🧑‍🚀 Data Platform Copilot (MVP)

**Data Platform Copilot** is a GenAI-powered assistant that translates natural language questions into **safe, validated SQL queries** against your Data Warehouse.  
It is designed as a foundation for a full **AI + Data Management platform**, combining **Data Engineering** and **AI Engineering** skills in one end-to-end system.

---

## ✨ Features (MVP)

- 🗨️ **Natural language → SQL** with LLM (OpenAI, OpenRouter, Ollama supported)  
- 🔒 **Safety first**:  
  - Only `SELECT` statements are allowed  
  - Forbidden keyword check (DDL/DML)  
  - Auto `LIMIT` injection  
  - Query timeout & row caps  
- 📑 **Schema-aware RAG**: models see schema docs to generate correct queries  
- 📊 **Preview & Explain**: returns query plan + top rows of result  
- 🐳 **Containerized**: easy `docker-compose up` deployment  
- 🧪 **Synthetic dataset** (customers, orders, items) for demo out-of-the-box  

---

## 🏗️ Project Structure

```

data-platform-copilot/
├─ README.md                # This file
├─ .env.example             # Example environment config
├─ docker-compose.yml
├─ infra/
│  ├─ Dockerfile.api        # API container
│  └─ requirements.txt
├─ data/                    # Synthetic CSV data
│  ├─ customers.csv
│  ├─ orders.csv
│  └─ items.csv
├─ db/
│  ├─ init\_duckdb.py        # Initialize DuckDB database
│  └─ schema\_docs.md        # Human-readable schema description
├─ src/
│  ├─ api/main.py           # FastAPI entrypoint
│  ├─ agent/                # Prompting & orchestration
│  ├─ llm/                  # LLM provider abstraction
│  ├─ tools/                # SQL runner, schema introspection
│  └─ rag/                  # (future) retrieval components
└─ tests/
└─ e2e\_eval.md           # Simple evaluation prompts

````

---

## 🚀 Quickstart

### 1. Clone & prepare
```bash
git clone https://github.com/<yourname>/data-platform-copilot.git
cd data-platform-copilot
cp .env.example .env
````

### 2. Configure environment

Choose an LLM provider in `.env`:

**OpenAI**

```env
LLM_PROVIDER=openai
LLM_MODEL=gpt-4o-mini
OPENAI_API_KEY=sk-...
```

**OpenRouter**

```env
LLM_PROVIDER=openrouter
LLM_MODEL=meta-llama/llama-3.1-70b-instruct:free
OPENROUTER_API_KEY=or-...
```

**Ollama (local)**

```env
LLM_PROVIDER=ollama
LLM_MODEL=llama3.1
OLLAMA_BASE_URL=http://host.docker.internal:11434
```

### 3. Initialize demo database

```bash
docker compose run --rm api python db/init_duckdb.py
```

### 4. Run the API

```bash
docker compose up --build
```

The service will be available at:
👉 [http://localhost:8000/docs](http://localhost:8000/docs) (Swagger UI)

---

## 🔎 Example Usage

**Request**

```bash
curl -X POST http://localhost:8000/chat \
  -H "content-type: application/json" \
  -d '{"question":"Top 5 countries by revenue in 2024"}'
```

**Response**

```json
{
  "sql": "SELECT c.country, SUM(o.total_amount) AS revenue
          FROM orders o JOIN customers c USING(customer_id)
          WHERE o.order_ts >= '2024-01-01'
            AND o.order_ts < '2025-01-01'
          GROUP BY 1
          ORDER BY revenue DESC
          LIMIT 5;",
  "plan": "... (DuckDB query plan) ...",
  "rows": [
    {"country": "PL", "revenue": 50230.5},
    {"country": "DE", "revenue": 42110.3},
    ...
  ]
}
```

---

## 🛡️ Safety Considerations

* ✅ Strict SQL validation (no DDL/DML, `SELECT`-only)
* ✅ Automatic `LIMIT` insertion to prevent dataset explosion
* ✅ Configurable timeout (`QUERY_TIMEOUT_MS`)
* ✅ Read-only database for demo setup

Future steps: role-based access control, PII masking, audit logging.

---

## 📈 Roadmap

* **Step 1 (MVP)**: NL → Safe SQL over demo DB ✅
* **Step 2**: dbt model generation + PR automation
* **Step 3**: Data Quality checks (Great Expectations)
* **Step 4**: Orchestrator integration (Airflow/Prefect)
* **Step 5**: Security hardening (RBAC, masking, guardrails)
* **Step 6**: Evaluation framework & observability

---

## 📊 Demo Dataset

The project comes with synthetic data:

* `customers` – 250 users across 10 countries
* `orders` – 3,000 transactions (2023–2025)
* `items` – line items with SKU, quantity, unit price

This ensures reproducible demos without external dependencies.

---

## 🤝 Contributing

Contributions are welcome!
Feel free to open Issues or PRs with improvements to:

* Prompt engineering
* New LLM providers
* Additional safety checks
* Observability & monitoring

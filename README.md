# 🚀 Data Pilot

**Data Pilot** is a comprehensive data platform that combines AI-powered SQL generation with robust data engineering capabilities. It integrates workflow orchestration, data transformations, and AI services into a unified platform for modern data operations.

---

## ✨ Features

- 🤖 **AI-Powered SQL Generation**: Natural language to SQL using OpenAI, OpenRouter, or local Ollama models
- 🔄 **Workflow Orchestration**: Prefect-based data pipelines with scheduling and monitoring
- 🏗️ **Data Transformations**: dbt integration for data modeling and transformations
- 🛡️ **Safety First**: Query validation, timeouts, row limits, and read-only operations
- 📊 **Monitoring & Metrics**: Prometheus integration with health checks
- 🐳 **Containerized Deployment**: Docker Compose setup for easy deployment
- 📈 **Scalable Architecture**: FastAPI backend with async processing

---

## 🛠️ Technology Stack

### Core Technologies
- **Backend**: FastAPI, Python 3.x
- **Orchestration**: Prefect 2.x
- **Transformations**: dbt (data build tool)
- **Databases**: DuckDB, PostgreSQL support
- **AI/ML**: OpenAI API, OpenRouter, Ollama
- **Containerization**: Docker, Docker Compose

### Key Dependencies
- `fastapi` - Web framework
- `prefect` - Workflow orchestration
- `duckdb` - Analytics database
- `psycopg2-binary` - PostgreSQL adapter
- `pandas` - Data manipulation
- `openai` - AI integration
- `prometheus-fastapi-instrumentator` - Metrics
- `uvicorn` - ASGI server

---

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+ (for local development)
- Git

### 1. Clone and Setup
```bash
    git clone <repository-url>
    cd data-pilot
    cp .env.example .env
```

### 2. Configure Environment Variables
Edit `.env` file with your settings:

**For OpenAI:**
```env
LLM_PROVIDER=openai
LLM_MODEL=gpt-4o-mini
OPENAI_API_KEY=sk-your-key-here
```

**For OpenRouter:**
```env
LLM_PROVIDER=openrouter
LLM_MODEL=meta-llama/llama-3.1-70b-instruct:free
OPENROUTER_API_KEY=sk-or-your-key-here
```

**For Local Ollama:**
```env
LLM_PROVIDER=ollama
LLM_MODEL=llama3.1
OLLAMA_BASE_URL=http://host.docker.internal:11434
```

### 3. Start Services
```bash
    # Start Prefect server and worker
    docker compose up --build
    
    # Optional: Start API service (uncomment in docker-compose.yaml)
    # The API will be available at http://localhost:8080
```

### 4. Access Services
- **Prefect UI**: http://localhost:4200
- **API Documentation**: http://localhost:8080/docs (if API service is enabled)
- **Health Check**: http://localhost:8080/health

---

## 📁 Project Structure

```
data-pilot/
├── README.md                    # This file
├── docker-compose.yaml          # Container orchestration
├── .env.example                 # Environment template
├── requirements.txt             # Python dependencies
├── Dockerfile.api              # API service container
├── entrypoint.sh               # Container entry script
├── query.http                  # Example API requests
│
├── src/                        # FastAPI application
│   ├── main.py                 # FastAPI app entry point
│   ├── routes.py               # API routes
│   ├── chain.py                # LLM prompt templates
│   ├── provider.py             # LLM provider abstraction
│   ├── sql_runner.py           # Query execution
│   ├── settings.py             # Configuration
│   ├── schema_docs.py          # Schema documentation
│   ├── orchestrator.py         # Workflow integration
│   ├── dbt_generator.py        # dbt model generation
│   ├── github_client.py        # Git integration
│   ├── metrics.py              # Prometheus metrics
│   └── dq.py                   # Data quality checks
│
├── flows/                      # Prefect workflows
│   ├── prefect.yaml            # Prefect project config
│   ├── daily_sales.py          # Example ETL flow
│   ├── main.py                 # Flow entry point
│   └── requirements-flows.txt  # Flow dependencies
│
├── infrastructure/             # Docker configurations
│   └── Dockerfile.prefect-worker
│
├── dbt/                        # dbt transformations
│   └── models/                 # dbt models
│
├── data/                       # Sample/demo data
├── db/                         # Database files
├── tests/                      # Test files
└── fixture-folder/             # Test fixtures
```

---

## 🔧 Usage

### Running Prefect Flows

**Execute flows directly:**
```bash
# Run daily sales flow
python flows/daily_sales.py

# Run with parameters
cd flows && python -c "from daily_sales import daily_sales_flow; daily_sales_flow(days_back=7)"
```

**Deploy flows:**
```bash
cd flows
prefect deploy --name daily-sales-deployment
```

### API Usage

**Natural Language to SQL:**
```bash
curl -X POST http://localhost:8080/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "Show me top 10 customers by revenue this year"}'
```

**Health and Status:**
```bash
# Health check
curl http://localhost:8080/health

# Database schema
curl http://localhost:8080/schema

# Metrics
curl http://localhost:8080/metrics
```

---

## 🌐 Environment Variables

### Required Variables
- `LLM_PROVIDER` - AI provider: `openai`, `openrouter`, or `ollama`
- `LLM_MODEL` - Model name (depends on provider)
- `OPENAI_API_KEY` - OpenAI API key (if using OpenAI)
- `OPENROUTER_API_KEY` - OpenRouter API key (if using OpenRouter)

### Optional Variables
- `OLLAMA_BASE_URL` - Ollama server URL (default: `http://host.docker.internal:11434`)
- `ROW_LIMIT` - Query result limit (default: `200`)
- `QUERY_TIMEOUT_MS` - Query timeout in milliseconds (default: `8000`)
- `PREFECT_API_URL` - Prefect server URL (default: `http://prefect:4200/api`)
- `PREFECT_LOGGING_LEVEL` - Logging level (default: `INFO`)
- `WORK_QUEUE` - Prefect work queue name (default: `default`)

---

## 🧪 Testing

```bash
# Run tests
python -m pytest tests/

# Run with coverage
python -m pytest tests/ --cov=src/

# Run specific test file
python -m pytest tests/test_specific.py
```

---

## 📊 Monitoring

The platform includes comprehensive monitoring:

- **Prometheus Metrics**: Available at `/metrics` endpoint
- **Health Checks**: Available at `/health` endpoint
- **Prefect UI**: Workflow monitoring and management
- **Application Logs**: Structured logging with configurable levels

---

## 🔒 Security & Safety

- **Query Validation**: Only SELECT statements allowed
- **Automatic Limits**: Row limits and query timeouts
- **Read-Only Operations**: No DDL/DML operations permitted
- **Environment Isolation**: Containerized deployment
- **API Key Management**: Secure credential handling

---

## 🗓️ Development

### Local Development Setup
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r flows/requirements-flows.txt

# Run API locally
python src/main.py

# Run Prefect flows locally
cd flows && python daily_sales.py
```

### Adding New Flows
1. Create new flow file in `flows/` directory
2. Update `prefect.yaml` with deployment configuration
3. Test locally and deploy using `prefect deploy`

---

## 📋 TODO

- [ ] **License**: Add project license file
- [ ] **Authentication**: Implement user authentication and RBAC
- [ ] **Data Quality**: Expand Great Expectations integration
- [ ] **CI/CD**: Add automated testing and deployment pipelines
- [ ] **Documentation**: Add API documentation and user guides
- [ ] **Performance**: Add query optimization and caching
- [ ] **Observability**: Enhanced logging and tracing

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📞 Support

For questions, issues, or contributions:
- Open an issue in the repository
- Review existing documentation
- Check Prefect and FastAPI documentation for framework-specific questions

---

*Last updated: 2025-09-27*
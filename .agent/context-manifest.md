# OptiVox-v1 — Context Manifest (Phase 0)
Generated: 2026-08-25

## Stack

### Backend
- Framework: FastAPI 0.115+, Python 3.11+
- LLM Layer: LiteLLM (direct) + LangChain Google GenAI (via LangGraph)
- Agent: LangGraph StateGraph (RAG → Architect → Generator → Reviewer → Corrector → Finalize)
- Vector Store: Pinecone (cloud, namespaced)
- Embeddings: gemini-embedding-001 (768 dim, 5 RPM / 100 RPD free tier)
- LLM Model: gemini-2.5-flash
- DB drivers: SQLAlchemy + PyMySQL + psycopg2 + oracledb
- SQL Validator: sqlglot
- Audit: SQLite (/tmp/audit.db default)
- CSV sandbox: SQLite files in ./csv_databases/
- Cache: LRU+TTL in-memory (200 entries, 30min)

### Frontend
- React 19.2 + Vite 8.0, Axios, Vanilla CSS

### CI/CD
- No automated CI (no GitHub Actions / GitLab CI)
- Procfile for Render, vercel.json for Vercel

## Entry Points
Backend: uvicorn app.main:app --host 0.0.0.0 --port 8000
Frontend: cd frontend && npm run dev

## Test Status
- No pytest test suite exists under app/
- Only test_query.py (manual integration, requires running server)
- Backend imports: ALL CLEAN

## Critical User Journeys
1. Playground: GET /tasks → POST /run → POST /check
2. AI Hint: POST /hint (requires Gemini key)
3. DB Connect: POST /connect → POST /schema → POST /query → POST /execute
4. Agentic SQL: POST /query/agent (LangGraph pipeline)
5. CSV DB: POST /csvdb/upload → GET /list → GET /{id}/schema → POST /{id}/query
6. ADIA Suite: /adia/nl-sql, /adia/teach, /adia/optimize, /adia/schema-analysis
7. WebSocket: /ws/query, /ws/tutor, /ws/chat

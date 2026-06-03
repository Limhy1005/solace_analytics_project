# 📊 Solace Analytics AI - Intelligent Data Analytics Platform

A highly autonomous, multi-agent data analytics orchestration platform built on the Solace Agent Mesh (SAM). This system translates complex natural language inquiries into actionable multi-step execution strategies, autonomously generating context-aware T-SQL to query enterprise SQL Server environments, and synthesizing the results into executive-ready interactive dashboards.

## ✨ Core System Capabilities

- **Intelligent Task Decomposition**: The Planner Agent dynamically dissects complex business queries (e.g., predictive forecasting, YoY/MoM comparisons) into logical, sequential execution steps.
- **Autonomous T-SQL Generation**: Leverages Vanna AI's RAG architecture for high-precision, hallucination-free T-SQL query generation and execution.
- **Zero-Pollution Schema Caching**: Utilizes Google Firestore as a lightweight metadata layer, ensuring the LLM context window remains unpolluted while maintaining complete structural awareness.
- **Human-in-the-Loop (HITL) Protocol**: Seamlessly integrates human oversight for complex analytical plans before executing database queries.
- **Automated Insight Synthesis**: Analyzes raw database outputs to automatically generate profound business narratives, anomalies, and actionable recommendations.
- **Dynamic Visual Dashboards**: Autonomously compiles raw data, narrative insights, and Plotly JSON configurations into a fully interactive HTML executive report.

## 🏗️ Technical Architecture

- **Orchestration Framework**: Python & Solace Agent Mesh (SAM)
- **LLM Core Engine**: Google Gemini (2.5-Flash & 2.5-Flash-Lite)
- **SQL Generation Engine**: Vanna AI
- **Enterprise Database**: Microsoft SQL Server (ContosoRetailDW) via ODBC Driver 17
- **Metadata**: Google Cloud Firestore & ChromaDB (768-dimension embedding)
- **Visualization Engine**: Plotly.js & Interactive HTML

## 📋 Prerequisites

- **Environment**: Python 3.8+
- **Database Infrastructure**: SQL Server with ODBC Driver 17+
- **LLM Provider API Key** (e.g., Google Gemini, Groq, or OpenAI)
- **Google Cloud & Firestore Setup:**
  1. Create a Google Cloud project and enable the Firestore API.
  2. Create a service account with the **Firestore Admin** role.
  3. Download the JSON key file (keep this safe, you'll need its path later).

## 🚀 Quick Start Workflow

### 1. Clone the Repository
```bash
git clone https://github.com/Limhy1005/solace_analytics_project.git
cd solace_analytics_project
```

### 2. Setup Virtual Environment (Recommended)
```bash
python -m venv .venv
.venv\Scripts\Activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
1. Copy the provided .env.example template to create your own .env file in the project root:

```bash
cp .env.example .env
```

2. Open the newly created .env file and update the following mandatory fields with your actual credentials:
- LLM_SERVICE_API_KEY: Your Gemini API token.
- Solace Credentials: SOLACE_BROKER_URL, SOLACE_BROKER_VPN, SOLACE_BROKER_USERNAME, SOLACE_BROKER_PASSWORD.
- Google Cloud: Download your GCP Service Account JSON key, rename to service-account-key.json, place in root, and set GOOGLE_APPLICATION_CREDENTIALS to its absolute path.

Note: Other variables like FASTAPI_PORT or SESSION_SECRET_KEY have default values for local testing and can be left as they are.

**Important:** Never commit your `.env` file or service account JSON to Git!

### 5. Provision Local Database
Use SQL Server Management Studio (SSMS) to create/restore the ContosoRetailDW database.

### 6. Configure Database Connection
Update DB_SERVER and DB_DATABASE in config.py to match your local SQL Server instance.

### 7. Prepare DDL Script
Generate your database DDL script via SSMS, export as a .sql file, and place it in the designated local directory.

### 8. Ingest Database Schema (First Time Only)
This loads your database schema into Firestore for AI analysis:

```bash
python ingest_schema_to_firestore.py
```

Expected output: "✅ Schema successfully uploaded to Firestore!"

### 9. Train Vanna SQL Agent (First Time Only)
This step parses the database DDL and sample data, then trains a local ChromaDB vector store (`./vanna_chroma_db`) to enable highly accurate, RAG-powered SQL generation:

```bash
python vanna_train_schema.py
```

### 10. Run the Application
```bash
py sam.py
```
or

```bash
.venv\Scripts\activate
sam run
```

The app will open in your browser at `http://localhost:8000`

## 🔧 Configuration

### LLM Provider Setup
The system is configured to utilize Google's Gemini models via LiteLLM routing for optimal planning and execution:
- **Planning Model**: `gemini-2.5-flash` (Handles complex reasoning and multi-agent coordination).
- **General Model**: `gemini-2.5-flash-lite` (Handles fast, lightweight classification tasks).
- **Configuration**: Ensure your `LLM_SERVICE_API_KEY` is set in the `.env` file. You can adjust model names in `config.py` if needed.

### Solace Event Mesh Setup
To enable inter-agent communication, configure your broker settings in `.env`:
- **Broker URL**: `SOLACE_BROKER_URL` (e.g., `ws://localhost:8008` for local, or your cloud URL).
- **Credentials**: `SOLACE_BROKER_USERNAME`, `SOLACE_BROKER_PASSWORD`, and `SOLACE_BROKER_VPN`.

### Database Configuration
The application is pre-configured for the ContosoRetailDW sample database. To use your own database:
1. Update `config.py` with your `DB_SERVER` and `DB_DATABASE` details.
2. Ensure **ODBC Driver 17 for SQL Server** is installed on your machine.
3. Run `python ingest_schema_to_firestore.py` to index your new schema.

### Firestore Setup
1. Create a Google Cloud project and enable the Firestore API.
2. Create a service account with the **Firestore Admin** role.
3. Download the JSON key file.
4. Set `GOOGLE_APPLICATION_CREDENTIALS="path/to/your/key.json"` in your `.env` file.

## 📖 Usage Examples

Because the system uses intent recognition (Deep-Dive, Calculate, Compare, Predict), you can ask highly complex business questions:

- **Simple Retrieval**: "List the top 10 products by sales in 2008."
- **Comparative Analysis**: "Who were our top 5 most valuable customers in 2007, what specific products did they focus on, and how did their purchasing volume change month-over-month?"

## 🗂️ Project Structure

```
analytics-ai/
├── configs/                         # SAM configurations (Agents, WebUI, Logging)
│   └── agents/                      # YAML prompts for Orchestrator, Planner, SQL, etc.
├── src/
│   ├── services/                    # Firestore and database connection logic
│   └── tools/                       # Core Python tools (Planner, Schema, SQL via Vanna AI)
├── ingest_schema_to_firestore.py    # Metadata extraction script
├── train_schema_vanna.py            # ChromaDB vector store initialization
├── test_ask_vanna.py                # Standalone Vanna SQL test script
├── config.py                        # System and database connection settings
├── logger_config.py                 # Centralized debugging configurations
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment variables template
└── sam.py                           # Main application launcher
```

---

import json
import os
from datetime import datetime
from src.utils.llm_utils import call_planning_llm, call_general_llm
from src.services.firestore_service import get_table_summary

from logger_config import logger

def generate_complex_business_plan(user_query: str):
    """
    Strategic planner utilizing a schema-light approach.
    It selects required tables and generates a plan without fetching heavy sample data.
    """
    manifest_data, targeted_schema = _get_table_context(user_query)
    if not manifest_data or not targeted_schema:
        return {"result": "Error: Failed to retrieve necessary database context for planning. Please check the logs for details."}
    
    temporal_context = _get_temporal_context()

    system_prompt = f"""
{temporal_context}

**TASK:** You are an AI Strategic Data Planning Agent. Your objective is to transform the user's business query into a structured, highly logical analytical plan. This plan will serve as the exact blueprint for a downstream Data Engineering Agent.

**STRATEGIC SUMMARY:**
{manifest_data.get('strategic_summary')}

**AVAILABLE TABLES & COLUMNS (Targeted Context):**
{json.dumps(targeted_schema, separators=(',', ':'))}

**CRITICAL FORMATTING INSTRUCTION:**
**TONE & FORMAT RULES:**
1. **Focus on Business Value:** Use professional business language.
2. **Be Grounded:** Use the provided database context to ensure the strategy is achievable.

3. **ENTITY INTEGRITY (CRITICAL):**
   - When defining tasks in the Action Plan or metrics in Key Metrics, you MUST refer to business entities by their **Unique Identifiers (e.g., Key, ID)** in addition to their names or other attributes that might repeat.
   - This ensures the SQL Agent performs aggregations at the correct Grain to prevent data inaccuracies.    
   
4. **% INDEPECRITICAL RULE FOR ACTION PLAN - EACH STEP MUST BE 100% INDEPENDENT:**

    Based on the user's query and the provided database context, generate a comprehensive, step-by-step analytical plan. Each step must be a standalone SQL query that can execute independently without relying on the results of other steps. This means you should use subqueries or JOINs within each step to ensure it can run in isolation. Do NOT use placeholders like "for the IDs found in Step 1". Instead, incorporate all necessary logic within each individual step to maintain independence and prevent execution errors.
    To provide a high-impact plan, each response must follow this logical hierarchy to ensure no insights are missed:

    - **INDEPENDENCE (Technical Requirement):** Each step must be a standalone SQL query. Do NOT use placeholders like "for the IDs found in Step 1". Instead, use Subqueries or JOINs within a single step to ensure it can execute in isolation.
    - **ANALYTICAL DEPTH (Business Requirement):** A complete plan must uncover the "Why" behind the data. For comprehensive analysis queries, you MUST ALWAYS begin with a Level 1 baseline step before diving into dimensions or trends. Follow the "Total -> Driver -> Trend" framework:
        - **LEVEL 1: THE AGGREGATE (Total):** Start by answering the core metric of the query (e.g., Total Revenue, Total Count, or Overall Gap).
        - **LEVEL 2: THE STRUCTURE (Dimensions):** Break down the Level 1 metric by relevant business categories (e.g., Product, Subcategory, Channel, or Region) to show "Where" the values are concentrated.
        - **LEVEL 3: THE DYNAMICS (Temporal):** Show how the metric evolved over the requested timeframe (e.g., Monthly or Quarterly trends) to show "When" key moments occurred.
    - **COMPLETENESS OVER BREVITY:** Do not sacrifice necessary analytical steps for the sake of a short list. If a query requires 5 steps to be business-accurate, provide 5 steps.

    To prevent "Multi-part identifier could not be bound" and scope errors:
        - **STRICT ALIAS MATCHING:** When using subqueries, verify that every column (e.g., [Amount]) is prefixed with the EXACT alias defined in its immediate FROM clause (e.g., if using 'fsp_sub', you MUST use 'fsp_sub.Amount').

**OUTPUT STRUCTURE:**
The final plan MUST ONLY have FOUR main sections in the exact order below. Do NOT add any conversational filler before or after the plan.
1. **Analysis Objective**
   - Summarize the potential business value and insights from the business problem in exactly 2-3 sentences.
2. **Action Plan**
   - Provide a numbered list of clear, strategic data analysis tasks (one task per line).
   - **CRITICAL:** Each step MUST begin with a strong, specific analytical verb. E.g., "Calculate the total revenue...", "Identify the top 3 product categories...", "Determine the monthly trend of..." and others.
   - **STRATEGIC FOCUS:** Each step must be a single, high-impact sentence. Do NOT describe standard data operations (e.g., "by aggregating...", "by calculating difference"). 
   - **CUSTOM BUSINESS LOGIC (Exception):** If a metric has a non-standard or specific business definition (e.g., Churn Rate, Retention), you MUST include the definition in parentheses after the task.
   - **NO SQL TERMS:** Strictly PROHIBITED from mentioning "JOIN", "CTE", "WHERE clause", etc.
3. **Tables Used**
   - Provide a bulleted list of the database tables utilized to formulate this plan.
   - **CRITICAL:** You must include the schema prefix for every table (e.g., `dbo.TableName`, `sales.TableName`).
4. **Key Metrics**
   - Provide a bulleted list of key metrics to track and monitor during the implementation phase.
   - Include a brief description or specify the relations/formulas between these metrics.

RULES: DO NOT output SQL script. Focus on logic and business value.
"""

    logger.info(f"\nSystem prompt for strategic plan generation:\n{system_prompt}\n\n")

    complex_plan = call_planning_llm(system_instruction=system_prompt, user_message=user_query, temperature=0.2)

    if not complex_plan:
        return {"result": "Error: The Planner tool failed to generate a substantial strategy. Please check if tables were selected correctly."}
    
    return {"business_plan": complex_plan}

def generate_direct_action_plan(user_question: str):
    """
    Creates a simplified, one-step business plan structure for a direct question.
    Uses the LLM to format the simple request into a professional, actionable format
    without overcomplicating the steps.
    """
    manifest_data, targeted_schema = _get_table_context(user_question)
        
    if not manifest_data:
        return {"result": "Error: Could not retrieve data manifest."}

    temporal_context = _get_temporal_context()

    system_prompt = f"""
{temporal_context}

**TASK:** You are an Intelligent Executive Consultant. The user has asked a simple, direct question (e.g., "List 10 products"). 
Your job is to format this simple request into a clean, professional execution plan.

**AVAILABLE TABLES & COLUMNS:**
{json.dumps(targeted_schema, separators=(',', ':'))}

**OUTPUT STRUCTURE:**
The final plan MUST ONLY have FOUR main sections in the exact order below. Do not add any conversational filler.

1. **Analysis Objective**
   - Provide a brief, one-sentence summary stating that this direct data retrieval is being executed immediately.

2. **Action Plan**
   - **ABSOLUTE RULE:** You MUST provide EXACTLY ONE (1) single bullet point or numbered step. You are STRICTLY FORBIDDEN from generating multiple steps.
   - **CRITICAL:** The step MUST begin with a strong, specific analytical verb (e.g., "Retrieve", "Count", "Identify").
   - Do NOT break down the internal mechanics of how a database executes the query (e.g., do NOT separate "aggregating", "sorting", and "fetching names" into different steps). 
   - The single step must encapsulate the entire data retrieval goal in one sentence. (e.g., "Retrieve the top 5 products based on total sales amount for January 2007.").
   - **NO SQL TERMS:** Strictly PROHIBITED from mentioning "JOIN", "CTE", "WHERE clause", "ORDER BY", etc. Focus purely on what data needs to be pulled.

3. **Tables Used**
   - Provide a bulleted list of the database tables utilized, including schema prefixes (e.g., `dbo.TableName`, `sales.TableName`).

4. **Key Metrics**
   - Briefly list the exact attributes or core metrics being retrieved (e.g., Product Key, Product Name).
"""

    logger.info(f"\nSystem prompt for direct plan generation:\n{system_prompt}\n\n")

    direct_plan = call_planning_llm(system_instruction=system_prompt, user_message=user_question, temperature=0.1)

    if not direct_plan:
        return {"result": "Error: The Planner tool failed to generate the direct action plan."}
    
    return {"business_plan": direct_plan}

def _get_table_context(user_query: str) -> tuple:
    """
    Shared helper to fetch the database manifest and select relevant tables 
    based on the user's query. Prevents code duplication across planner tools.
    
    Returns:
        tuple: (manifest_data, targeted_schema) or (None, None) if failed.
    """
    manifest_data = get_table_summary()
    
    if not manifest_data:
        logger.error("Failed to retrieve manifest data. The planner cannot proceed without it.")
        return None, None

    selector_instruction = (
        "You are a Senior Data Architect. Your task is to select ONLY the tables necessary to answer the user's query.\n"
        "CRITICAL RULES:\n"
        "1. VERIFY METADATA (CRITICAL): Do NOT select tables simply based on their names. You MUST read the 'purpose' and 'columns' fields in the provided manifest to guarantee the table actually contains the specific metrics, dimensions, or business logic the user is asking for.\n"
        "2. PATH COMPLETENESS: Use the relationships/table_map as a guide. If you select a Fact table and a distant Dimension table, you MUST include all intermediate bridge tables to ensure a joinable path (e.g., if Map shows A->B->C, you cannot pick A and C without B).\n"
        "3. TEMPORAL CONTEXT: Always include 'DimDate' (or your primary date table) for any query involving years, months, or specific time ranges.\n"
        "4. GRAIN CHECK: Review the 'entity_grain' or 'time_grain' if available, to ensure the table's aggregation level matches the query requirements.\n"
        "5. EFFICIENCY: Do not include tables that do not contribute to the specific query metrics or filters.\n"
        "6. STRICT OUTPUT: Return ONLY a JSON list of strings representing the exact table names (e.g., [\"dbo_FactSales\", \"dbo_DimStore\"]). No conversational text, thought processes, or markdown blocks (e.g., NO ```json wrappers)."
    )

    selector_prompt = f"Query: {user_query}\nTables: {manifest_data}"
    
    selected_raw = call_planning_llm(system_instruction=selector_instruction, user_message=selector_prompt)
    
    table_menu = manifest_data.get('tables', {})
    
    # Parse table names from AI response
    try:
        clean_json = selected_raw.strip().replace("```json", "").replace("```", "")
        selected_table_names = json.loads(clean_json)
    except Exception:
        # Fallback: simple string match if AI doesn't return clean JSON
        selected_table_names = [name for name in table_menu.keys() if name.lower() in selected_raw.lower()]

    logger.info(f"\nSelected tables for planning: {selected_table_names}\n\n")

    # Build the targeted schema containing only the necessary tables
    targeted_schema = {name: info for name, info in table_menu.items() if name in selected_table_names}

    return manifest_data, targeted_schema

def _get_temporal_context() -> str:
    """
    Returns the standard temporal context shared across all prompt generations.
    """
    current_date = datetime.now().strftime('%B %d, %Y')
    return f"""
**IMPORTANT - TEMPORAL CONTEXT:**
- Today's Date: {current_date}
- Date table (DimDate) covers 2005-2011.
- Actual business activity (FactSales, FactStrategyPlan) only exists for 2007-2009.
- Years without facts (2005-2006, 2010-2011) should be treated as having zero sales/plan.
"""
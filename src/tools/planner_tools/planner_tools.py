import json
import os
from datetime import datetime
from src.utils.llm_utils import call_planning_llm, call_general_llm
from src.services.firestore_service import get_table_summary

from logger_config import logger

# ==========================================
# Prompt Components
# ==========================================

BASE_RULES = """
**TASK:** You are an AI Strategic Data Planning Agent. Your objective is to transform the user's business query into a structured, highly logical analytical plan.

**CRITICAL CONTEXT:** Your output is designed for TWO audiences who will read the ENTIRE plan: 
1. The Business User (who reads the steps to validate the analytical logic and business alignment). 
2. A Downstream SQL Agent (which executes each step sequentially as an independent database query).
Therefore, the plan must remain readable and professional for humans, while being strictly structured for the SQL Agent.

**STEP LINKAGE & DYNAMIC VARIABLES (CRITICAL):**
- **Dependency Handling:** Steps are executed sequentially. If Step 2 depends on entities found in Step 1, you MUST use a placeholder.
- **Placeholder Format:** Use **[Found from Step X: Attribute Name]**. 
- **IDENTIFIERS ONLY (CRITICAL RED LINE):** Placeholders MUST ONLY be used for Entity Identifiers/Keys/Names (e.g., `ProductKey`, `StoreName`, `CalendarMonthLabel`) to pass filtering criteria to the next step. You are STRICTLY FORBIDDEN from passing metric values, amounts, or entire datasets between steps. Every step MUST query the database directly. If a step requires a complex calculation (like MoM or YoY) based on an aggregated baseline, the downstream SQL Agent will handle the subqueries/CTEs natively in a SINGLE step.
- **COLLECTION RULE (CRITICAL):** If a step returns a list (e.g., "top 5 products"), use a SINGLE placeholder (e.g., `[Found from Step 2: ProductKey]`). Do NOT invent variables like `ProductKey1`, `ProductKey2`, etc.
- **UNIFIED TARGETING (GLOBAL RULE):** Whenever you need to identify specific cohorts (e.g., "Top 5 products", "Worst performing stores"), you MUST identify the targets AND retrieve their relevant baseline metrics in a SINGLE, unified step. You are STRICTLY FORBIDDEN from splitting "finding the targets" and "getting their data" into two separate steps.

- **NO NATURAL LANGUAGE REFERENCES (STRICT REPLACEMENT):**
  When referring to ANY data, entities, or values identified in prior steps, you are STRICTLY FORBIDDEN from using ANY pronouns, descriptive nouns, or directional phrases (e.g., "it", "their", "these products", "the selected categories", "from the previous step", "using the results above").
  **MANDATORY:** You MUST replace every single dependency or reference EXCLUSIVELY with the bracketed placeholder format. If a value is not inside a bracketed placeholder, the downstream SQL Agent will fail to see it.
  ❌ WRONG: 
     2. Calculate the profit margin for these categories using the sales from the previous step.
  ❌ WRONG:
     2. Based on the targets identified above, calculate the total cost.
  ✅ RIGHT: 
     2. Calculate the profit margin for the [Found from Step 1: ProductCategoryKey].
     
**TONE & FORMAT RULES:**
1. **Focus on Business Value:** Use professional business language for an executive.
2. **Be Grounded:** Use the provided database context.
3. **No SQL Terms:** Strictly PROHIBITED from mentioning "JOIN", "WHERE", "CTE", etc.
4. **USER-FACING LANGUAGE:** ALL exact table names and raw column names MUST be relegated to the 'Tables Used' and 'Key Metrics' sections ONLY. The ONLY exception is inside the bracketed placeholders [Found from Step X: AttributeName], where you MUST use the exact technical column name.

**OUTPUT STRUCTURE:**
The final plan MUST ONLY have FOUR main sections in the exact order below:
1. **Analysis Objective**
   - Summarize the potential business value and insights from the business problem in exactly 2-3 sentences.

2. **Action Plan**
   - Start directly with analytical verbs. Do not use conversational filler. 
     ❌ WRONG: "1. First, we need to calculate..." 
     ✅ RIGHT: "1. Calculate..."

3. **Tables Used**
   - Provide a bulleted list of the database tables utilized to formulate this plan.
   - **CRITICAL:** You must include the schema prefix for every table (e.g., `dbo.TableName`, `sales.TableName`).
   
4. **Key Metrics**
   - Provide a bulleted list of key metrics to track and monitor during the implementation phase.
   - Include a brief description or specify the relations/formulas between these metrics.
"""

DEEPDOWN_PROMPT = """
**DEEP DIVE RULES**:
To provide high-impact insights, you must follow the "Target -> Deep Dive -> Trend" flow. This allows the system to identify a specific "suspect" (ID/Category) and then investigate it:

1. **LEVEL 1: CORE TARGETING (Target):** Establish the primary baseline or specific subject of inquiry.
   - **Scenario A (Metric-focused):** If the query asks for a value (e.g., "Total Sales", "Growth"), calculate the aggregate total for the specified scope.
   - **Scenario B (Entity-focused):** If the query asks for a "Who/Which" (e.g., "Top 5 products", "Best store"), you MUST identify the targets AND retrieve their Unique Identifiers in a SINGLE unified step. DO NOT split identification and ID retrieval into two steps.
     ❌ WRONG: 
        1. Identify the top 5 best-selling products by calculating their total sales amount. 
        2. Retrieve the [Found from Step 1: ProductKey].
     ✅ RIGHT: 
        1. Identify the top 5 best-selling products by calculating their total sales amount.

2. **LEVEL 2: ATTRIBUTION & DRILL-DOWN (Deep Dive):** Analyze the "Why" or "Where".
   - **If Level 1 was Scenario A (Metric-focused):** Break down the total by primary business dimensions to identify failure/success origins. 
        *FLEXIBILITY RULE:* If the user implies a root-cause search (e.g., "why", "driver", "root cause"), YOU MUST prioritize identifying specific outliers (e.g., "Identify the top 3 categories with the most significant impact"). Otherwise, a general breakdown ("Calculate for each category") is acceptable.
   - **If Level 1 was Scenario B (Entity-focused):** Investigate the specific behaviors of the [Found from Step 1: Identifier].

3. **LEVEL 3: TREND:** Analyze the "When" for the baseline or the identified entity over time.
"""

CALC_PROMPT = """
**CALCULATION RULES**:
Focus on formulating accurate mathematical or temporal calculations (e.g., Month-over-Month, Year-over-Year, Profit Margins). 
- **UNIFIED CALCULATION (CRITICAL):** Do NOT split the retrieval of base values and the application of the mathematical formula into two steps. You MUST generate a SINGLE step that describes the final mathematical goal (e.g., "Calculate the Month-over-Month sales growth percentage..."). The downstream SQL Agent will use internal database functions (e.g., LAG, CTEs, aggregation) to handle the entire calculation within one query.
- **PREREQUISITE RULE:** If the calculation targets a specific pre-filtered subset of entities (e.g., "Top 5 products"), you must still identify those targets in a preceding step before calculating their metrics.
"""


COMPARE_PROMPT = """
**COMPARISON RULES**:
Your objective is to extract the correct baseline data so the downstream system can compare them later. 

- **PREREQUISITE RULE (TARGETING FIRST):** If the comparison involves specific cohorts (e.g., Top 5, Bottom 5, or filtered segments), you MUST generate the initial steps to identify those exact targets and retrieve their relevant metrics.
- **AGGREGATION STRATEGY (CRITICAL):** 
  - **Scenario A (Same Dimension):** If comparing entities within the SAME dimension (e.g., Online vs. Offline channels, Male vs. Female), you MUST use a SINGLE step to calculate the metric grouped by that dimension.
  - **Scenario B (Distinct Cohorts):** If comparing highly complex or distinct cohorts (e.g., Top 3 products vs. Bottom 3 products), you MUST identify and calculate them in SEPARATE steps.
- **SQL-ONLY RULE (ABSOLUTE FATAL ERROR IF VIOLATED):** Every single step you generate MUST translate directly into an actionable database query. Do NOT add a final analytical step that says "Compare the results" or "Present a comparative view". 
"""

PREDICT_PROMPT = """
**PREDICTION RULES**:
Your objective is to extract the correct historical time-series data so the downstream system can use it to forecast future trends. 

- **DATA RETRIEVAL ONLY:** Formulate steps to extract historical baselines (e.g., sales over the last 12 months). 
- **SQL-ONLY RULE (ABSOLUTE FATAL ERROR IF VIOLATED):** You MUST stop after retrieving the historical data. Do NOT generate a final step that says "Forecast future trends" or "Predict the next month". The downstream Insight Agent will perform the actual prediction using the data you retrieve.
"""
# ==========================================
# Dynamic Router
# ==========================================

def generate_complex_business_plan(user_query: str):
    """
    Strategic planner utilizing dynamic prompt assembly based on query intent.
    """
    manifest_data, targeted_schema = _get_table_context(user_query) 
    if not manifest_data or not targeted_schema:
        return {"result": "Error: Failed to retrieve necessary database context."}
    
    temporal_context = _get_temporal_context()

    # ---------------------------------------------------------
    # STEP 1: Intent Recognition
    # ---------------------------------------------------------

    intent_instruction = """
    Classify the core analytical intents of the user query.
    Select ALL that apply from: [DEEPDOWN, CALC, COMPARE, PREDICT].
    Return ONLY a JSON list of strings, e.g., ["DEEPDOWN", "CALC"].
    - If the query asks "why", "what is the root cause", or asks to analyze the internal composition/drivers of a specific metric, include DEEPDOWN.
    - If it asks for mathematical formulas, ratios, percentages, or growth (e.g., MoM, YoY), include CALC.
    - If it asks to contrast entities, find gaps, or uses "vs", "compare", "top vs bottom", include COMPARE.
    - If it asks for future, forecast, or "what if", include PREDICT.
    """
    
    intent_response = call_planning_llm(system_instruction=intent_instruction, user_message=user_query)
    logger.info(f"Intent Classification Response: {intent_response}")

    try:
        clean_json = intent_response.strip().replace("```json", "").replace("```", "")
        intents = json.loads(clean_json)
        if not isinstance(intents, list): intents = ["DEEPDOWN"]
    except Exception as e:
        logger.warning(f"Intent parsing failed, defaulting to DEEPDOWN: {e}")
        intents = ["DEEPDOWN"]

    logger.info(f"Identified Intents: {intents}")

    # ---------------------------------------------------------
    # Dynamic Playbook Assembly
    # ---------------------------------------------------------
    active_prompt = ""
    if "DEEPDOWN" in intents: active_prompt += DEEPDOWN_PROMPT + "\n"
    if "CALC" in intents:     active_prompt += CALC_PROMPT + "\n"
    if "COMPARE" in intents:  active_prompt += COMPARE_PROMPT + "\n"
    if "PREDICT" in intents:  active_prompt += PREDICT_PROMPT + "\n"


    if not active_prompt: active_prompt = DEEPDOWN_PROMPT + "\n"

    # ---------------------------------------------------------
    # STEP 3: Final Prompt Generation
    # ---------------------------------------------------------
    system_prompt = f"""
    {temporal_context}
    
    **STRATEGIC SUMMARY:**
    {manifest_data.get('strategic_summary')}
    
    **AVAILABLE TABLES & COLUMNS (Targeted Context):**
    {json.dumps(targeted_schema, separators=(',', ':'))}
    
    {BASE_RULES}
    
    {active_prompt}

    **CRITICAL EXECUTION ORDER (CONFLICT RESOLUTION):**
    If multiple playbooks are active, sequence your steps based on the **Dependency Chain** of the user's query.

    1. **DECLARATIVE COMPRESSION (CORE RULE):** You MUST think like a declarative SQL engine. You are STRICTLY FORBIDDEN from generating intermediate steps to "prepare" data. Any complex calculation (MoM, YoY, Variance, Ratios) combined with its necessary filtering/targeting MUST be compressed into a SINGLE step.
       - *Example:* "Identify the worst month based on MoM growth" = 1 SINGLE STEP.

    2. **LOGICAL SEQUENCING & PLAYBOOK HIERARCHY (FLOW CONTROL):**
       - **Map the Dependency Chain (COMPARE vs DEEPDOWN):** 
         * *Macro-to-Micro (Compare -> Deep Dive):* If comparing entities to find a winner/loser to investigate (e.g., "Compare regions, find the worst, break down its categories"), Step 1 MUST run the comparison AND retrieve the target ID. Step 2 uses that ID for the breakdown.
         * *Micro-to-Macro (Target -> Compare):* If identifying specific cohorts to compare their metrics (e.g., "Identify the top 3 products and compare their YoY growth"), Step 1 MUST identify the target IDs. Step 2 uses those IDs to execute the comparative calculation.
       - **Identify First, Analyze Second:** If the query requires acting upon a specific subset, you MUST establish that target in the first step before performing further deep-dives or secondary calculations on it.
       - **Placeholder Usage:** Always use the `[Found from Step X: AttributeName]` format to link these dependent steps.

    3. **NO NON-SQL STEPS:** Stop generating steps once all required database retrieval tasks are mapped out. The downstream Orchestrator and Insight Agent will handle the actual business logic natively.
    """

    logger.info(f"\nDynamically Assembled System Prompt:\n{system_prompt}\n\n")

    complex_plan = call_planning_llm(system_instruction=system_prompt, user_message=user_query, temperature=0.0) 

    if not complex_plan:
        return {"result": "Error: The Planner tool failed to generate a substantial strategy."}
    
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

**STRATEGIC SUMMARY:**
{manifest_data.get('strategic_summary')}

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

    direct_plan = call_planning_llm(system_instruction=system_prompt, user_message=user_question, temperature=0.)

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
import json
import os
import config 
import re
import pyodbc
import pandas as pd

from logger_config import logger

CONNECTION_STRING = config.CONNECTION_STRING

def execute_sql(sql_blocks: list) -> str:
    """
    Parses a multi-step Markdown string from the SqlAgent, 
    extracts each SQL block, and executes them sequentially.
    """

    logger.debug(f"\nReceived raw SQL from Agent:\n{sql_blocks}\n")

    if not sql_blocks:
        return json.dumps({"error": "No valid SQL blocks found in the payload."})

    # This dictionary will store the final consolidated report.
    final_report = {}

    logger.debug(f"Executing {len(sql_blocks)} SQL blocks.")

    
    # 3. Execution Loop: Iterate through each extracted SQL block.
    for i, raw_query in enumerate(sql_blocks):
        
        # Clean the query (remove any leading/trailing garbage chars).
        logger.debug(f"\nOriginal SQL Block {i+1}:\n{raw_query}\n")
        clean_query = raw_query.strip()
        logger.debug(f"\nCleaned SQL Block {i+1}:\n{clean_query}\n")

        step_key = f"Step_{i+1}"
        
        try:
            with pyodbc.connect(config.CONNECTION_STRING) as conn:
                df = pd.read_sql(clean_query, conn)
                
                final_report[step_key] = {
                    "status": "success",
                    "row_count": len(df),
                    "data": df.head(100).to_dict(orient='records')
                }
        except Exception as e:
            logger.error(f"Error at {step_key}: {str(e)}")
            final_report[step_key] = {
                "status": "error",
                "message": str(e)
            }
        
    logger.debug(f"\nFinal Report: {final_report}\n")

    # 4. Serialization: Return the final structured JSON string.
    return json.dumps(final_report, indent=2, default=str)
import os
import sys
import logging
from google.cloud import firestore
import config
import json
from typing import Any

from logger_config import logger

_cached_firestore_client = None
_cached_knowledge_base = None

def get_firestore_client():
    """
    Ensures the client is initialized only once during the application lifecycle.
    """
    global _cached_firestore_client
    if _cached_firestore_client is None:
        try:
            _cached_firestore_client = firestore.Client()
            logger.info( "\n✅ Firestore client successfully initialized.\n")
        except Exception as e:
            logger.error(f"❌ Critical Failure: Firestore Initialization failed: {e}")
            return None
    return _cached_firestore_client

def get_knowledge_base_data():
    """ Retrieves all database data and stores it in cache to avoid repeated database calls."""
    global _cached_knowledge_base

    if _cached_knowledge_base is not None:
        return _cached_knowledge_base
    
    db = get_firestore_client()
    if db is None:
        logger.error("🚨 ERROR: DATABASE CONNECTION FAILED")
        return {"__STATUS__": "ERROR_CONNECTION_FAILED"}

    logger.info("\n📡 Fetching full knowledge base from Firestore...\n")
    
    try:
        # Create a reference to the specific document in Firestore
        doc_ref = db.collection(config.KNOWLEDGE_BASE_COLLECTION).document(config.KNOWLEDGE_BASE_DOCUMENT)
        # Execute the network request to fetch the document
        doc = doc_ref.get()
        # Convert the Firestore snapshot into a Python dictionary
        data = doc.to_dict()    

        if data:
            _cached_knowledge_base = data
            

            # Extract classified_tables
            tables = data.get('classified_tables', {})
            # Extract _relationships table
            relationships = data.get('_relationships', []) 

            log_message = [
                "\n" + "="*50,
                f"📦 KNOWLEDGE BASE LOADED",
                f"   - Total Tables: {len(tables)}",
                f"   - Total Relationships: {len(relationships)}",
                "="*50 + "\n"
            ]

            logger.info("\n".join(log_message))

            return _cached_knowledge_base
        else:
            logger.warning("⚠️ Warning: Knowledge base document is empty.")
            return {"__STATUS__": "ERROR_EMPTY_DOCUMENT"}
        
    except Exception as e:
        logger.error(f"❌ Error fetching knowledge base: {e}")
        return {"__STATUS__": "ERROR_FETCH_EXCEPTION"}

def get_table_summary():
    """ 
    Fetches table names, purpose summaries, and schema-light details (columns + types).
    Strictly NO sample rows are included to keep the context window light.
    """

    full_data = get_knowledge_base_data()
    
    if full_data:

        table_summary = {}
        tables = full_data.get('classified_tables', {})
        for name, details in tables.items():

            raw_columns_dict = details.get('raw_columns', {})
            formatted_columns = []

            if isinstance(raw_columns_dict, dict):
                for col_name, col_type in sorted(raw_columns_dict.items()):
                    formatted_columns.append(f"{col_name} ({col_type})")

            table_summary[name] = {
                "classification": details.get('classification', 'N/A'),
                "purpose": details.get('purpose_summary', 'No summary'),
                # "entity_grain": details.get('EntityGrain', 'N/A'),
                # "time_grain": details.get('TimeGrain', 'N/A'),
                "columns": formatted_columns if formatted_columns else ["No column data available"]
            }

        all_relationships = full_data.get('_relationships', [])
        relationship_map = []
        for rel in all_relationships:
            p = rel.get('parent_table', '')
            r = rel.get('referenced_table', '')
            relationship_map.append(f"{p} -> {r}")
            
        result = {
            "strategic_summary": full_data.get('strategic_summary'),
            "tables": table_summary,
            "relationships": relationship_map
        }

        formatted_manifest = json.dumps(result, indent=4, ensure_ascii=False)

        logger.info(
            f"\n================  TABLE SUMMARY START  ================\n"
            # f"{formatted_manifest}\n"
            f"================  TABLE SUMMARY END  ================\n\n"
        )
        
        return result
    
    return {"__STATUS__": "ERROR_UNKNOWN_EMPTY_DATA"}

def get_specific_table_details(table_names: list[str]) -> dict[str, Any]:
    # def fetch_raw_schema_data(table_names: list[str]) -> dict[str, Any]:
    """
    Core function to retrieve raw schema and relationship data from Firestore.
    This function does NOT contain any AI or Caching logic.
    """
    
    # 1.INPUT SANITIZATION
    if isinstance(table_names, str):
        raw = table_names.strip()
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                table_names = [str(x) for x in parsed]
            else:
                table_names = [str(parsed)]
        except Exception:
            table_names = [t.strip() for t in raw.split(",") if t.strip()]

    full_data = get_knowledge_base_data() 
    all_tables = full_data.get('classified_tables', {})
    all_relationships = full_data.get('_relationships', [])
    
    result = {
        "tables": {},
        "relevant_relationships": []
    }

    # 2. TABLE DATA EXTRACTION
    # Use '.' in logic, but Firestore keys use '_'. We must bridge this gap here.
    for name in table_names:

        storage_key = name.replace('.', '_')

        if storage_key in all_tables:
            # Insert the schmema details for the table into the tables dictionary under the result variable
            result["tables"][name] = all_tables[storage_key]
            logger.info(f"✅ Found metadata for {name}")
        else:
            logger.warning(f"⚠️ Warning: Table {name} not found in knowledge base.")
    

    # 3. RELATIONSHIP FILTERING
    # We normalize both targets and relationship data to lowercase dot-format for safe comparison.
    target_names_lower = [table.lower() for table in table_names]
    target_names_no_schema = [name.split('.')[-1] for name in target_names_lower]

    logger.debug(f"\nTarget table names for relationship matching: {target_names_no_schema}\n")

    for rel in all_relationships:
        # If the tables names no dbo need to chanege the logic here
        parent = rel.get('parent_table', '').lower()
        referenced = rel.get('referenced_table', '').lower()
        
        if parent in target_names_no_schema or referenced in target_names_no_schema:
            result["relevant_relationships"].append(rel)

    logger.info(f"🔗 Found {len(result['relevant_relationships'])} relevant relationships.\n")
    
    logger.info("\n------------------- 📝 DETAILED SCHEMA CONTENT -------------------------")
    logger.info(json.dumps(result, indent=4, ensure_ascii=False)) 
    logger.info("------------------------------------------\n")
    
    return result
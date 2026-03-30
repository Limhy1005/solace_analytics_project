from google.cloud import firestore

def sanitize_keys(data):
    """
    Recursively replaces '.' with '_' in dictionary keys.
    This fixes the 'type unset' error in the Firestore UI.
    """
    if not isinstance(data, dict):
        return data
    
    cleaned_data = {}
    for key, value in data.items():
        # FIX 1: Actually replace the period with an underscore
        new_key = key.replace('.', '_') if isinstance(key, str) else key
        cleaned_data[new_key] = sanitize_keys(value)
    return cleaned_data

def update_and_fix_firestore(project_id, collection_name, doc_id, table_key, field_name, new_value):
    db = firestore.Client(project=project_id)
    doc_ref = db.collection(collection_name).document(doc_id)
    
    doc = doc_ref.get()
    if not doc.exists:
        print(f"Error: Document {doc_id} not found.")
        return

    # 1. Get current data and fix all keys (converting '.' to '_')
    data = doc.to_dict()
    cleaned_data = sanitize_keys(data)
    
    # Force the target table key to use underscores just to be safe
    sanitized_table_key = table_key.replace('.', '_')
    
    # FIX 2: Look inside the 'classified_tables' dictionary
    if 'classified_tables' in cleaned_data and sanitized_table_key in cleaned_data['classified_tables']:
        print(f"Updating {field_name} in {sanitized_table_key}...")
        cleaned_data['classified_tables'][sanitized_table_key][field_name] = new_value
    else:
        print(f"Warning: {sanitized_table_key} not found in document. Creating it.")
        # Optional safeguard: create the structure if it somehow got deleted
        if 'classified_tables' not in cleaned_data:
            cleaned_data['classified_tables'] = {}
        cleaned_data['classified_tables'][sanitized_table_key] = {field_name: new_value}

    # 3. Overwrite the document with the fixed keys and new value
    doc_ref.set(cleaned_data)
    print("Success! Your Firestore UI should now work for this document.")

# --- CONFIGURATION ---
PROJECT_ID = 'solace-agent-mesh-ai'
COLLECTION_NAME = 'crdw_ai_knowledge_base'
DOCUMENT_ID = 'crdw_schema'

TABLE_NAME = 'dbo_FactSales' 
TARGET_FIELD = 'purpose_summary'

NEW_VALUE = "Master omni-channel sales table containing transactions for all 4 channels (Store, Online, Catalog, Reseller). CRITICAL NOTE: The 'Online' channel here refers ONLY to Store-Fulfilled orders. Use this table for physical retail performance. WARNING: This table DOES NOT contain pure e-commerce sales. To calculate TOTAL COMPANY SALES, you MUST select this table AND FactOnlineSales together."

# --- EXECUTE ---
update_and_fix_firestore(
    PROJECT_ID, 
    COLLECTION_NAME, 
    DOCUMENT_ID, 
    TABLE_NAME, 
    TARGET_FIELD, 
    NEW_VALUE
)
import os
import re
import json                
import pandas as pd
from google import genai
from vanna.chromadb import ChromaDB_VectorStore
from vanna.google import GoogleGeminiChat
from chromadb.api.types import Documents, Embeddings, EmbeddingFunction
from logger_config import logger

class GeminiEmbeddingFunction(EmbeddingFunction):
    """
    Custom embedding function to ensure ChromaDB uses Gemini 
    instead of its default 384-dimension model.
    """
    def __init__(self, api_key: str, model_name: str = "gemini-embedding-001"):
        self.client = genai.Client(api_key=api_key) if api_key else genai.Client()
        self.model_name = model_name

    def __call__(self, input: Documents) -> Embeddings:
        embeddings = []
        for text in input:
            result = self.client.models.embed_content(
                model=self.model_name,
                contents=text
            )
            embeddings.append(result.embeddings[0].values)
        return embeddings
    
# 1. Class Definition
class MyVanna(ChromaDB_VectorStore, GoogleGeminiChat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        GoogleGeminiChat.__init__(self, config=config)
        
        api_key = config.get("api_key")
        self.model_name = config.get("model", "gemini-2.5-flash") 
        self.client = genai.Client(api_key=api_key) if api_key else genai.Client()

    # Override Vanna's default submit_prompt to use the new google.genai SDK for generating SQL.
    def submit_prompt(self, prompt, **kwargs):
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=str(prompt)
        )
        return response.text
    
# 2. Configuration
api_key = os.environ.get("LLM_SERVICE_API_KEY")
config = {
    "api_key": api_key,
    "model": "gemini-2.5-flash",
    "embedding_function": GeminiEmbeddingFunction(api_key=api_key),
    "path": "./vanna_chroma_db"
}
vn = MyVanna(config=config)

# ==========================================
# 3. Read Data Sources
# ==========================================
sql_file_path = r"C:\Users\User\Downloads\script.sql"
json_file_path = "crdw_schema_metadata.json"

print(f"Reading DDL file from: {sql_file_path}")
with open(sql_file_path, "r", encoding="utf-16") as file: 
    full_ddl = file.read()

print(f"Reading rich sample data from: {json_file_path}")
with open(json_file_path, "r", encoding="utf-8") as f:
    schema_json_data = json.load(f)

# ==========================================
# 4. Parse SQL Skeleton (Extract Relations and Structures)
# ==========================================
print("Step 1: Parsing DDL to extract structures and relationships...")

# --- LOGIC: EXTRACT RELATIONSHIPS (ALTER TABLE) ---
relationship_match = re.search(r'ALTER TABLE.*', full_ddl, re.DOTALL)
# group(0): Extract the entire matched string that starts from the first 'ALTER TABLE' to the end of the file.
all_relationship_text = relationship_match.group(0) if relationship_match else ""

grouped_relations_map = {}
if all_relationship_text:
    individual_sql_list = re.split(r'(?=ALTER TABLE)', all_relationship_text)
    print(f"individual_sql_list: {individual_sql_list}")

    for stmt in individual_sql_list:
        clean_stmt = stmt.strip()
        print(f"stmt: {stmt}")
        if not clean_stmt: continue
            
        match = re.search(r'ALTER TABLE \[dbo\]\.\[(\w+)\]', clean_stmt)
        print(f"match: {match}")
        if match:
            table_name = match.group(1)
            print(f"Found table: {table_name}")
            if table_name not in grouped_relations_map:
                grouped_relations_map[table_name] = ""
            grouped_relations_map[table_name] += "\n" + clean_stmt
            print("grouped_relations_map:", grouped_relations_map)

# --- LOGIC: EXTRACT TABLE STRUCTURES (CREATE TABLE) ---
tables_only_section = full_ddl.replace(all_relationship_text, "")
table_blocks = re.split(r'(?=/\*\*\*\*\*\* Object:  Table)', tables_only_section)

table_structures_dict = {}
for block in table_blocks:
    clean_block = block.strip()
    if "CREATE TABLE" in clean_block:
        name_match = re.search(r'CREATE TABLE \[dbo\]\.\[(\w+)\]', clean_block)
        if name_match:
            table_name = name_match.group(1)
            table_structures_dict[table_name] = clean_block
            print(f"table_structures_dict_create_table: {table_structures_dict}")

# ==========================================
# 5. Merge and Train (Structure + Relation + CSV Sample Data)
# ==========================================
print("\nStep 2: Merging Structures, Relations, and CSV Sample Data...")

for table_name, create_stmt in table_structures_dict.items():
    # Start with the CREATE TABLE statement
    combined_ddl = create_stmt
    
    # 1. Attach Foreign Keys
    if table_name in grouped_relations_map:
        combined_ddl += grouped_relations_map[table_name]
        print(f" -> Indexing [Structure + Relations] for Table: {table_name}")
    else:
        print(f" -> Indexing [Structure Only] for Table: {table_name}")
    
    # 2. Attach Sample Data in pure CSV format
    json_key = f"dbo.{table_name}"
    if json_key in schema_json_data:
        sample_rows = schema_json_data[json_key].get("sample_rows", [])
        
        if isinstance(sample_rows, list) and len(sample_rows) > 0:
            # Convert JSON array to Pandas DataFrame
            df = pd.DataFrame(sample_rows)
            
            # Convert exactly to: Fieldname\nRow1\nRow2
            sample_data_text = df.to_csv(index=False, lineterminator='\n', na_rep='NULL')
            
            # Wrap it in SQL comments
            combined_ddl += f"\n\n/* Sample Data:\n{sample_data_text}*/\n"
            print(f"    + Successfully attached CSV Sample Data.")
        else:
            print(f"    - No valid Sample Data found.")

    # 3. Train ChromaDB
    vn.train(ddl=combined_ddl)

print("\n✅ ALL DONE! The database is chunked by entity and ready for accurate RAG.")
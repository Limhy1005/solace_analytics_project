import os
import config
from google import genai
from vanna.chromadb import ChromaDB_VectorStore
from vanna.google import GoogleGeminiChat
from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

import pyodbc
import pandas as pd
CONNECTION_STRING = config.CONNECTION_STRING


class GeminiEmbeddingFunction(EmbeddingFunction):
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
    
# 1. Define the custom Vanna class with the NEW SDK and Token Usage tracking
class MyVanna(ChromaDB_VectorStore, GoogleGeminiChat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        GoogleGeminiChat.__init__(self, config=config)
        
        # Initialize Gemini API using the new SDK approach
        api_key = config.get("api_key")
        self.model_name = config.get("model", "gemini-2.5-flash")
        self.client = genai.Client(api_key=api_key) if api_key else genai.Client()
    
    # Override submit_prompt to intercept and display token consumption
    def submit_prompt(self, prompt, **kwargs):
        # Request completion from the Gemini API using the new SDK syntax
        global_rules = """
        [CRITICAL RULES]:
        1. DATEFROMPARTS() month MUST be 1-12. If [CalendarMonth] is YYYYMM (e.g., 200701), you MUST use (CalendarMonth % 100) to extract the month.
        --------------------------------------------------
        """
        
        final_prompt = global_rules + "\n" + str(prompt)

        response = self.client.models.generate_content(
            model=self.model_name,
            contents=final_prompt
        )
        
        # Display Token Usage Metadata
        if hasattr(response, 'usage_metadata') and response.usage_metadata:
            usage = response.usage_metadata
            print(f"\n📊 --- Token Usage Report ---")
            print(f"Input Tokens: {usage.prompt_token_count}")
            print(f"Output Tokens: {usage.candidates_token_count}")
            print(f"Total Tokens: {usage.total_token_count}\n")
            print(f"------------------------------\n")
        
        return response.text
    
    def run_sql(self, sql: str, **kwargs) -> pd.DataFrame:
        """
        Custom function to connect to SQL Server and execute the generated query.
        Takes a SQL string as input and returns a Pandas DataFrame.
        """
        # 1. Establish the connection using your existing connection string
        conn = pyodbc.connect(CONNECTION_STRING)
        
        try:
            # 2. Use Pandas to run the SQL and automatically convert it to a DataFrame
            # pd.read_sql is highly optimized for this exact task
            df = pd.read_sql(sql, conn)
            return df
            
        except Exception as e:
            # 3. Catch and raise any database-level errors (e.g., syntax errors)
            print(f"❌ Database execution error: {str(e)}")
            raise e
            
        finally:
            # 4. CRITICAL: Always close the connection to prevent connection leaks!
            conn.close()

# 2. Configuration
api_key = os.environ.get("LLM_SERVICE_API_KEY")
config = {
    "api_key": api_key,
    "model": "gemini-2.5-flash",

    "path": "./vanna_chroma_db",
    "embedding_function": GeminiEmbeddingFunction(api_key=api_key)
}

# 3. Initialize Vanna
print("Initializing Vanna with existing ChromaDB memory...")
vn = MyVanna(config=config)

if __name__ == "__main__":
    print("Initializing Vanna with existing ChromaDB memory...")
    
    question = """
    Identify the top 5 performing stores and the bottom 5 performing stores based on their total sales amount for the year 2007.
    """

    print(f"\n🤔 User Question: {question.strip()}")
    print("\n🧠 AI is retrieving schema context from local ChromaDB and generating SQL...")

    try:
        sql = vn.generate_sql(question)
        print("-" * 50)
        print(f"✨ SUCCESS! Generated SQL:\n")
        print(sql)
        print("-" * 50)
    except Exception as e:
        print(f"❌ Failed to generate SQL: {e}")
        
# 6. Optional: Execution
# df = vn.run_sql(sql)
# print("\n📊 Data Preview:")   
# print(df.head())
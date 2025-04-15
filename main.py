import os
import json
import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from bigquery_client import execute_query

load_dotenv()

GCP_CREDENTIALS = "light-moon-455005-g5-e7cc7653b70f.json"
GCP_PROJECT = "light-moon-455005-g5"
BQ_DATASET = "shopping"
GCP_REGION = "us-central1"
GEN_AI_MODEL = "gemini-2.0-flash"


scoped_credentials = service_account.Credentials.from_service_account_file(
    GCP_CREDENTIALS,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
scoped_credentials.refresh(Request())
token = scoped_credentials.token


endpoint = (
    f"https://{GCP_REGION}-aiplatform.googleapis.com/v1/projects/"
    f"{GCP_PROJECT}/locations/{GCP_REGION}/publishers/google/models/{GEN_AI_MODEL}:generateContent"
)


headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}


schema = {
  "table": "shopping",
  "columns": [
    {"name": "Customer ID", "type": "INTEGER", "description": "Unique ID for customer"},
    {"name": "Age", "type": "INTEGER", "description": "Customer's age"},
    {"name": "Gender", "type": "STRING", "description": "Gender of the customer"},
    {"name": "Item Purchased", "type": "STRING", "description": "Name of the product purchased"},
    {"name": "Category", "type": "STRING", "description": "Category of the item"},
    {"name": "Purchase Amount ", "type": "INTEGER", "description": "Amount spent on the purchase"},
    {"name": "Location", "type": "STRING", "description": "Location of the purchase"},
    {"name": "Size", "type": "STRING", "description": "Size of the purchased item"},
    {"name": "Color", "type": "STRING", "description": "Color of the purchased item"},
    {"name": "Season", "type": "STRING", "description": "Season during which the item was purchased"},
    {"name": "Review Rating", "type": "FLOAT", "description": "Customer rating for the product"},
    {"name": "Subscription Status", "type": "BOOLEAN", "description": "Whether the customer is subscribed or not. TRUE/FALSE"},
    {"name": "Payment Method", "type": "STRING", "description": "Method used for payment"},
    {"name": "Shipping Type", "type": "STRING", "description": "Type of delivery used"},
    {"name": "Discount Applied", "type": "STRING", "description": "Whether a discount was applied to the purchase"},
    {"name": "Promo Code Used", "type": "STRING", "description": "Whether a promo code was used"},
    {"name": "Previous Purchases", "type": "INTEGER", "description": "Total previous purchases made by the customer"},
    {"name": "Preferred Payment Method", "type": "STRING", "description": "Customer's preferred method of payment"},
    {"name": "Frequency of Purchases", "type": "STRING", "description": "How often the customer makes a purchase"}
  ]
}


dialogue = []

MAX_HISTORY = 6
def manage_context(role, msg):
    global dialogue
    dialogue.append({"role": role, "content": msg})
    if len(dialogue) > MAX_HISTORY:
        dialogue = dialogue[-MAX_HISTORY:]
    return dialogue



def ask_sql_engine(user_input, context,execute=True):
    meta_prompt = f"""
You are a SQL expert helping with BigQuery. Here's the schema and examples to guide your query generation.

Project: {GCP_PROJECT}
Dataset: {BQ_DATASET}
Table: shopping

Schema:
{json.dumps(schema, indent=2)}


Examples:
Note: The column 'Subscription Status' is of BOOLEAN type. Use TRUE/FALSE without quotes in SQL.
Q: What's the average amount spent by subscribed users?
A:
SELECT AVG(`Purchase Amount `) AS avg_spent
FROM `{GCP_PROJECT}.{BQ_DATASET}.shopping`
WHERE `Subscription Status` = TRUE;

Q: Show all winter clothing items rated above 4.
A:
SELECT * FROM `{GCP_PROJECT}.{BQ_DATASET}.shopping`
WHERE Category = 'Clothing' AND Season = 'Winter' AND `Review Rating` > 4;

Q: Locations with highest avg spend?
A:
SELECT Location, AVG(`Purchase Amount `) AS avg_amt
FROM `{GCP_PROJECT}.{BQ_DATASET}.shopping`
GROUP BY Location
ORDER BY avg_amt DESC;

Context:
{context}

User's Question:
"""

    payload = {
        "contents": [
            {"role": "model", "parts": [{"text": meta_prompt}]},
            {"role": "user", "parts": [{"text": user_input}]}
        ]
    }

    res = requests.post(endpoint, headers=headers, data=json.dumps(payload))
    if res.status_code == 200:
        sql_code = res.json()["candidates"][0]["content"]["parts"][0]["text"]
        cleaned = sql_code.strip().removeprefix("```sql").removesuffix("```").strip()
        if not execute:
            return cleaned
    #     if validate_sql(cleaned):
    #         bq_result = execute_query(cleaned)
    #         respond_with_summary(user_input, bq_result)
    #         return bq_result
    #     else:
    #         print("\nBlocked: Only SELECT queries are allowed!")
    #         return None
    # else:
    #     print(f"Generation Error ({res.status_code}): {res.text}")
    #     return None
    
def validate_sql(query: str) -> bool:
    """Returns True only for SELECT queries with no dangerous clauses"""
    query = query.strip().lower()
    
    
    if not (query.startswith("select") or query.startswith("with")):
        return False
    
    
    forbidden = ["insert", "update", "delete", "drop", "alter", 
                "truncate", "create", "replace", "merge", "grant"]
    return not any(keyword in query for keyword in forbidden)


def respond_with_summary(question, result):
    followup_prompt = f"""
You are a data assistant. Given a user question and the JSON result of a BigQuery query, create a clear and friendly English summary.

User Question:
{question}

Query Result:
{result}

Short response:
"""
    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": followup_prompt}]}
        ]
    }

    res = requests.post(endpoint, headers=headers, data=json.dumps(payload))
    if res.status_code == 200:
        reply = res.json()["candidates"][0]["content"]["parts"][0]["text"]
        print("\nAnswer Summary:\n", reply)
    else:
        print(f"Summary Error ({res.status_code}): {res.text}")


if __name__ == "__main__":
    user_input = ""
    print("Hi,I'm your SQL assistant! Ask me anything about the shopping dataset.")
    
    while user_input.lower() != "exit":
        user_input = input("\nAsk a question: ")
        if user_input.lower() == "exit":
            break
        
    
        memory = manage_context("user", user_input)
        sql_response = ask_sql_engine(user_input, memory, execute=False)  
        
        if sql_response: 
            if validate_sql(sql_response):
                print("\nGenerated SQL:\n", sql_response)
                result = execute_query(sql_response)
                print("\nQuery Output:\n", result)
                respond_with_summary(user_input, result)
                manage_context("assistant", f"SQL Result: {result}")
            else:
                print("\n Blocked: This query could modify data. Only SELECT queries are allowed.")
                manage_context("assistant", "I can only generate SELECT queries for data analysis.")
        else:
            manage_context("assistant", "I couldn't generate a valid SQL query for that question.")

    print("\nSession closed. Have a great day!")
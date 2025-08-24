from dotenv import load_dotenv
import os
import json
from io import BytesIO
from datetime import datetime, timedelta
import pandas as pd
# import matplotlib.pyplot as plt
import redis
from sentence_transformers import SentenceTransformer, util
from openai import AzureOpenAI
from Conn import execute_sql
from filter_tables import merge_unique_table_definitions
import schema
import re
from pathlib import Path
import json
from azure.storage.blob import BlobSasPermissions
from azure.storage.blob import BlobServiceClient,generate_blob_sas,BlobSasPermissions
from io import StringIO
now=datetime.now()
load_dotenv()

# Redis & LLM setup

redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


client = AzureOpenAI(
    azure_endpoint=os.environ["OPENAI_ENDPOINT"],
    api_key=os.environ["OPENAI_API_KEY"],
    api_version="2024-02-15-preview"
)
model1 = SentenceTransformer("all-MiniLM-L6-v2")


def savetoBlog(df,user_id,tenantId):
    try:
        # --- Read credentials from environment variables ---
        # account_name = os.getenv("BLOB_ACCOUNT_NAME")
        # account_key = os.getenv("BLOB_ACCOUNT_KEY")
        # container_name = os.getenv("BLOB_CONTAINER_NAME")
        account_name = os.environ["BLOB_ACCOUNT_NAME"]
        account_key = os.environ["BLOB_ACCOUNT_KEY"]
        container_name = os.environ["BLOB_CONTAINER_NAME"]
 
        # --- Create BlobServiceClient ---
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
        container_client = blob_service_client.get_container_client(container_name)

        # --- Convert DataFrame to CSV in memory ---
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
 
        # --- Upload CSV to blob ---
        blob_name = f"my_dataframe_{user_id}_{tenantId}.csv"
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
 
        # Generate SAS token valid for 1 hour
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )
 
        # Full blob URL with SAS token
        blob_url_with_sas = f"{account_url}/{container_name}/{blob_name}?{sas_token}"

        return blob_url_with_sas,True
 
    except Exception as e:
        print(f'{{"success": false, "error": "{str(e)}"}}')
        return "",False


# Follow-up logic
follow_up_keywords = [
    "what about", "how about", "and", "also", "but", "then", "next",
    "can you", "could you", "show me", "what if", "instead", "again",
    "now", "give me", "more", "ok", "so", "tell me", "same", "like before", "add new columns", "add","create custom column","custom column","new column"
]
reference_keywords = ["it", "this", "that", "those", "them", "he", "she", "they", "do it", "same thing"]

def is_follow_up_keyword_based(current_input, previous_input):
    if not current_input or not previous_input:
        return False
    input_lower = current_input.lower()
    for k in follow_up_keywords + reference_keywords:
        if input_lower.startswith(k) or f" {k} " in input_lower:
            return True
    return False

# Redis-based message storage

def store_user_message(user_id,tenantId, message, is_follow_up=False, previous_user_input=None, previous_sql_query=None,previous_schemas=None):
    msg = {
        "role": "user",
        "content": message,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "follow_up": is_follow_up,
        "previous_user_input": previous_user_input if is_follow_up else None,
        "previous_sql_query": previous_sql_query if is_follow_up else None,
        "previous_schemas": previous_schemas if is_follow_up else None,
    }

    key = f"chat:{user_id}_{tenantId}"
    redis_client.rpush(key, json.dumps(msg))
    redis_client.set(f"{key}:last_message", json.dumps(msg))

    if is_follow_up:
        if previous_sql_query:
            redis_client.set(f"{key}:last_sql_query", previous_sql_query)
        if previous_schemas:
            redis_client.set(f"{key}:previous_schemas", json.dumps(previous_schemas))

    MAX_HISTORY_LENGTH = 10
    redis_client.ltrim(key, -MAX_HISTORY_LENGTH, -1)
    print(msg)

def store_assistant_message(user_id,tenantId,message, previous_sql_query=None,previous_schemas=None):
    msg = {
        "role": "assistant",
        "content": message,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "previous_sql_query": previous_sql_query,
        "previous_schemas": previous_schemas 
    }

    key = f"chat:{user_id}_{tenantId}"
    redis_client.rpush(key, json.dumps(msg))

    #  Keep separate storage updated

    if previous_sql_query:
        redis_client.set(f"{key}:last_sql_query", previous_sql_query)
    if previous_schemas:
        redis_client.set(f"{key}:previous_schemas", json.dumps(previous_schemas))

    # # Also save to local JSON backup
    # LOCAL_CHAT_FILE = "app5.json"
    # try:
    #     history = []
    #     if Path(LOCAL_CHAT_FILE).exists():
    #         with open(LOCAL_CHAT_FILE, "r", encoding="utf-8") as f:
    #             history = json.load(f)
    #     history.append(msg)
    #     with open(LOCAL_CHAT_FILE, "w", encoding="utf-8") as f:
    #         json.dump(history, f, indent=2)
    # except Exception as e:
    #     print(f"Failed to save assistant message locally: {e}")

    MAX_HISTORY_LENGTH = 10
    redis_client.ltrim(key, -MAX_HISTORY_LENGTH, -1)
    redis_client.set(f"{key}:last_message", json.dumps(msg))

def get_last_user_message(user_id,tenantId):
    last = redis_client.get(f"chat:{user_id}_{tenantId}:last_message")
    return json.loads(last) if last else None

def get_chat_history(user_id,tenantId):
    key = f"chat:{user_id}_{tenantId}"
    return [json.loads(m) for m in redis_client.lrange(key, 0, -1)]

def get_last_assistant_sql(user_id,tenantId):
    sql = redis_client.get(f"chat:{user_id}_{tenantId}:last_sql_query")
    return sql if sql else None


# User input handler

def get_last_schema_list(user_id,tenantId):
    last = redis_client.get(f"chat:{user_id}_{tenantId}:previous_schemas")
    return json.loads(last) if last else None

def handle_user_input(user_id,tenantId, new_input):
    last_msg = get_last_user_message(user_id,tenantId)
    last_user_input = last_msg["content"] if last_msg and last_msg["role"] == "user" else None
    is_followup = is_follow_up_keyword_based(new_input, last_user_input)
    last_sql = get_last_assistant_sql(user_id,tenantId) if is_followup else None
    last_schemas = get_last_schema_list(user_id,tenantId) if is_followup else None 
    store_user_message(user_id,tenantId, new_input, is_follow_up=is_followup,previous_user_input=last_user_input,previous_sql_query=last_sql,previous_schemas=last_schemas)
    return is_followup

# Top schema selector

def get_top_schemas(user_input, schema_list, threshold=0.0):
    user_embedding = model1.encode(user_input, convert_to_tensor=True)
    scores = []
    for s in schema_list:
        schema_embedding = model1.encode(s["description"], convert_to_tensor=True)
        sim = util.cos_sim(user_embedding, schema_embedding).item()
        scores.append((s["name"], sim))
    return [item for item in sorted(scores, key=lambda x: x[1], reverse=True) if item[1] >= threshold]

def store_last_sql_query(user_id,tenantId, sql_query):
    redis_client.set(f"chat:{user_id}_{tenantId}:last_sql_query", sql_query)

##################################
def smartReportingTool(user_input,user_id,tenantId):
    chat_history = get_chat_history(user_id,tenantId)
    sql_query=""
    df = pd.DataFrame() 

    if user_input:
        is_follow_up = handle_user_input(user_id,tenantId, user_input)
        top_schema = get_top_schemas(user_input, schema.schema)
        schema_names = [s[0] for s in top_schema]
        selected_schemas = [s for s in schema.schema if s["name"] in schema_names]
        schema_descriptions = "\n".join([f"- {s['name']}: {s['description']}" for s in selected_schemas])

        with open("C:/Users/DanukaDilshanRathnay/Desktop/report Automation/Prompts/schema_prompt.txt",'r',encoding="UTF-8") as f:
            schema_prompt=f.read()

        clarification_prompt=schema_prompt.format(schema_descriptions=schema_descriptions,top_schema=top_schema,user_input=user_input)

        schema_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": clarification_prompt}, {"role": "user", "content": user_input}],
            max_tokens=200, temperature=0.1, top_p=0.1

        )

        # Extract new schemas from LLM output

        new_schemas = [s.strip() for s in schema_response.choices[0].message.content.strip().split(",")]

       # Merge if follow-up and drop the duplicates

        if is_follow_up:
            previous_schemas = get_last_schema_list(user_id, tenantId) or []
            final_selected = list(set(previous_schemas + new_schemas))
        else:
            final_selected = new_schemas

        # Store final schemas in Redis for next userQ

        redis_client.set(f"chat:{user_id}_{tenantId}:previous_schemas", json.dumps(final_selected))
        chunks = []
        with open("fixed_RAG_queries.jsonl", "r", encoding="utf-8") as f:
            for raw_line in f:
                cleaned_line = re.sub(r"[\x00-\x1F\x7F]", "", raw_line)
                try:
                    chunk = json.loads(cleaned_line)
                    if chunk["category"] in final_selected:
                        chunks.append(chunk)
                except json.JSONDecodeError:
                    continue

        embeddings = []
        for chunk in chunks:
            combined_text = chunk["table_requirement"] + "\n" + chunk["sql_query"]
            embedding = model1.encode(combined_text)
            embeddings.append(embedding)

        user_embedding = model1.encode(user_input)
        scores = [util.cos_sim(user_embedding, emb)[0][0].item() for emb in embeddings]
        top_n = 3
        top_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_n]
        top_chunks = [chunks[i] for i in top_indices]


        rag_examples = "\n\n".join([
            f"-- {chunk['table_requirement']}\n{chunk['sql_query']}"
            for chunk in top_chunks
        ])

        selected_schema_text = "\n".join([s["tables"] for s in schema.schema if s["name"] in final_selected])
        merged_schema_text = merge_unique_table_definitions(final_selected, schema.schema)
 
        with open("C:/Users/DanukaDilshanRathnay/Desktop/report Automation/Prompts/SQL_prompt.txt",'r',encoding="UTF-8") as f:
            sql_prompt=f.read()
        SYSTEM_PROMPT =sql_prompt.format(merged_schema_text=merged_schema_text,user_input=user_input,current_date=now.day,
                                        current_year=now.year, current_month=now.month,rag_examples=rag_examples)
        chat_messages = [
                {"role": "assistant" if m.get("role") == "AI" else m.get("role"), 
                "content": m.get("content"),
                "follow_up": m.get("follow_up"),
                "previous_user_input": m.get("previous_user_input"),
                "previous_sql_query": m.get("previous_sql_query")}
                for m in get_chat_history(user_id, tenantId)
                if m.get("role") in ("user", "AI") and isinstance(m.get("content"), str) and m.get("content").strip()
            ]
        response_sql = ""
        stream = client.chat.completions.create(
        model="gpt-4o-mini",
                    messages=[{"role": "system", "content": SYSTEM_PROMPT},*chat_messages[-6:]],
                    temperature=0,
                    max_tokens=1500,
                    n=1,
                    top_p=0.1)
        response_sql = stream.choices[0].message.content
        sql_query = response_sql.replace("```sql", "").replace("```", "").strip()
        store_last_sql_query(user_id,tenantId, sql_query)
        def remove_code_fences(text: str) -> str:
                """Remove markdown code fences like ```sql or ```python or ``` from the input text."""
                pattern = r"```[a-zA-Z]*\n|```"
                cleaned_text = re.sub(pattern, "", text)
                return cleaned_text.strip()
        response_sql=remove_code_fences(sql_query)
        
        result_table = execute_sql(response_sql)
        df = pd.DataFrame(result_table)
        blobURL,BlobSavestatus=savetoBlog(df,user_id,tenantId)

        output = {
        #"userInput": user_input,
        "sqlQuery": sql_query,
        "BlobURL":blobURL,
        "BlobStatus":BlobSavestatus
        }
        jsondu=json.dumps(output,default=str)

        return  jsondu


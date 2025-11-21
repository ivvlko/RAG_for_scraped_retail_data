import os
import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from openai import OpenAI
import psycopg2
from psycopg2.extras import RealDictCursor

from config import rag_configs

PG_URI = os.getenv("PG_URI", "postgresql://postgres:postgres@db:5432/postgres")
EMBEDDING_MODEL = rag_configs.get("EMBEDDING_MODEL")
LLM_MODEL = rag_configs.get("LLM_MODEL")
TOP_K = rag_configs.get("TOP_K", 5)
SYSTEM_PROMPT = rag_configs.get("SYSTEM_PROMPT")

client = OpenAI(api_key=os.getenv("OPEN_AI_API_KEY"))

app = FastAPI()


def get_db():
    conn = psycopg2.connect(PG_URI, cursor_factory=RealDictCursor)
    return conn


def embed(text: str):
    resp = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text
    )
    return resp.data[0].embedding


async def semantic_search(query: str, top_k: int = TOP_K):
    vector = embed(query)

    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT 
            id,
            name,
            url,
            price,
            text,
            1 - (embedding <=> %s::vector) AS score
        FROM fosils_embeddings
        ORDER BY embedding <=> %s::vector
        LIMIT %s;
        """,
        (vector, vector, top_k)
    )

    results = cur.fetchall()
    conn.close()
    return results


async def generate_gpt_answer(context: str, question: str):
    completion = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"}
        ]
    )
    return completion.choices[0].message.content


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            data = await ws.receive_text()
            try:
                msg = json.loads(data)
                query = msg.get("query")
                top_k = msg.get("top_k", TOP_K)
                use_gpt = msg.get("gpt_answer", False)

                if not query:
                    await ws.send_text(json.dumps({"error": "No query provided"}))
                    continue

                results = await semantic_search(query, top_k)

                response = {"results": results}

                if use_gpt:
                    context = "\n\n".join([r["text"] for r in results])
                    answer = await generate_gpt_answer(context, query)
                    response["answer"] = answer

                await ws.send_text(json.dumps(response))

            except Exception as e:
                await ws.send_text(json.dumps({"error": str(e)}))
    except WebSocketDisconnect:
        print("Client disconnected")

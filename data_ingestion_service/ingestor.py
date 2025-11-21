import os
import json
from pathlib import Path
from openai import OpenAI
from sqlalchemy import create_engine, Column, String, Float, Integer
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.types import UserDefinedType
from sqlalchemy_utils import database_exists, create_database

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIM = 1536 if "large" in EMBEDDING_MODEL else 1536
PG_URI = os.getenv("PG_URI", "postgresql://postgres:postgres@db:5432/postgres")
Base = declarative_base()

class Vector(UserDefinedType):
    def __init__(self, dimensions=1536):
        self.dimensions = dimensions
    def get_col_spec(self):
        return f"vector({self.dimensions})"
    def bind_expression(self, bindvalue):
        return bindvalue
    def column_expression(self, col):
        return col

class ProductEmbedding(Base):
    __tablename__ = "fosils_embeddings"
    id = Column(String, primary_key=True)
    name = Column(String)
    url = Column(String)
    price = Column(Float)
    chunk_index = Column(Integer)
    text = Column(String)
    embedding = Column(Vector(1536))

class DataIngestor:
    def __init__(self, processed_dir="/data/processed/fosili"):
        self.processed_dir = Path(processed_dir)
        self.client = OpenAI(api_key=os.getenv("OPEN_AI_API_KEY"))
        self.engine = create_engine(PG_URI)
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def embed_text(self, text: str) -> list:
        resp = self.client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=text
        )
        return resp.data[0].embedding

    def ingest_file(self, filepath: Path):
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        session = self.Session()
        try:
            for idx, chunk in enumerate(data.get("chunks", [])):
                embedding = self.embed_text(chunk)
                record_dict = {
                    "id": f"{data['id']}_{idx}",
                    "name": data["name"],
                    "url": data["url"],
                    "price": data["price"],
                    "chunk_index": idx,
                    "text": chunk,
                    "embedding": embedding
                }
                stmt = insert(ProductEmbedding).values(**record_dict)
                stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
                session.execute(stmt)
            session.commit()
            print(f"Ingested {filepath.name} ({len(data.get('chunks', []))} chunks)")
        finally:
            session.close()

    def run(self):
        files = list(self.processed_dir.glob("*.json"))
        for file in files:
            self.ingest_file(file)

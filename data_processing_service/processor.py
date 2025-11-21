import json
import os
from utils import clean_text, clean_price
from chunker import chunk_text


class DataProcessor:
    def __init__(self, raw_dir="/data/raw/fosili", processed_dir="/data/processed/fosili"):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        os.makedirs(self.processed_dir, exist_ok=True)

    def process_file(self, filepath: str):
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        processed = {
            "id": data.get("id"),
            "name": clean_text(data.get("name")),
            "price": clean_price(data.get("price")),
            "url": data.get("url"),
            "chunks": chunk_text(clean_text(data.get("description")))
        }

        out_path = os.path.join(self.processed_dir, f"{processed['id']}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(processed, f, ensure_ascii=False, indent=2)

        print(f"Processed {filepath} → {out_path}")

    def run(self):
        files = [os.path.join(self.raw_dir, f) for f in os.listdir(self.raw_dir) if f.endswith(".json")]
        for file in files:
            self.process_file(file)

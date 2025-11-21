import requests
from bs4 import BeautifulSoup
import json
from typing import List


class BaseScraper:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; RAG-Scraper/1.0)"
    }

    def fetch(self, url: str) -> str:
        response = requests.get(url, headers=self.headers, timeout=10)
        response.raise_for_status()
        return response.text

    def parse_sitemap(self, sitemap_url: str) -> List[str]:
        xml = self.fetch(sitemap_url)
        soup = BeautifulSoup(xml, "xml")
        return [loc.text for loc in soup.find_all("loc")]

    def save_json(self, data: dict, path: str):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

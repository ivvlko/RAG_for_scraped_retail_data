import json
from bs4 import BeautifulSoup
from base_scraper import BaseScraper
import os


class FosilScraper(BaseScraper):
    SITEMAP_URL = "https://vantony.com/sitemap.xml"

    def get_fosili_links(self):
        #Return only URLs containing /4-fosili/.
        urls = self.parse_sitemap(self.SITEMAP_URL)
        fosili_urls = [u for u in urls if "/4-fosili/" in u]
        return fosili_urls

    def parse_product_page(self, url: str) -> dict:
        html = self.fetch(url)
        soup = BeautifulSoup(html, "html.parser")

        script_tag = soup.find("script", type="application/ld+json")
        if not script_tag:
            return None

        raw_json = script_tag.text

        cleaned = (
            raw_json
            .replace("\n", " ")
            .replace("\t", " ")
            .replace("\r", "")
        )

        cleaned = "".join(ch for ch in cleaned if ord(ch) >= 32)

        cleaned = cleaned.replace(", }", " }")
        cleaned = cleaned.replace(", }", " }")
        cleaned = cleaned.replace(", ]", " ]")

        try:
            data = json.loads(cleaned)
        except Exception as e:
            print(f"WARNING: Failed to parse JSON-LD on {url}: {e}")
            print("Raw JSON-LD snippet:", raw_json[:200])
            return None

        product_id_tag = soup.find("input", {"name": "product_id"})
        product_id = product_id_tag["value"] if product_id_tag else None

        return {
            "id": product_id,
            "name": data.get("name"),
            "description": data.get("description"),
            "price": data.get("offers", {}).get("price"),
            "url": url
        }


    def run(self):
        fosili_urls = self.get_fosili_links()
        os.makedirs("/data/raw/fosili", exist_ok=True)

        for url in fosili_urls:
            print(f"Scraping {url}")
            product = self.parse_product_page(url)
            if product:
                outfile = f"/data/raw/fosili/{product['id']}.json"
                self.save_json(product, outfile)
                print(f"Saved {outfile}")
            else:
                print(f"Failed to extract product: {url}")

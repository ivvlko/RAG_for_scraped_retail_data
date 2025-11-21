import re
import html


def clean_text(text: str) -> str:
    # some basic html cleaning here
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"\s{2,}", " ", text)
    text = text.strip()
    return text


def clean_price(price: str) -> float:
    #Making sure price is float
    if not price:
        return 0.0
    price = price.replace(",", ".").strip()
    try:
        return float(price)
    except ValueError:
        return 0.0

# simple chunk strategy is here based on our data 
# paragraphs, newlines, and word boundaries are respected first(mostly for the description field)
# and only if adding paragraph exceeds max_length, save current chunk


def chunk_text(text: str, max_length: int = 1000) -> list:
    if not text:
        return []

    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]

    chunks = []
    current_chunk = ""

    for para in paragraphs:
        if len(para) > max_length:
            words = para.split()
            for word in words:
                if len(current_chunk) + len(word) + 1 <= max_length:
                    current_chunk += (" " if current_chunk else "") + word
                else:
                    chunks.append(current_chunk.strip())
                    current_chunk = word
            continue

        if len(current_chunk) + len(para) + 1 > max_length:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = para
        else:
            current_chunk += ("\n" if current_chunk else "") + para

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks

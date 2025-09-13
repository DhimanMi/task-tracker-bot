from rapidfuzz import process, fuzz

def find_similar_titles(query: str, rows: list, limit: int = 5, score_cutoff: int = 60):
    titles = [r[1] for r in rows]
    mapping = {r[1]: r for r in rows}
    results = process.extract(query, titles, scorer=fuzz.WRatio, limit=limit)
    filtered = [(mapping[r[0]], r[1]) for r in results if r[1] >= score_cutoff]
    return filtered
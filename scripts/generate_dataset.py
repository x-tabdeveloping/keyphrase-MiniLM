import json
import zipfile
from pathlib import Path
from typing import Iterable, Optional

from tqdm import tqdm


def extract_paragraphs(entry: dict) -> list[str]:
    paragraphs = []
    integer_keys = [key for key in entry.keys() if key.isdigit()]
    for key in sorted(integer_keys):
        if "para" in entry[key]:
            paragraphs.extend(entry[key]["para"])
    return paragraphs


def parse_entry(text: str) -> Optional[dict]:
    try:
        entry = json.loads(text)
        keywords = entry.get("keyword", [])
        if len(keywords) == 0:
            return None
        paragraphs = extract_paragraphs(entry)
        if len(paragraphs) == 0:
            return None
        return dict(content="\n".join(paragraphs), keywords=keywords)
    except Exception:
        return None


def stream_entries() -> Iterable[dict]:
    zip_paths = list(Path("M3LS dataset").glob("*.zip"))
    for zip_path in tqdm(zip_paths, desc="Going through all sources."):
        source = zip_path.stem
        print(f"Processing {zip_path}")
        with zipfile.ZipFile(source) as zip_ref:
            src_dirs = (
                dir
                for dir in zipfile.Path(zip_ref).joinpath(source).iterdir()
                if dir.is_dir()
            )
            for src_dir in src_dirs:
                article_files = [
                    file
                    for file in src_dir.joinpath("articles").iterdir()
                    if file.is_file() and (file.suffix == ".json")
                ]
                for article_file in tqdm(article_files, desc="Streaming articles."):
                    with article_file.open() as in_file:
                        entry = parse_entry(in_file.read())
                        if entry:
                            yield entry


def main() -> None:
    keyword_file = Path("dat/keywords.jsonl")
    with keyword_file.open("w") as out_file:
        for entry in stream_entries():
            out_file.write(json.dumps(entry) + "\n")

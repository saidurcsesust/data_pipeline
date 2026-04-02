import json
from pathlib import Path

def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path



def write_json_file(payload, output_file: Path) -> Path:
    ensure_directory(output_file.parent)
    with output_file.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, indent=2, ensure_ascii=False)
    return output_file



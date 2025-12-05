import json
import os
from typing import Dict, Any, List

ROR_POLITO = "https://ror.org/00bgk9508"


def load_polito_works(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_all_datasets(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Build a list of all datasets including those with only Politecnico di Torino authors.
    Returns a list of datasets:
    [
        {
            "dataset_id": "...",
            "title": "...",
            "year": 2023
        },
        ...
    ]
    """
    all_datasets = []
    seen_ids = set()

    for work in works:
        authorships = work.get("authorships") or []

        # Determine if the work has at least one Polito author
        has_polito = False
        for auth in authorships:
            for inst in auth.get("institutions") or []:
                if inst.get("ror") == ROR_POLITO:
                    has_polito = True
                    break
            if has_polito:
                break

        if not has_polito:
            # Skip works without Polito authors
            continue

        work_id = work.get("id")
        if not work_id or work_id in seen_ids:
            continue

        seen_ids.add(work_id)

        title = work.get("display_name") or work.get("title")
        year = work.get("publication_year")

        all_datasets.append({
            "dataset_id": work_id,
            "title": title,
            "year": year,
        })

    # Sort by year (descending), then by title
    all_datasets.sort(
        key=lambda d: (d.get("year") or 0, d.get("title") or ""),
        reverse=True,
    )

    return all_datasets


def main() -> None:
    src = "data/polito_works.json"
    out = "data/all_datasets.json"

    if not os.path.exists(src):
        raise SystemExit(f"{src} not found – run get_data_from_OpenAlex.py first.")

    works = load_polito_works(src)
    all_datasets = build_all_datasets(works)

    with open(out, "w", encoding="utf-8") as f:
        json.dump(all_datasets, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(all_datasets)} datasets to {out}")


if __name__ == "__main__":
    main()


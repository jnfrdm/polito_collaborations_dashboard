import csv
import json
import os
import time
from collections import defaultdict
from typing import Dict, Any, List

import requests

ROR_POLITO = "https://ror.org/00bgk9508"
OPENALEX_INSTITUTION_API = "https://api.openalex.org/institutions/{}"


def load_polito_works(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_country_codes(path: str) -> Dict[str, Dict[str, Any]]:
    """
    Load country codes from CSV and create a mapping from country codes to country info.
    Returns a dictionary like:
    {
        "IT": {
            "name": "Italy",
            "latitude": 41.87194,
            "longitude": 12.56738,
            "coords": [41.87194, 12.56738]  # [lat, lng] for Leaflet
        },
        ...
    }
    """
    country_map = {}
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country_code = row.get("country", "").strip()
            if country_code:
                try:
                    latitude = float(row.get("latitude", 0))
                    longitude = float(row.get("longitude", 0))
                    country_map[country_code] = {
                        "name": row.get("name", "").strip(),
                        "latitude": latitude,
                        "longitude": longitude,
                        "coords": [latitude, longitude],  # Leaflet format: [lat, lng]
                    }
                except (ValueError, TypeError):
                    # Skip rows with invalid coordinates
                    continue
    return country_map


def get_institution_country_code(inst: Dict[str, Any], cache: Dict[str, str]) -> str:
    """
    Try to get the country_code from the institution object itself,
    otherwise fall back to querying the OpenAlex institution endpoint.
    """
    # Directly present in the work payload (most common case)
    if "country_code" in inst and inst["country_code"]:
        return inst["country_code"]

    inst_id = inst.get("id")
    if not inst_id:
        return ""

    # Example id: "https://openalex.org/I55143463"
    short_id = inst_id.rsplit("/", 1)[-1]

    if short_id in cache:
        return cache[short_id]

    url = OPENALEX_INSTITUTION_API.format(short_id)
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            cc = data.get("country_code") or ""
            cache[short_id] = cc
            # Be gentle with the API
            time.sleep(0.1)
            return cc
    except Exception:
        # In case of network errors or unexpected payloads,
        # just return empty and let the caller skip it.
        return ""

    return ""


def build_collaborations(works: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a dictionary keyed by country_code:
    {
      "IT": {
         "country_code": "IT",
         "collaborations": [
            {
               "partner": "University of Turin",
               "dataset_id": "...",
               "title": "...",
               "year": 2023
            },
            ...
         ]
      },
      ...
    }
    """
    inst_country_cache: Dict[str, str] = {}

    by_country: Dict[str, Dict[str, Any]] = {}

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
            # According to the data collection process this should be rare,
            # but we enforce it explicitly.
            continue

        # Collect external institutions (non-Polito)
        external_pairs = []  # (institution, country_code)
        for auth in authorships:
            for inst in auth.get("institutions") or []:
                if inst.get("ror") == ROR_POLITO:
                    continue

                country_code = get_institution_country_code(inst, inst_country_cache)
                if not country_code:
                    continue

                external_pairs.append((inst, country_code))

        # Skip works that only have Polito authors
        if not external_pairs:
            continue

        # Add this work for each external institution's country
        work_id = work.get("id")
        title = work.get("display_name") or work.get("title")
        year = work.get("publication_year")

        # Avoid adding the same (work, institution) twice per country
        seen_for_work_country = set()

        for inst, cc in external_pairs:
            key = (work_id, inst.get("id"), cc)
            if key in seen_for_work_country:
                continue
            seen_for_work_country.add(key)

            if cc not in by_country:
                by_country[cc] = {
                    "country_code": cc,
                    "collaborations": [],
                }

            by_country[cc]["collaborations"].append(
                {
                    "partner": inst.get("display_name"),
                    "dataset_id": work_id,
                    "title": title,
                    "year": year,
                }
            )

    return by_country


def main() -> None:
    src = "data/polito_works.json"
    country_codes_path = "data/all_country_codes.csv"
    out = "data/collaborations.json"

    if not os.path.exists(src):
        raise SystemExit(f"{src} not found – run get_data_from_OpenAlex.py first.")

    if not os.path.exists(country_codes_path):
        raise SystemExit(f"{country_codes_path} not found.")

    # Load country codes mapping
    country_map = load_country_codes(country_codes_path)
    print(f"Loaded {len(country_map)} country codes from {country_codes_path}")

    works = load_polito_works(src)
    data_by_country = build_collaborations(works)

    # Convert to a list sorted by number of collaborations (descending)
    # and enrich with country information from CSV
    result = []
    for cc, payload in data_by_country.items():
        collabs = payload["collaborations"]
        
        # Get country information from CSV
        country_info = country_map.get(cc, {})
        
        # Build the entry with country information
        entry = {
            "country_code": cc,
            "collaborations_count": len(collabs),
            "collaborations": sorted(
                collabs,
                key=lambda c: (c.get("year") or 0, c.get("partner") or ""),
                reverse=True,
            ),
        }
        
        # Add country information if available
        if country_info:
            entry["country"] = {
                "name": country_info.get("name", ""),
                "latitude": country_info.get("latitude", 0),
                "longitude": country_info.get("longitude", 0),
                "coords": country_info.get("coords", [0, 0]),  # [lat, lng] for Leaflet
            }
        else:
            # If country code not found in CSV, still include basic info
            entry["country"] = {
                "name": cc,  # Fallback to code if not found
                "latitude": 0,
                "longitude": 0,
                "coords": [0, 0],
            }
            print(f"Warning: Country code '{cc}' not found in country codes CSV")
        
        result.append(entry)

    result.sort(key=lambda x: x["collaborations_count"], reverse=True)

    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(result)} countries to {out}")


if __name__ == "__main__":
    main()



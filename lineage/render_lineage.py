#!/usr/bin/env python3
"""
Read dbt manifest.json and emit a simple Mermaid flowchart of model dependencies.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


def load_manifest(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"manifest not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def model_edges(manifest: Dict[str, Any]) -> Tuple[List[str], List[Tuple[str, str]]]:
    nodes: Set[str] = set()
    edges: List[Tuple[str, str]] = []

    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        name = node.get("name")
        if not name:
            continue
        nodes.add(name)
        for dep_id in node.get("depends_on", {}).get("nodes", []):
            dep = manifest.get("nodes", {}).get(dep_id, {})
            if dep.get("resource_type") != "model":
                continue
            dep_name = dep.get("name")
            if dep_name:
                edges.append((dep_name, name))

    return sorted(nodes), edges


def to_mermaid(nodes: List[str], edges: List[Tuple[str, str]]) -> str:
    lines = ["flowchart LR"]
    safe = lambda s: "".join(c if c.isalnum() else "_" for c in s)

    for n in nodes:
        lines.append(f"  {safe(n)}[\"{n}\"]")

    seen: Set[Tuple[str, str]] = set()
    for src, dst in edges:
        key = (src, dst)
        if key in seen:
            continue
        seen.add(key)
        lines.append(f"  {safe(src)} --> {safe(dst)}")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Render dbt lineage as Mermaid.")
    parser.add_argument(
        "--manifest",
        default="dbt_project/target/manifest.json",
        help="Path to manifest.json",
    )
    parser.add_argument(
        "--output",
        default="lineage/lineage.mmd",
        help="Output .mmd file",
    )
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    output_path = Path(args.output)

    try:
        manifest = load_manifest(manifest_path)
        nodes, edges = model_edges(manifest)
        text = to_mermaid(nodes, edges)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text, encoding="utf-8")

        md_path = output_path.with_suffix(".md")
        md_path.write_text(
            "# dbt model lineage (Mermaid)\n\n```mermaid\n" + text + "```\n",
            encoding="utf-8",
        )
        print(f"Wrote {output_path} and {md_path}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

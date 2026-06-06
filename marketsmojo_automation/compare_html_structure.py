from pathlib import Path
from bs4 import BeautifulSoup
from collections import Counter
import hashlib
import json

real_file = Path.home() / "Downloads" / "mojo.html"
auto_files = list(Path("saved_pages").glob("marketsmojo_mojoscore_*.html")) + list(Path("saved_pages").glob("marketsmojo_server_mojoscore_*.html"))
auto_file = sorted(auto_files, key=lambda x: x.stat().st_mtime)[-1]

def analyse(path):
    html = path.read_text(errors="ignore")
    soup = BeautifulSoup(html, "lxml")

    tags = Counter(tag.name for tag in soup.find_all())

    classes = Counter()
    ids = Counter()
    ng_clicks = Counter()

    for tag in soup.find_all():
        if tag.get("class"):
            for c in tag.get("class"):
                classes[c] += 1
        if tag.get("id"):
            ids[tag.get("id")] += 1
        if tag.get("ng-click"):
            ng_clicks[tag.get("ng-click")] += 1

    text = soup.get_text(" ", strip=True)

    return {
        "file": str(path),
        "size_bytes": path.stat().st_size,
        "html_hash": hashlib.md5(html.encode(errors="ignore")).hexdigest(),
        "title": soup.title.get_text(strip=True) if soup.title else "",
        "tag_count": sum(tags.values()),
        "tags": tags,
        "top_classes": classes.most_common(40),
        "top_ids": ids.most_common(40),
        "ng_clicks": ng_clicks.most_common(40),
        "has_eajee": "Eajee" in text,
        "has_mojoscore": "MOJOSCORE" in text,
        "has_show_more": "Show More" in text,
        "stock_rows_hint": text.count("Score"),
    }

real = analyse(real_file)
auto = analyse(auto_file)

print("REAL FILE:", real["file"])
print("AUTO FILE:", auto["file"])
print()

for key in ["size_bytes", "title", "tag_count", "has_eajee", "has_mojoscore", "has_show_more", "stock_rows_hint"]:
    print(f"{key}:")
    print("  real:", real[key])
    print("  auto:", auto[key])
    print()

print("TAG DIFFERENCE:")
all_tags = set(real["tags"]) | set(auto["tags"])
for tag in sorted(all_tags):
    r = real["tags"].get(tag, 0)
    a = auto["tags"].get(tag, 0)
    if r != a:
        print(f"  {tag}: real={r}, auto={a}, diff={a-r}")

print()
print("COMMON STRUCTURE SCORE:")
same_tags = sum(min(real["tags"].get(t,0), auto["tags"].get(t,0)) for t in all_tags)
total_tags = max(sum(real["tags"].values()), sum(auto["tags"].values()))
score = round((same_tags / total_tags) * 100, 2) if total_tags else 0
print(f"  {score}%")

report = {
    "real": {k: v for k, v in real.items() if k != "tags"},
    "auto": {k: v for k, v in auto.items() if k != "tags"},
    "tag_diff": {
        tag: {
            "real": real["tags"].get(tag, 0),
            "auto": auto["tags"].get(tag, 0),
            "diff": auto["tags"].get(tag, 0) - real["tags"].get(tag, 0),
        }
        for tag in sorted(all_tags)
        if real["tags"].get(tag, 0) != auto["tags"].get(tag, 0)
    },
    "structure_score_percent": score,
}

Path("logs/html_structure_compare.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
print()
print("Saved detailed report: logs/html_structure_compare.json")

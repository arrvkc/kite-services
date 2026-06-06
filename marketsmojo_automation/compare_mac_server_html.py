from pathlib import Path
from bs4 import BeautifulSoup
from collections import Counter

mac_file = sorted(Path("saved_pages").glob("marketsmojo_mojoscore_*.html"), key=lambda x: x.stat().st_mtime)[-1]
server_file = sorted(Path("saved_pages").glob("marketsmojo_server_mojoscore_*.html"), key=lambda x: x.stat().st_mtime)[-1]

def analyse(path):
    html = path.read_text(errors="ignore")
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    tags = Counter(tag.name for tag in soup.find_all())
    classes = Counter()
    ids = Counter()

    for tag in soup.find_all():
        for c in tag.get("class", []):
            classes[c] += 1
        if tag.get("id"):
            ids[tag.get("id")] += 1

    return {
        "file": str(path),
        "size": path.stat().st_size,
        "title": soup.title.get_text(strip=True) if soup.title else "",
        "tag_count": sum(tags.values()),
        "tags": tags,
        "classes": classes,
        "ids": ids,
        "has_eajee": "Eajee" in text,
        "has_mojoscore": "MOJOSCORE" in text,
        "has_show_more": "Show More" in text,
        "score_count": text.count("Score"),
        "text_len": len(text),
    }

mac = analyse(mac_file)
server = analyse(server_file)

print("MAC FILE:", mac["file"])
print("SERVER FILE:", server["file"])
print()

for k in ["size", "title", "tag_count", "text_len", "has_eajee", "has_mojoscore", "has_show_more", "score_count"]:
    print(f"{k}:")
    print("  mac   :", mac[k])
    print("  server:", server[k])
    print()

print("TAG DIFFERENCE:")
for tag in sorted(set(mac["tags"]) | set(server["tags"])):
    m = mac["tags"].get(tag, 0)
    s = server["tags"].get(tag, 0)
    if m != s:
        print(f"  {tag}: mac={m}, server={s}, diff={s-m}")

print()
print("TOP CLASS DIFFERENCES:")
for cls in sorted(set(mac["classes"]) | set(server["classes"])):
    m = mac["classes"].get(cls, 0)
    s = server["classes"].get(cls, 0)
    if abs(s - m) >= 20:
        print(f"  {cls}: mac={m}, server={s}, diff={s-m}")

print()
print("TOP ID DIFFERENCES:")
for id_ in sorted(set(mac["ids"]) | set(server["ids"])):
    m = mac["ids"].get(id_, 0)
    s = server["ids"].get(id_, 0)
    if m != s:
        print(f"  {id_}: mac={m}, server={s}, diff={s-m}")

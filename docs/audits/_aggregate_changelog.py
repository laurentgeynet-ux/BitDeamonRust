# -*- coding: utf-8 -*-
"""Merge docs/audits/changelog/A*.md fragments into CHANGELOG.md [Unreleased].

Each audit running in `apply` mode writes its Keep-a-Changelog entries to
docs/audits/changelog/ANN.md instead of editing CHANGELOG.md directly —
that eliminates merge conflicts between parallel audits. After the PRs are
merged, run this once:

    python docs/audits/_aggregate_changelog.py

It groups fragment entries under their `### Section` headings, merges them
into the matching sections of `## [Unreleased]` (creating sections that do
not exist yet, at the end of the block), then deletes the fragments.
"""

import glob
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CHANGELOG = os.path.join(ROOT, "CHANGELOG.md")
FRAG_DIR = os.path.join(ROOT, "docs", "audits", "changelog")

# Keep a Changelog canonical order — used when creating new sections.
SECTION_ORDER = ["Added", "Changed", "Deprecated", "Removed", "Fixed", "Security"]


def trim(lines):
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return lines


def parse_fragment(path):
    """Return {section_title: [entry_lines]} from one fragment file."""
    sections, cur = {}, None
    with open(path, encoding="utf-8") as f:
        for line in f.read().split("\n"):
            m = re.match(r"^###\s+(.*\S)\s*$", line)
            if m:
                cur = m.group(1)
                sections.setdefault(cur, [])
            elif cur is not None:
                sections[cur].append(line)
    return {k: trim(v) for k, v in sections.items() if trim(v)}


def main():
    frags = sorted(glob.glob(os.path.join(FRAG_DIR, "A*.md")))
    if not frags:
        print("no fragments in docs/audits/changelog/ — nothing to do")
        return 0

    # Collect entries per section, fragments processed in audit-number order.
    pending, skipped = {}, []
    for path in frags:
        secs = parse_fragment(path)
        if not secs:
            skipped.append(path)
            continue
        for title, lines in secs.items():
            pending.setdefault(title, []).extend(lines)

    if not pending:
        print("fragments contain no '### Section' entries — nothing merged")
        return 1

    lines = open(CHANGELOG, encoding="utf-8").read().split("\n")
    try:
        u = lines.index("## [Unreleased]")
    except ValueError:
        print("CHANGELOG.md has no '## [Unreleased]' section")
        return 1
    end = next((i for i in range(u + 1, len(lines)) if lines[i].startswith("## ")),
               len(lines))

    def last_nonblank(start, stop):
        i = stop - 1
        while i >= start and not lines[i].strip():
            i -= 1
        return i

    # Map existing '### Title' sections inside [Unreleased] to their ranges.
    # First occurrence wins when the same title appears several times.
    heads = [i for i in range(u + 1, end) if lines[i].startswith("### ")]
    sec_at = {}
    for h in heads:
        sec_at.setdefault(lines[h][4:].strip().lower(), h)
    sec_end = {h: next((x for x in heads if x > h), end) for h in heads}

    # Surgical inserts only — everything outside the insertion points is
    # preserved byte-for-byte (no reformatting of the rest of the file).
    ordered = sorted(pending, key=lambda t: SECTION_ORDER.index(t)
                     if t in SECTION_ORDER else len(SECTION_ORDER))
    edits, new_secs = [], []
    for title in ordered:
        entries = pending[title]
        h = sec_at.get(title.lower())
        if h is not None:
            ins = last_nonblank(h + 1, sec_end[h]) + 1
            edits.append((ins, entries))
        else:
            new_secs.append(title)
    if new_secs:
        # Insert at end of [Unreleased]: blank line before the first new
        # '###' heading, one blank between consecutive new sections, no
        # trailing blank (the block's existing trailing blanks remain).
        payload = [""]
        for title in new_secs:
            if len(payload) > 1:
                payload.append("")
            payload += [f"### {title}", ""] + pending[title]
        edits.append((last_nonblank(u + 1, end) + 1, payload))

    for ins, payload in sorted(edits, key=lambda e: -e[0]):
        lines[ins:ins] = payload

    with open(CHANGELOG, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    merged = [os.path.basename(p) for p in frags if p not in skipped]
    print(f"merged {len(merged)} fragment(s) into [Unreleased]: "
          + ", ".join(merged))
    for path in frags:
        if path not in skipped:
            os.remove(path)
    if skipped:
        print("skipped (no '### Section' found, left on disk): "
              + ", ".join(os.path.basename(p) for p in skipped))
    print("review CHANGELOG.md, then commit: "
          "docs(changelog): aggregate audit entries")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Overlay your homeserver.yaml onto a fully documented template of every option.

The template is rendered from schema/synapse-config.schema.yaml: each option gets its
description, and sits commented out at its default (`#key: default`). Options you set
are uncommented with your value, so a sparse deployed config becomes a complete,
documented one. Your file stays the source of truth; use the merged copy to read, or
diff it against a copy made from an older schema to see which options an upgrade added.

    scripts-dev/merge_yaml.py [-c homeserver.yaml]
                              [-s schema/synapse-config.schema.yaml]
                              [-o homeserver.merged.yaml]

A -s that is not a *.schema.yaml file is treated as a plain sample config and
overlaid as is (e.g. docs/sample_config.yaml).

The output contains your secrets (it is gitignored as /homeserver*.yaml).
Needs PyYAML. Options your file sets that the template lacks are appended as a
commented block at the end and listed on stderr.
"""

import argparse
import json
import os
import re
import sys
import tempfile
import textwrap
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

import yaml

KEY_RE = re.compile(r"^(\s*)([A-Za-z0-9_.\-]+):(?:\s+(.*?))?\s*$")


def scalar(v: Any) -> str:
    """Format a scalar as a JSON literal, which is valid YAML."""
    if v is None:
        return "null"
    try:
        return json.dumps(v, ensure_ascii=False)
    except TypeError:
        return json.dumps(str(v), ensure_ascii=False)


def is_scalar(v: Any) -> bool:
    """Return whether v is neither a dict nor a list."""
    return not isinstance(v, (dict, list))


def trailing_comment(text: str) -> str:
    """Return a YAML comment suffix outside any quoted scalar, if present."""
    quote = None
    escaped = False
    for i, char in enumerate(text):
        if quote == '"' and escaped:
            escaped = False
            continue
        if quote == '"' and char == "\\":
            escaped = True
            continue
        if quote and char == quote:
            quote = None
        elif not quote and char in "'\"" and (i == 0 or text[i - 1] in " \t[{,:"):
            # A quote only opens a scalar at a token start; an apostrophe inside
            # a plain scalar (don't) must not hide a later comment.
            quote = char
        elif not quote and char == "#" and (i == 0 or text[i - 1].isspace()):
            return text[i:]
    return ""


def dump_block(v: Any, indent: int) -> str:
    """Dump v as indented block-style YAML lines."""
    text = yaml.safe_dump(
        v, default_flow_style=False, sort_keys=False, allow_unicode=True
    )
    pad = " " * indent
    return "".join(pad + ln + "\n" for ln in text.splitlines())


def lookup(cfg: Any, path: Sequence[str]) -> tuple[bool, Any]:
    """Return (found, value) for a key path in a nested dict."""
    cur = cfg
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return False, None
        cur = cur[p]
    return True, cur


def leaves(v: Any, path: tuple[str, ...] = ()) -> Iterator[tuple[tuple[str, ...], Any]]:
    """Leaf paths of v; lists and empty dicts count as leaves."""
    if isinstance(v, dict) and v:
        for k, sub in v.items():
            yield from leaves(sub, path + (k,))
    else:
        yield path, v


class Off(str):
    """A template line that is commented out in the output unless overlaid."""

    __slots__ = ()


DISABLED_RE = re.compile(r"^#( *)((?:[A-Za-z0-9_.\-]+:|-)(?:\s.*)?)$")


# A plain sample marks options as `#key:` or `#  key:` (two or more spaces);
# a single space after `#` is prose, so it is left alone.
SAMPLE_DISABLED_RE = re.compile(r"^#( {2,}|)((?:[A-Za-z0-9_.\-]+:|-)(?:\s.*)?)$")


def resolve(node: dict[str, Any], defs: dict[str, Any]) -> dict[str, Any]:
    """Inline a $ref (siblings win) so the node carries its own type/default."""
    ref = node.get("$ref", "")
    if ref.startswith("#/$defs/"):
        return {**defs.get(ref[len("#/$defs/") :], {}), **node}
    return node


def render_schema(schema: dict[str, Any]) -> list[str]:
    """Render a JSON schema as a documented template with options commented out."""
    defs = schema.get("$defs", {})
    out = [
        "## Synapse homeserver.yaml: every option, rendered from the config schema.\n",
        "## Lines starting with `##` are documentation. Lines starting with a single\n",
        "## `#` are options at their default; merge_yaml.py enables the ones you set.\n",
        "\n",
    ]

    def doc(node: dict[str, Any], indent: int) -> None:
        text = (node.get("description") or "").strip()
        lines = []
        for ln in text.splitlines():
            lines.extend(
                textwrap.wrap(
                    ln, 88 - indent, break_long_words=False, break_on_hyphens=False
                )
                or [""]
            )
        example = node.get("examples")
        if example:
            dumped = yaml.safe_dump(
                example[0], default_flow_style=None, sort_keys=False, allow_unicode=True
            )
            lines += ["", "Example:"] + [x for x in dumped.splitlines() if x != "..."]
        pad = "##" + " " * (indent + 1)
        out.extend((pad + ln).rstrip() + "\n" for ln in lines)

    def emit(props: dict[str, Any], indent: int) -> None:
        for key, raw in props.items():
            node = resolve(raw, defs)
            doc(node, indent)
            pad = "#" + " " * indent
            # Objects whose default is empty are expanded too, so their options
            # are documented rather than hidden behind `#key: {}`.
            if (
                isinstance(node.get("properties"), dict)
                and node.get("default", {}) == {}
            ):
                out.append(f"{pad}{key}:\n")
                emit(node["properties"], indent + 2)
            elif "default" not in node:
                out.append(f"{pad}{key}:\n")
            else:
                val = node["default"]
                if is_scalar(val) or not val:
                    lit = (
                        scalar(val)
                        if is_scalar(val)
                        else ("[]" if isinstance(val, list) else "{}")
                    )
                    out.append(f"{pad}{key}: {lit}\n")
                else:
                    out.append(f"{pad}{key}:\n")
                    out.extend(
                        "#" + ln for ln in dump_block(val, indent + 2).splitlines(True)
                    )
            if indent == 0:
                out.append("\n")

    emit(schema["properties"], 0)
    return out


def enable_disabled(
    lines: Iterable[str], pattern: re.Pattern[str] = DISABLED_RE
) -> list[str]:
    """Turn `#key: v` template lines into in-memory YAML lines tagged as Off."""
    res = []
    for ln in lines:
        m = pattern.match(ln.rstrip("\n"))
        res.append(Off(m.group(1) + m.group(2) + "\n") if m else ln)
    return res


def merge(  # pylint: disable=too-many-locals
    sample_lines: Sequence[str], user: Any
) -> tuple[list[str], set[tuple[str, ...]]]:
    """Overlay user values onto the sample lines; return (lines, used paths)."""
    out: list[str] = []
    stack: list[tuple[int, str]] = []
    used: set[tuple[str, ...]] = set()
    i, n = 0, len(sample_lines)

    def block_end(start: int, key_indent: int) -> int:
        """Index after the indented block (comments included) under a key line."""
        j = start
        while j < n:
            ln = sample_lines[j]
            if ln.strip() and not ln.lstrip().startswith("#"):
                line_indent = len(ln) - len(ln.lstrip())
                if line_indent < key_indent or (
                    line_indent == key_indent and not ln.lstrip().startswith("-")
                ):
                    break
            j += 1
        # do not swallow trailing blank/comment lines that belong to the next key
        while j > start and (
            not sample_lines[j - 1].strip()
            or sample_lines[j - 1].lstrip().startswith("#")
        ):
            j -= 1
        return j

    while i < n:
        line = sample_lines[i]
        m = KEY_RE.match(line.rstrip("\n"))
        if not m or line.lstrip().startswith(("#", "-")):
            out.append(line)
            i += 1
            continue
        indent, key, inline = len(m.group(1)), m.group(2), m.group(3) or ""
        comment = trailing_comment(inline)
        if comment and not comment.startswith(" "):
            comment = " " + comment
        while stack and stack[-1][0] >= indent:
            stack.pop()
        path = tuple(k for _, k in stack) + (key,)
        has, val = lookup(user, path)
        stack.append((indent, key))
        child_end = block_end(i + 1, indent)
        has_children = child_end > i + 1 and any(
            sample_lines[j].strip() and not sample_lines[j].lstrip().startswith("#")
            for j in range(i + 1, child_end)
        )
        first = next(
            (
                sample_lines[j]
                for j in range(i + 1, child_end)
                if sample_lines[j].strip()
                and not sample_lines[j].lstrip().startswith("#")
            ),
            "",
        )
        sample_is_list = first.lstrip().startswith("-")
        if not has:
            out.append(line)
            if sample_is_list:  # copy the list block verbatim, never parse inside it
                body = slice(i + 1, child_end)
                out.extend(sample_lines[body])
                i = child_end
            else:
                i += 1
            continue
        if is_scalar(val):
            out.append(f"{' ' * indent}{key}: {scalar(val)}{comment}\n")
            used.add(path)
            i = child_end if has_children else i + 1
        elif (
            sample_is_list
            or (not has_children)
            or isinstance(val, list)
            or (isinstance(val, dict) and not val)
        ):
            # lists, and dicts where the sample has a scalar/empty value: replace whole
            out.append(
                f"{' ' * indent}{key}:{comment}\n"
                if val
                else f"{' ' * indent}{key}: {'[]' if isinstance(val, list) else '{}'}{comment}\n"
            )
            if val:
                out.append(dump_block(val, indent + 2))
            used.update(p for p, _ in leaves(val, path))
            i = child_end
        else:
            out.append(str(line))  # dict with sample children: enable and recurse
            i += 1
    return out, used


def main() -> None:
    """Parse arguments, merge the config into the sample and write the result."""
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("-c", "--config", default="homeserver.yaml")
    ap.add_argument("-s", "--sample", default="schema/synapse-config.schema.yaml")
    ap.add_argument("-o", "--output", default="homeserver.merged.yaml")
    a = ap.parse_args()
    with open(a.config, encoding="utf-8") as f:
        user = yaml.safe_load(f) or {}
    with open(a.sample, encoding="utf-8") as f:
        if a.sample.endswith((".schema.yaml", ".schema.yml")):
            lines = enable_disabled(render_schema(yaml.safe_load(f)))
        else:
            lines = enable_disabled(f.readlines(), SAMPLE_DISABLED_RE)
    out, used = merge(lines, user)
    out = ["#" + ln if isinstance(ln, Off) else ln for ln in out]
    missing = [
        p
        for p, _ in leaves(user)
        if p and p not in used and not any(p[:k] in used for k in range(1, len(p)))
    ]
    if missing:
        rest: dict[str, Any] = {}
        for p, v in leaves(user):
            if p in missing:  # p is never empty: empty overrides are skipped above
                cur = rest
                for k in p[:-1]:
                    cur = cur.setdefault(k, {})
                cur[p[-1]] = v
        out.append(
            "\n# --- Set in your config but not in the sample (not applied above) ---\n"
        )
        out.extend(
            "# " + ln + "\n"
            for ln in yaml.safe_dump(rest, sort_keys=False).splitlines()
        )
        print(
            "Not in sample: " + ", ".join(".".join(p) for p in missing), file=sys.stderr
        )
    # The merged config holds secrets: write it to a group-readable (0640) temporary file in
    # the output directory, then replace the output atomically, so a failure
    # leaves any existing output untouched and no partial file behind.
    # Resolve a symlinked output so the link's target is updated, not the link.
    dest = os.path.realpath(a.output)
    out_dir = os.path.dirname(dest)
    fd, tmp_path = tempfile.mkstemp(dir=out_dir, prefix=".merge_yaml.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.writelines(out)
        try:
            os.chmod(tmp_path, 0o640)
        except OSError:
            pass  # best effort on platforms without POSIX permissions
        os.replace(tmp_path, dest)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    print(
        f"Wrote {a.output}: {len(used)} of your values applied, "
        f"{len(missing)} not in sample"
    )


if __name__ == "__main__":
    main()

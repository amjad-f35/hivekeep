"""Checks the sweperf task catalog and previews the templates it yields.

workloads/deploy.sh renders one ActorTemplate per sweperf task at deploy
time, from tasks.tsv (task, step count, optional image) and
sweperf-template.yaml.tmpl. This tool applies the same rules ahead of a
deploy, so a bad catalog line or template fails here and not mid-deploy:

  sweperf_catalog.py --check                      # validate catalog and template
  sweperf_catalog.py --check --require-pinned     # also demand digest-pinned images
  sweperf_catalog.py 'django*' --image-repo R     # print the rendered templates
  sweperf_catalog.py '*' --image-repo R -o DIR    # write them to DIR

A task without an image column uses <image repo>/sweperf:<task>, as deploy.sh
does with SWEPERF_IMAGE_REPO. Trajectory images are rebuilt in place under
one tag, so production catalogs should pin each image by digest
(repo@sha256:...); --require-pinned enforces that.

Standard library only.
"""

from __future__ import annotations

import argparse
import collections
import fnmatch
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
CATALOG = HERE / "tasks.tsv"
TEMPLATE = HERE / "sweperf-template.yaml.tmpl"

# The placeholders deploy.sh fills from a catalog line; every other ${...} in
# the template (e.g. SNAPSHOT_BASE_URI) is a deploy-wide value left as is.
TASK_PLACEHOLDERS = ("SWEPERF_TASK", "SWEPERF_CONTAINER", "SWEPERF_STEPS", "SWEPERF_IMAGE")

# <repository>-<instance>, e.g. astropy-7336; it becomes part of a template
# name and its first part the container name, both DNS labels.
TASK_NAME = re.compile(r"[a-z0-9]+(-[a-z0-9]+)+")
PLACEHOLDER = re.compile(r"\$\{([A-Z0-9_]+)\}")


def load_catalog(path: pathlib.Path, require_pinned: bool = False) -> list[dict]:
    """Parses a tasks.tsv: blank lines and lines starting with # are skipped;
    each other line is task, steps and an optional image, tab-separated.
    Raises ValueError naming the line for anything deploy.sh or the load
    class could not use."""
    tasks = []
    for lineno, raw in enumerate(path.read_text().splitlines(), 1):
        if not raw.strip() or raw.startswith("#"):
            continue
        cols = raw.split("\t")
        where = f"{path}:{lineno}"
        if len(cols) < 2 or len(cols) > 3:
            raise ValueError(f"{where}: want task<TAB>steps[<TAB>image], got {raw!r}")
        name, steps = cols[0], cols[1]
        image = cols[2] if len(cols) == 3 and cols[2] else None
        if not TASK_NAME.fullmatch(name):
            raise ValueError(f"{where}: task {name!r} must look like <repo>-<instance>, e.g. astropy-7336")
        if not steps.isdigit() or int(steps) <= 0:
            raise ValueError(f"{where}: {name}: steps must be a positive integer, got {steps!r}")
        if require_pinned and (image is None or "@sha256:" not in image):
            raise ValueError(f"{where}: {name}: image must be pinned by digest (repo@sha256:...)")
        tasks.append({"name": name, "steps": int(steps), "image": image})
    # deploy.sh takes the first matching line, so a duplicate would be
    # silently ignored.
    counts = collections.Counter(t["name"] for t in tasks)
    duplicates = sorted(n for n, c in counts.items() if c > 1)
    if duplicates:
        raise ValueError(f"{path}: duplicate task names: {duplicates}")
    return tasks


def check_template(text: str) -> list[str]:
    """How a template differs from what deploy.sh fills: task placeholders
    it lacks, and SWEPERF_* placeholders deploy.sh would leave unfilled."""
    found = set(PLACEHOLDER.findall(text))
    problems = [f"missing placeholder: ${{{p}}}" for p in TASK_PLACEHOLDERS if p not in found]
    problems += [f"unknown placeholder: ${{{p}}}" for p in sorted(found)
                 if p.startswith("SWEPERF_") and p not in TASK_PLACEHOLDERS]
    return problems


def image_for(task: dict, image_repo: str | None) -> str:
    if task["image"]:
        return task["image"]
    if not image_repo:
        raise ValueError(f"sweperf-{task['name']} has no image and no image repository was given")
    return f"{image_repo.rstrip('/')}/sweperf:{task['name']}"


def render(tasks: list[dict], template: str, image_repo: str | None = None) -> dict[str, str]:
    """The ActorTemplate of each task, by file name, filled as deploy.sh
    fills it."""
    out = {}
    for t in tasks:
        values = {
            "SWEPERF_TASK": t["name"],
            # astropy-7336 -> astropy
            "SWEPERF_CONTAINER": t["name"].split("-")[0],
            "SWEPERF_STEPS": str(t["steps"]),
            "SWEPERF_IMAGE": image_for(t, image_repo),
        }
        text = template
        for k, v in values.items():
            text = text.replace("${" + k + "}", v)
        out[f"sweperf-{t['name']}-template.yaml"] = text
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("task", nargs="?", help="task name or glob to render, such as '*'")
    parser.add_argument("--catalog", type=pathlib.Path, default=CATALOG)
    parser.add_argument("--template", type=pathlib.Path, default=TEMPLATE)
    parser.add_argument("--image-repo", help="registry path for tasks without an image (deploy.sh's SWEPERF_IMAGE_REPO)")
    parser.add_argument("--require-pinned", action="store_true", help="fail on images not pinned by digest")
    parser.add_argument("--check", action="store_true", help="validate the catalog and the template, render nothing")
    parser.add_argument("-o", "--out", type=pathlib.Path, help="write the rendered templates here instead of printing")
    args = parser.parse_args()

    try:
        tasks = load_catalog(args.catalog, args.require_pinned)
    except ValueError as e:
        print(e, file=sys.stderr)
        return 1
    template = args.template.read_text()
    problems = check_template(template)
    for p in problems:
        print(f"{args.template}: {p}", file=sys.stderr)
    if problems:
        return 1
    if args.check:
        print(f"{len(tasks)} task(s) ok")
        return 0

    if not args.task:
        parser.error("give a task name or glob, or --check")
    selected = [t for t in tasks if fnmatch.fnmatch(t["name"], args.task)]
    if not selected:
        print(f"no task matches {args.task!r}; known: {', '.join(t['name'] for t in tasks)}", file=sys.stderr)
        return 1
    try:
        rendered = render(selected, template, args.image_repo)
    except ValueError as e:
        print(e, file=sys.stderr)
        return 1
    for fname, content in rendered.items():
        if args.out:
            args.out.mkdir(parents=True, exist_ok=True)
            (args.out / fname).write_text(content)
            print(args.out / fname)
        else:
            print(f"--- {fname}\n{content}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

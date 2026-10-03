"""Validate repository materials without executing research programs or experiments."""
from __future__ import annotations

import ast
import json
import os
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    readme_name = os.environ.get('README_ENTRY', 'README.md')
    minimum = int(os.environ.get('MIN_EXPECTED_PYTHON_SOURCES', '1'))
    errors = []
    readme = root / readme_name
    if not readme.is_file() or not readme.read_text(encoding='utf-8-sig').strip():
        errors.append('Repository usage documentation is missing or empty.')
    checked = 0
    ignored = {'.git', '__pycache__', '.pytest_cache', '.venv', 'venv', 'node_modules'}
    for path in sorted(root.rglob('*.py')):
        if any(part in ignored for part in path.relative_to(root).parts):
            continue
        if path.is_symlink() and not path.resolve().is_relative_to(root):
            errors.append('Source symlink leaves the repository: ' + path.relative_to(root).as_posix())
            continue
        try:
            ast.parse(path.read_bytes(), filename=path.relative_to(root).as_posix())
            checked += 1
        except (SyntaxError, UnicodeError, OSError) as exc:
            errors.append(path.relative_to(root).as_posix() + ': ' + str(exc))
    # Count research sources, not this validator, as the material-presence check.
    research_sources = checked - 1
    if research_sources < minimum:
        errors.append(f'Expected at least {minimum} supplied Python sources, found {research_sources}.')
    for path in (root / 'pyproject.toml', root / 'package.json'):
        if path.is_file():
            try:
                if path.suffix == '.json':
                    json.loads(path.read_text(encoding='utf-8-sig'))
                else:
                    import tomllib
                    tomllib.loads(path.read_text(encoding='utf-8-sig'))
            except (ValueError, UnicodeError, OSError) as exc:
                errors.append(path.name + ': ' + str(exc))
    print(json.dumps({'check': 'repository-material-integrity', 'python_sources_parsed': checked,
                      'research_sources_present': research_sources, 'minimum_supplied_sources': minimum,
                      'usage_document_present': readme.is_file(), 'errors': errors,
                      'scope': 'Documentation and source/configuration integrity only. Research programs, proofs and experiments are not executed.'}, indent=2))
    return 1 if errors else 0


if __name__ == '__main__':
    raise SystemExit(main())

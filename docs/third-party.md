# Third-party source notices

The experiment reads 107 retained Python source files from eight upstream projects. It parses them as text and never imports or executes them. Extraction and event generation are wrappers; upstream internals are not modified. The exact consumed source subset is retained, with upstream notices in each `inputs/sources/<project>/notices/` directory. No third-party paper PDF is redistributed.

| Source | Applicable retained license | Notice |
|---|---|---|
| attrs | MIT | attrs/notices/LICENSE |
| cachetools | MIT | cachetools/notices/LICENSE |
| click | BSD 3-clause | click/notices/LICENSE.txt |
| itsdangerous | BSD 3-clause | itsdangerous/notices/LICENSE.txt |
| jinja2 | BSD 3-clause | jinja2/notices/LICENSE.txt |
| markupsafe | BSD 3-clause | markupsafe/notices/LICENSE.txt |
| packaging | BSD 2-clause or Apache 2.0; redistribution here relies on the BSD option | packaging/notices/LICENSE, LICENSE.BSD, LICENSE.APACHE |
| toolz | BSD 3-clause | toolz/notices/LICENSE.txt |

Paths above are relative to `inputs/sources/`. The inventory gives the installed distribution release strings and ordinary repository-origin URLs; it does not assert an independently verified checkout or release-byte match. The package selection is a convenience sample, not a market or popularity sample. Upstream author and copyright names are preserved as required; they are source attribution, not authorship or endorsement of this project. Retained source bytes and notices, rather than downloads from a changing default branch, are the reproduction inputs.

## Held-out public commit history

The supplemental validation also retains metadata and function-body hunks for six MIT-licensed cachetools commits after the exact v7.1.4 base already included above. `docs/real-history-provenance.md` records the selection rule, commit identifiers, stage digests, query scope, and exclusions. The artifact does not redistribute third-party paper PDFs or claim that the projected module is a complete repository checkout.

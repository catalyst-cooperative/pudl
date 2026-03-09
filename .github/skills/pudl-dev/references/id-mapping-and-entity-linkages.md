# ID Mapping and Entity Linkages

## Use this when

- Debugging or extending cross-dataset entity matching.
- Interpreting historical FERC/EIA utility and plant linkage behavior.
- Explaining why many-to-many entity mappings occur in legacy workflows.

## Context

- PUDL links entities across heterogeneous reporting systems and names.
- Utility and plant mappings are often association-based, not strict one-to-one joins.
- Ambiguity is common when utilities reorganize, merge, or report inconsistently.

## Practical guidance

- Treat mapping outputs as probabilistic/curated associations where appropriate.
- Validate edge cases with multiple independent signals (name, geography, related assets).
- Document assumptions and caveats when changing linkage logic.

## Canonical sources

- `docs/dev/pudl_id_mapping.rst`
- [PUDL ID Mapping Methodology](../../../../docs/pudl/id_mapping/id_mapping.md)
- `docs/methodology/index.rst`
- `src/pudl/analysis/` (where applicable)

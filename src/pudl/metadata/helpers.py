"""Functions for manipulating metadata constants."""
from typing import Dict, List, Tuple

from .constants import FOREIGN_KEY_RULES
from .resources import RESOURCES


def _follow_key(
    tree: Dict[str, Dict[tuple, Tuple[str, tuple]]],
    name: str,
    fields: tuple
) -> List[Tuple[tuple, str, tuple]]:
    """Traverse forein key tree."""
    # (local fields, reference name, reference fields)
    visited = []
    if name not in tree or fields not in tree[name]:
        return visited
    ref_name, ref_fields = tree[name][fields]
    visited.append([fields, ref_name, ref_fields])
    if ref_name not in tree:
        return visited
    for next_fields in tree[ref_name]:
        if set(next_fields) <= set(ref_fields):
            for xfields, xref, xref_fields in _follow_key(tree, ref_name, next_fields):
                ofields = tuple(fields[ref_fields.index(field)]
                                for field in xfields)
                visited.append([ofields, xref, xref_fields])
    return visited


def build_foreign_keys() -> Dict[str, List[str]]:
    """Build foreign key descriptors for each resource."""
    # Build foreign key tree
    # [local_name][local_fields] => (reference_name, reference_fields)
    tree = {}
    for name in RESOURCES:
        # NOTE: Assumes rules are { (): [resource, ?(fields)] }
        for fields, ref in FOREIGN_KEY_RULES.items():
            ref_name = ref[0]
            # Assume local and reference fields are the same by default
            ref_fields = ref[1] if len(ref) > 1 else fields
            # Check that reference fields are the reference's primary key
            if 'primaryKey' not in RESOURCES[ref_name]['schema']:
                raise ValueError(
                    f"Foreign key reference '{ref_name}' does not have a primary key"
                )
            primary_key = RESOURCES[ref_name]['schema']['primaryKey']
            if set(primary_key) != set(ref_fields):
                raise ValueError(
                    f"Foreign key reference fields {ref_fields}"
                    f" do not match reference '{ref_name}' primary key {primary_key}"
                )
            # Include key if reference is not self and all local fields are present
            # NOTE: Assumes resource fields is a list of names
            if (
                name != ref_name and
                set(fields) <= set(RESOURCES[ref_name]['schema']['fields'])
            ):
                if name not in tree:
                    tree[name] = {}
                tree[name][fields] = ref_name, ref_fields
    # Build foreign key descriptors
    foreign_keys = {}
    for name in tree:
        firsts = []
        followed = []
        for fields in tree[name]:
            path = _follow_key(tree, name, fields)
            firsts.append(path[0])
            followed.extend(path[1:])
        # Keep key if not on path of other key
        kept = [key for key in firsts if key not in followed]
        foreign_keys[name] = [
            {
                'fields': list(key[0]),
                'reference': {'resource': key[1], 'fields': list(key[2])}
            } for key in kept
        ]
    return foreign_keys

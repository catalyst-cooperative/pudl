"""Functions for manipulating metadata constants."""
from collections import defaultdict
from typing import Dict, List, Tuple, Union


def _parse_field_names(fields: List[Union[str, dict]]) -> List[str]:
    """
    Parse field names.

    Args:
        fields: Either field names or field descriptors with a `name` key.

    Returns:
        Field names.
    """
    return [field if isinstance(field, str) else field["name"] for field in fields]


def _parse_foreign_key_rules(meta: dict, name: str) -> List[dict]:
    """
    Parse foreign key rules from resource descriptor.

    Args:
        meta: Resource descriptor.
        name: Resource name.

    Returns:
        Foreign key rules:
        * `fields` (List[str]): Local fields.
        * `reference['name']` (str): Reference resource name.
        * `reference['fields']` (List[str]): Reference primary key fields.
        * `exclude` (List[str]): Names of resources to exclude, including `name`.
    """
    rules = []
    if "foreignKeyRules" in meta["schema"]:
        for fields in meta["schema"]["foreignKeyRules"]["fields"]:
            exclude = meta["schema"]["foreignKeyRules"].get("exclude", [])
            rules.append({
                "fields": fields,
                "reference": {"name": name, "fields": meta["schema"]["primaryKey"]},
                "exclude": [name] + exclude
            })
    return rules


def _build_foreign_key_tree(
    resources: Dict[str, dict]
) -> Dict[str, Dict[Tuple[str], dict]]:
    """
    Build foreign key tree.

    Args:
        resources: Resource descriptors by name.

    Returns:
        Foreign key tree where the first key is a resource name (str),
        the second key is resource field names (Tuple[str]),
        and the value describes the reference resource (dict):
        * `reference['name']` (str): Reference name.
        * `reference['fields']` (List[str]): Reference field names.
    """
    # Parse foreign key rules
    # [{fields: [], reference: {name: '', fields: []}, exclude: []}, ...]
    rules = []
    for name, meta in resources.items():
        rules.extend(_parse_foreign_key_rules(meta, name=name))
    # Build foreign key tree
    # [local_name][local_fields] => (reference_name, reference_fields)
    tree = defaultdict(dict)
    for name, meta in resources.items():
        fields = _parse_field_names(meta["schema"]["fields"])
        for rule in rules:
            local_fields = rule["fields"]
            if name not in rule["exclude"] and set(local_fields) <= set(fields):
                tree[name][tuple(local_fields)] = rule["reference"]
    return dict(tree)


def _traverse_foreign_key_tree(
    tree: Dict[str, Dict[Tuple[str], dict]],
    name: str,
    fields: Tuple[str]
) -> List[Tuple[tuple, str, tuple]]:
    """
    Traverse foreign key tree.

    Args:
        tree: Foreign key tree (see :func:`_build_foreign_key_tree`).
        name: Local resource name.
        fields: Local resource fields.

    Returns:
        Sequence of foreign keys starting from `name` and `fields`:
        * `fields` (List[str]): Local fields.
        * `reference['name']` (str): Reference resource name.
        * `reference['fields']` (List[str]): Reference primary key fields.
    """
    keys = []
    if name not in tree or fields not in tree[name]:
        return keys
    ref = tree[name][fields]
    keys.append({"fields": list(fields), "reference": ref})
    if ref["name"] not in tree:
        return keys
    for next_fields in tree[ref["name"]]:
        if set(next_fields) <= set(ref["fields"]):
            for key in _traverse_foreign_key_tree(tree, ref["name"], next_fields):
                mapped_fields = [
                    fields[ref["fields"].index(field)] for field in key["fields"]
                ]
                keys.append({"fields": mapped_fields, "reference": key["reference"]})
    return keys


def build_foreign_keys(
    resources: Dict[str, dict], prune: bool = True
) -> Dict[str, List[dict]]:
    """
    Build foreign keys for each resource.

    A resource's `foreignKeyRules` (if present)
    determines which other resources will be assigned a foreign key (`foreignKeys`)
    to the reference's primary key.
    * `fields` (List[List[str]]): Sets of field names for which to create a foreign key.
      These are assumed to match the order of the reference's primary key fields.
    * `exclude` (Optional[List[str]]): Names of resources to exclude.

    Args:
        resources: Resource descriptors by name.
        prune: Whether to prune redundant foreign keys.

    Returns:
        Foreign keys for each resource (if any), by resource name.
        * `fields` (List[str]): Field names.
        * `reference['name']` (str): Reference resource name.
        * `reference['fields']` (List[str]): Reference resource field names.

    Examples:
        >>> resources = {
        ...     'x': {
        ...         'schema': {
        ...             'fields': ['z'],
        ...             'primaryKey': ['z'],
        ...             'foreignKeyRules': {'fields': [['z']]}
        ...         }
        ...     },
        ...     'y': {
        ...         'schema': {
        ...             'fields': ['z', 'yy'],
        ...             'primaryKey': ['z', 'yy'],
        ...             'foreignKeyRules': {'fields': [['z', 'zz']]}
        ...         }
        ...     },
        ...     'z': {'schema': {'fields': ['z', 'zz']}}
        ... }
        >>> keys = build_foreign_keys(resources)
        >>> keys['z']
        [{'fields': ['z', 'zz'], 'reference': {'name': 'y', 'fields': ['z', 'yy']}}]
        >>> keys['y']
        [{'fields': ['z'], 'reference': {'name': 'x', 'fields': ['z']}}]
        >>> keys = build_foreign_keys(resources, prune=False)
        >>> keys['z'][0]
        {'fields': ['z'], 'reference': {'name': 'x', 'fields': ['z']}}
    """
    tree = _build_foreign_key_tree(resources)
    keys = {}
    for name in tree:
        firsts = []
        followed = []
        for fields in tree[name]:
            path = _traverse_foreign_key_tree(tree, name, fields)
            firsts.append(path[0])
            followed.extend(path[1:])
        keys[name] = firsts
        if prune:
            # Keep key if not on path of other key
            keys[name] = [key for key in keys[name] if key not in followed]
    return keys

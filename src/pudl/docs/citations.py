"""Generate the Citations & Media pages from the project's BibTeX files."""

import jinja2
from pybtex.database import parse_file as parse_bibtex_file
from pybtex.plugin import register_plugin
from pybtex.style.formatting.plain import Style as PlainStyle
from pybtex.style.sorting import BaseSortingStyle

from pudl import PUDL_DOCS_PATH

# Handle bibtex formatting to produce a numbered list
# without labels and sorted by descending date in document
# we can't use any default style because there are multiple bibs on one page


class YearDescendingSortingStyle(BaseSortingStyle):
    """Create style that sorts by descending year."""

    def sorting_key(self, entry):
        """Return sorting key that descends by year."""
        year_str = entry.fields.get("year", "0")
        try:
            year = int(year_str)
        except ValueError:
            year = 0

        author = entry.persons.get("author", [])
        author_key = str(author[0]) if author else ""
        title = entry.fields.get("title", "")

        return (-year, author_key, title)


class NoLabelStyle(PlainStyle):
    """Create citation style without label and sorting on descending year."""

    default_sorting_style = "year_desc"

    def format_label(self, entry):
        """Override default label."""
        return ""


register_plugin(
    "pybtex.style.sorting",
    "year_desc",
    YearDescendingSortingStyle,
)

register_plugin(
    "pybtex.style.formatting",
    "nolabel",
    NoLabelStyle,
)


# One generated page per .bib file. `description` is plain RST and may contain
# markup (e.g. hyperlinks); it's inserted into the page without escaping (see
# autoescape=False below), so keep it trusted, hand-written text, never user-
# or data-derived content.
# When adding a new page here, ALSO add its output path to the docs-clean pixi
# task in pyproject.toml so the generated file gets removed.
CITATIONS_MEDIA_PAGES = [
    {
        "name": "catalyst_publications",
        "title": "Catalyst Publications",
        "bibfile": "catalyst_pubs.bib",
        "description": (
            "Data, software, and analyses that we have published for public use. "
            "We self-archive all of our publications and the input data for PUDL "
            "in the `Catalyst Cooperative Zenodo Community "
            "<https://zenodo.org/communities/catalyst-cooperative/>`__."
        ),
    },
    {
        "name": "citing_pudl",
        "title": "Work Citing PUDL and other Catalyst Analyses",
        "bibfile": "catalyst_cites.bib",
        "description": (
            "Academic, policy, and industry publications that reference PUDL and "
            "analyses done by Catalyst Cooperative."
        ),
    },
    {
        "name": "citing_cooperative",
        "title": "Work Citing Catalyst Cooperative",
        "bibfile": "cooperative_cites.bib",
        "description": (
            "Academic, policy, and industry publications referencing Catalyst's "
            "role as a worker-owned software cooperative."
        ),
    },
    {
        "name": "further_reading",
        "title": "Further Reading",
        "bibfile": "further_reading.bib",
        "description": "Other research and publications relevant to the work we do.",
    },
]

# Human-readable labels for known BibTeX entry kinds, used as a section
# heading when an entry has no explicit `type` field. Any entry kind found in
# a .bib file that isn't listed here still gets its own section (titlecased
# from the raw name) -- so a newly introduced reference kind is still
# rendered somewhere instead of silently vanishing because it didn't match a
# hardcoded filter. Sections are displayed alphabetically by heading (see
# citations_media_to_rst), with "misc" always sorted last, so this dict's
# order doesn't otherwise matter.
CITATION_TYPE_LABELS = {
    "phdthesis": "PhD Thesis",
    "mastersthesis": "Master's Thesis",
    "book": "Book",
    "article": "Journal or News Article",
    "inproceedings": "Conference Paper",
    "techreport": "Report",
    "misc": "Other",
}


def _bibtex_entry_heading(entry) -> str:
    """Pick a section heading for a pybtex entry.

    BibTeX exports (e.g. from Zotero) often carry a free-text ``type`` field
    that elaborates on the entry's kind -- e.g. an ``@techreport`` with
    ``type = {Working Paper}``, or an ``@misc`` with
    ``type = {{SSRN} {Scholarly} {Paper}}`` (braces protect capitalization).
    When that field is present, it makes a more specific and useful heading
    than the entry's bare BibTeX kind (``@techreport``, ``@misc``, ...), so
    prefer it, stripped of the protective braces. Entries without a ``type``
    field fall back to a label for their BibTeX kind.
    """
    type_field = entry.fields.get("type", "").replace("{", "").replace("}", "").strip()
    if type_field:
        return type_field
    kind = entry.type.lower()
    return CITATION_TYPE_LABELS.get(kind, kind.replace("_", " ").title())


def citations_media_to_rst(app):
    """Generate the Citations & Media pages, grouped by citation heading.

    Each page gets one section per heading (see `_bibtex_entry_heading`) that
    is actually in use by its .bib file -- never an empty section for a
    heading with no (or no longer any) entries, and never an entry silently
    dropped for having a kind or ``type`` field we hadn't seen before. Entries
    that land on the same heading -- whether because they share a BibTeX kind
    or happen to share an explicit ``type`` field -- are grouped into a single
    section rather than repeating the heading.
    """
    print("Generating Citations & Media pages from bibliography files.")
    # autoescape=False: this template produces RST, not HTML, so escaping
    # would corrupt both the hand-written hyperlink markup in `description`
    # and any heading containing an apostrophe (e.g. "Master's Theses").
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(PUDL_DOCS_PATH / "templates"),
        autoescape=False,  # noqa: S701
    )
    template = env.get_template("citations_media_page.rst.jinja")
    for page in CITATIONS_MEDIA_PAGES:
        bibdata = parse_bibtex_file(str(PUDL_DOCS_PATH / page["bibfile"]))
        groups: dict[str, list[str]] = {}
        for key, entry in bibdata.entries.items():
            heading = _bibtex_entry_heading(entry)
            groups.setdefault(heading, []).append(key)

        misc_label = CITATION_TYPE_LABELS["misc"]
        ordered_headings = sorted(h for h in groups if h != misc_label)
        if misc_label in groups:
            ordered_headings.append(misc_label)

        sections = [
            {
                "heading": heading,
                # Underline must be computed per-heading (rather than a
                # fixed-width constant) since headings can be derived from
                # arbitrary, unbounded ``type`` field text.
                "heading_underline": "-" * len(heading),
                # Named `citation_keys`, not `keys`: a plain dict's built-in
                # `.keys` method would otherwise shadow this entry when Jinja
                # resolves `section.keys` via attribute access.
                "citation_keys": sorted(groups[heading]),
            }
            for heading in ordered_headings
        ]
        rendered = template.render(
            title=page["title"],
            description=page["description"],
            bibfile=page["bibfile"],
            sections=sections,
        )
        out_path = PUDL_DOCS_PATH / f"citations_media/{page['name']}.rst"
        out_path.write_text(rendered)

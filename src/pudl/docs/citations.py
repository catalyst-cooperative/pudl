"""Generate the Citations & Media pages from the project's BibTeX files."""

from pathlib import Path

from pybtex.database import parse_file as parse_bibtex_file
from pybtex.plugin import register_plugin
from pybtex.style.formatting.plain import Style as PlainStyle
from pybtex.style.sorting import BaseSortingStyle

import pudl.logging_helpers
from pudl import PUDL_DOCS_PATH
from pudl.docs.templates import get_environment

logger = pudl.logging_helpers.get_logger(__name__)


# Handle bibtex formatting to produce a numbered list
# without labels and sorted by descending date in document
# we can't use any default style because there are multiple bibs on one page
class YearDescendingSortingStyle(BaseSortingStyle):
    """Sort bibliography entries by descending year.

    Registered as the ``year_desc`` pybtex sorting style. Sphinx's bibliography
    directive uses it, through :class:`NoLabelStyle`, to list the newest
    publications first. See :meth:`sorting_key` for how ties and undated entries
    are handled.
    """

    def sorting_key(self, entry):
        """Return a sorting key that orders entries by descending year.

        Entries are ordered newest first. Ties on year are broken by the first
        author's name and then by title, both ascending. An entry whose ``year``
        field is missing or isn't an integer (e.g. ``"in press"``) is treated as
        year 0, so it sorts after every dated entry.

        Args:
            entry: The pybtex entry to compute a key for.

        Returns:
            A ``(-year, first_author, title)`` tuple. Comparing these tuples
            yields the descending-year order.
        """
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
    """Format citations like pybtex's plain style, but without labels.

    Registered as the ``nolabel`` pybtex formatting style, and used as the default
    bibtex style for the docs. The Citations & Media pages show each bibliography
    as an enumerated list that supplies its own numbering, and combine several
    .bib files on one page, so none of the built-in styles fit. Entries are sorted
    with :class:`YearDescendingSortingStyle`, newest first.
    """

    default_sorting_style = "year_desc"

    def format_label(self, entry):
        """Return an empty label for every entry.

        The Citations & Media pages render each bibliography as an enumerated
        list, which supplies its own numbering, so the style's default labels
        (e.g. ``[Smi20]``) would be redundant.

        Args:
            entry: The pybtex entry being formatted. Unused.

        Returns:
            The empty string.
        """
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

BIBTEX_DEFAULT_STYLE = "nolabel"

# One generated page per .bib file. `description` is plain RST and may contain
# markup (e.g. hyperlinks); it's inserted into the page without escaping (see
# autoescape=False below), so keep it trusted, hand-written text, never user-
# or data-derived content. Generated pages are removed by
# :func:`pudl.docs.build.remove_generated_files`.
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

BIBTEX_FILES = [page["bibfile"] for page in CITATIONS_MEDIA_PAGES]

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


def citations_media_page_paths(docs_dir: Path = PUDL_DOCS_PATH) -> list[Path]:
    """Return the paths of all generated Citations & Media pages.

    There is one page per entry in :data:`CITATIONS_MEDIA_PAGES`, in the same
    order. This is the single source of truth for where those pages are written
    and removed, so :func:`citations_media_to_rst` and the cleanup in
    :mod:`pudl.docs.build` can't disagree about it.

    Args:
        docs_dir: The documentation source directory.

    Returns:
        Paths of the form ``<docs_dir>/citations_media/<page name>.rst``. The
        files need not exist yet.
    """
    return [
        docs_dir / "citations_media" / f"{page['name']}.rst"
        for page in CITATIONS_MEDIA_PAGES
    ]


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

    Args:
        entry: A pybtex ``Entry``. Only its ``type`` attribute (the BibTeX kind)
            and ``type`` field are used.

    Returns:
        The section heading for the entry. This is the cleaned ``type`` field if
        present, else the label in :data:`CITATION_TYPE_LABELS` for the entry's
        kind, else the kind itself with underscores replaced by spaces and
        title-cased.
    """
    type_field = entry.fields.get("type", "").replace("{", "").replace("}", "").strip()
    if type_field:
        return type_field
    kind = entry.type.lower()
    return CITATION_TYPE_LABELS.get(kind, kind.replace("_", " ").title())


def citations_media_to_rst(docs_dir: Path = PUDL_DOCS_PATH) -> None:
    """Generate the Citations & Media pages, grouped by citation heading.

    Each page gets one section per heading (see `_bibtex_entry_heading`) that
    is actually in use by its .bib file -- never an empty section for a
    heading with no (or no longer any) entries, and never an entry silently
    dropped for having a kind or ``type`` field we hadn't seen before. Entries
    that land on the same heading -- whether because they share a BibTeX kind
    or happen to share an explicit ``type`` field -- are grouped into a single
    section rather than repeating the heading.

    Sections are sorted alphabetically by heading, except that the "Other"
    section for ``@misc`` entries always comes last. The generated pages don't
    contain the citations themselves. They contain ``bibliography`` directives
    listing citation keys, which ``sphinxcontrib.bibtex`` expands at build time
    using the style registered in this module. Existing pages are overwritten.

    Args:
        docs_dir: The documentation source directory. It must contain every .bib
            file named in :data:`CITATIONS_MEDIA_PAGES`, the
            ``citations_media_page.rst.jinja`` template under ``templates/``, and
            an existing ``citations_media/`` output directory.

    Raises:
        FileNotFoundError: If a .bib file or the ``citations_media`` output
            directory is missing.
        jinja2.TemplateNotFound: If the page template is missing.
        pybtex.database.PybtexError: If a .bib file can't be parsed.
    """
    logger.info("Generating Citations & Media pages from bibliography files.")
    # autoescape=False: this template produces RST, not HTML, so escaping
    # would corrupt both the hand-written hyperlink markup in `description`
    # and any heading containing an apostrophe (e.g. "Master's Theses").
    env = get_environment(docs_dir / "templates", autoescape=False)
    template = env.get_template("citations_media_page.rst.jinja")
    for page, out_path in zip(
        CITATIONS_MEDIA_PAGES, citations_media_page_paths(docs_dir), strict=True
    ):
        bibdata = parse_bibtex_file(str(docs_dir / page["bibfile"]))
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
        out_path.write_text(rendered)

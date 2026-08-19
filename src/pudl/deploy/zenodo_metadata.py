"""Gather Zenodo deposition metadata for a PUDL data release from repo sources.

Every monthly PUDL data release requires updating the Zenodo deposition's creators,
keywords, version, and description. This module assembles that metadata
programmatically from files already checked into the repo, instead of hand-editing it
in the Zenodo web UI:

* Creators and keywords come from ``.zenodo.json`` (the same file GitHub's own Zenodo
  integration uses to archive the repository itself), so there's a single canonical
  place to update either list.
* The description body is extracted from the built Sphinx docs
  (``docs/_build/html/release_notes.html``) rather than re-implemented with a
  standalone RST-to-HTML converter, since the release notes use Sphinx-specific roles
  (``:pr:``, ``:issue:``, ``:user:``, ``:doc:``, autoapi cross-references) that only
  Sphinx itself can resolve correctly.
"""

import json
import subprocess
from pathlib import Path

import yaml
from bs4 import BeautifulSoup

from pudl.logging_helpers import get_logger
from pudl.metadata.constants import LICENSES
from pudl.metadata.sources import SOURCES

logger = get_logger(__name__)

DOCS_BASE_URL = "https://docs.catalyst.coop/pudl/en"
GITHUB_REPO_URL = "https://github.com/catalyst-cooperative/pudl"
AWS_OPEN_DATA_REGISTRY_URL = "https://registry.opendata.aws/catalyst-cooperative-pudl/"
S3_BUCKET_URL = "s3://pudl.catalyst.coop"
GCS_BUCKET_URL = "gs://pudl.catalyst.coop"

CONTACT_US_HTML = """\
<h1><strong>Contact Us</strong></h1>
<p><strong>If you're using PUDL, we would love to hear from you!</strong> Even if \
it's just a note to let us know that you exist, and how you're using the software or \
data. Here's a bunch of different ways to get in touch:</p>
<ul>
<li><a href="https://github.com/catalyst-cooperative">Follow us on GitHub</a></li>
<li>Use the <a href="https://github.com/catalyst-cooperative/pudl/issues">PUDL \
Github issue tracker</a> to let us know about any bugs or data issues you \
encounter</li>
<li><a href="https://github.com/orgs/catalyst-cooperative/discussions">GitHub \
Discussions</a> is where we provide user support.</li>
<li>Watch our <a href="https://github.com/orgs/catalyst-cooperative/projects/9">\
GitHub Project</a> to see what we're working on.</li>
<li>Email us at <a href="mailto:hello@catalyst.coop">hello@catalyst.coop</a> for \
private communications.</li>
<li>On Mastodon: <a href="https://mastodon.energy/@catalystcoop">\
@CatalystCoop@mastodon.energy</a></li>
<li>On BlueSky: <a href="https://bsky.app/profile/catalyst.coop">\
@catalyst.coop</a></li>
<li>Connect with us <a href="https://www.linkedin.com/company/catalyst-cooperative/">\
on LinkedIn</a></li>
<li>Play with our data and notebooks \
<a href="https://www.kaggle.com/catalystcooperative">on Kaggle</a></li>
<li>Combine our data with ML models \
<a href="https://huggingface.co/catalystcooperative">on HuggingFace</a></li>
<li>Learn more about us on our website: \
<a href="https://catalyst.coop">https://catalyst.coop</a></li>
<li>Subscribe to our announcements list for \
<a href="https://catalyst.coop/updates">email updates</a>.</li>
</ul>"""


def load_zenodo_json(path: Path) -> tuple[list[dict], list[str]]:
    """Read creators and keywords out of ``.zenodo.json``.

    Args:
        path: Path to the repo's ``.zenodo.json`` file.

    Returns:
        A ``(creators, keywords)`` tuple. ``creators`` entries are already in the
        shape the Zenodo deposit API expects (``name``, ``affiliation``, ``orcid``).
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    return data["creators"], data["keywords"]


def get_data_license_id() -> str:
    """Get the Zenodo license ID for the PUDL data release.

    ``pudl.metadata.sources.SOURCES["pudl"]["license_pudl"]`` is the authoritative
    record of what license PUDL's own outputs (as opposed to the raw inputs we
    archive, which can carry their own separate licenses) are released under. This
    looks up which ``pudl.metadata.constants.LICENSES`` key that entry corresponds to,
    since that key (e.g. ``"cc-by-4.0"``) is the ID Zenodo's API expects.

    Returns:
        The Zenodo license ID, e.g. ``"cc-by-4.0"``.
    """
    license_pudl = SOURCES["pudl"]["license_pudl"]
    for license_id, license_info in LICENSES.items():
        if license_info == license_pudl:
            return license_id
    raise ValueError(
        f"SOURCES['pudl']['license_pudl'] ({license_pudl!r}) doesn't match any "
        f"entry in LICENSES -- can't determine the Zenodo license ID."
    )


def load_citation_cff_version(path: Path) -> str:
    """Read the latest released version out of ``CITATION.cff``, as a release tag.

    ``CITATION.cff``'s ``version`` field is bumped as part of cutting a release, so it
    always names the most recently *published* PUDL version (e.g. while ``main`` is
    accumulating changes for the upcoming ``v2026.9.0``, this still reads
    ``"v2026.8.0"``, the last one actually released). Used as a stand-in release tag
    for sandbox test runs, which aren't publishing a real new version and so have no
    real release tag of their own to look up release notes for.

    Args:
        path: Path to the repo's ``CITATION.cff``.

    Returns:
        The version formatted as a PUDL release tag, e.g. ``"v2026.8.0"``.
    """
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return f"v{data['version']}"


def run_git(cmd: list[str], cwd: Path | None = None) -> str:
    """Run a git command and return its stdout, logging stderr on failure.

    Shared by every git-shelling-out call in ``pudl.deploy`` (branch updates, build
    lookups, this module's tag verification), so there's one place that knows how to
    invoke git and report failures consistently.

    Args:
        cmd: The full command to run, e.g. ``["git", "rev-parse", "HEAD"]``.
        cwd: Working directory to run the command in. Defaults to the current
            process's working directory.

    Returns:
        The command's stdout, unstripped.

    Raises:
        subprocess.CalledProcessError: If the command exits non-zero.
    """
    try:
        return subprocess.run(  # noqa: S603
            cmd, cwd=cwd, check=True, capture_output=True, text=True
        ).stdout
    except subprocess.CalledProcessError as exc:
        logger.error(f"Command failed: {' '.join(cmd)}\n{exc.stderr}")
        raise


def verify_git_tag_checked_out(tag: str, repo_root: Path) -> None:
    """Raise if the working tree at ``repo_root`` isn't exactly the given git tag.

    Release notes, creators, and keywords are all read live from the working tree
    rather than from Zenodo or any other tag-pinned source, so producing metadata for
    a specific release requires actually having that release's tag checked out --
    running from a later (or earlier) commit can silently pick up different release
    notes text, a different author/keyword list, etc.

    Args:
        tag: The expected git tag, e.g. ``"v2026.8.0"``.
        repo_root: Path to the repo's working tree to check.

    Raises:
        ValueError: If ``tag`` can't be resolved, or doesn't point at the working
            tree's current ``HEAD``.
    """
    try:
        tag_commit = run_git(
            ["git", "rev-parse", f"{tag}^{{commit}}"], cwd=repo_root
        ).strip()
    except subprocess.CalledProcessError as exc:
        raise ValueError(
            f"Could not resolve git tag {tag!r} in {repo_root} -- fetch it first "
            f"(git fetch --tags) or double check the tag name."
        ) from exc

    head_commit = run_git(["git", "rev-parse", "HEAD"], cwd=repo_root).strip()

    if head_commit != tag_commit:
        raise ValueError(
            f"Working tree at {repo_root} (HEAD={head_commit[:9]}) does not match "
            f"tag {tag!r} ({tag_commit[:9]}). Release notes, creators, and keywords "
            f"are all read live from the working tree, so a mismatched checkout can "
            f"produce incorrect metadata. Check out {tag!r} first, e.g. "
            f"`git checkout {tag}`."
        )


def render_release_notes_html(docs_html_dir: Path, version_tag: str) -> str:
    """Extract one version's section out of the built release notes HTML.

    Sphinx wraps every heading in a ``<section>`` and appends a ``headerlink``
    permalink anchor to each heading; both are stripped here so the output matches
    the flat ``<h1>/<h2>/<h3>`` shape used in the Zenodo description. Sphinx numbers
    these headings starting from ``<h2>`` (the version heading), since ``<h1>`` is
    reserved for the page's own "PUDL Release Notes" title; since the description is
    its own standalone document rather than a subsection of that page, every heading
    is bumped up one level so the version heading becomes the top-level ``<h1>``.
    Relative links (to other docs pages, or same-page anchors) are rewritten to
    absolute URLs under the versioned docs site, since the description lives outside
    the docs site.

    Args:
        docs_html_dir: Path to a built Sphinx HTML output directory (i.e.
            ``docs/_build/html``), produced by ``pixi run docs-build``.
        version_tag: The release version tag, e.g. ``"v2026.8.0"``.

    Returns:
        An HTML fragment containing just that version's release notes.

    Raises:
        ValueError: If the given version's section can't be found.
    """
    release_notes_path = docs_html_dir / "release_notes.html"
    soup = BeautifulSoup(release_notes_path.read_text(encoding="utf-8"), "lxml")
    anchor_id = f"release-{version_tag.replace('.', '-')}"
    anchor = soup.find(id=anchor_id)
    if anchor is None:
        raise ValueError(
            f"Could not find a release notes section for {version_tag!r} (looked "
            f"for id={anchor_id!r}) in {release_notes_path}"
        )
    section = anchor.find_parent("section")
    if section is None:
        raise ValueError(
            f"Anchor {anchor_id!r} in {release_notes_path} isn't inside a <section>"
        )

    section = section.extract()
    anchor.decompose()
    for headerlink in section.find_all("a", class_="headerlink"):
        headerlink.decompose()

    docs_base = f"{DOCS_BASE_URL}/{version_tag}"
    for link in section.find_all("a", href=True):
        # bs4 types href as str | AttributeValueList (multi-valued attrs like
        # `class`), but `href` is always single-valued in practice.
        href = str(link["href"])
        if href.startswith(("http://", "https://", "mailto:")):
            continue
        link["href"] = (
            f"{docs_base}/release_notes.html{href}"
            if href.startswith("#")
            else f"{docs_base}/{href}"
        )

    for nested_section in section.find_all("section"):
        nested_section.unwrap()

    for level in range(2, 7):
        for heading in section.find_all(f"h{level}"):
            heading.name = f"h{level - 1}"

    return "".join(str(child) for child in section.contents).strip()


def build_related_resources(
    version_tag: str, github_archive_doi_url: str | None
) -> tuple[str, list[dict]]:
    """Build the "Other Resources" description footer and matching related_identifiers.

    The same three release-specific URLs (versioned docs, GitHub release tag, and the
    GitHub-repo Zenodo software archive) are surfaced twice: once as human-readable
    links in the description footer, and once as structured Zenodo
    ``related_identifiers`` entries so DataCite/OpenAIRE can index the relationships.

    Args:
        version_tag: The release version tag, e.g. ``"v2026.8.0"``.
        github_archive_doi_url: Resolvable DOI URL of the GitHub-repo Zenodo software
            archive for this release (e.g. ``https://doi.org/10.5281/zenodo.XXXXX``),
            or ``None`` on the sandbox server, where GitHub's Zenodo integration never
            publishes.

    Returns:
        A ``(footer_html, related_identifiers)`` tuple.
    """
    docs_url = f"{DOCS_BASE_URL}/{version_tag}/"
    data_dictionary_url = f"{docs_url}data_dictionaries/pudl_db.html"
    github_release_url = f"{GITHUB_REPO_URL}/releases/tag/{version_tag}"
    s3_path = f"{S3_BUCKET_URL}/{version_tag}/"
    gcs_path = f"{GCS_BUCKET_URL}/{version_tag}/"

    items = [
        f'<li><a href="{data_dictionary_url}">PUDL {version_tag} '
        f"Data Dictionary</a></li>",
        f'<li><a href="{docs_url}">PUDL {version_tag} Documentation</a></li>',
        f'<li><a href="{AWS_OPEN_DATA_REGISTRY_URL}">PUDL in the AWS Open Data '
        f"Registry</a></li>",
        f"<li>PUDL {version_tag} in a free, public AWS S3 bucket: {s3_path}</li>",
        f"<li>PUDL {version_tag} in a requester-pays GCS bucket: {gcs_path}</li>",
    ]
    related_identifiers = [
        {"identifier": docs_url, "relation": "isDocumentedBy"},
    ]

    if github_archive_doi_url is not None:
        items.append(
            f'<li><a href="{github_archive_doi_url}">Zenodo archive of the PUDL '
            f"GitHub repo for this release</a></li>"
        )
        related_identifiers.append(
            {
                "identifier": github_archive_doi_url,
                "relation": "isSupplementedBy",
                "resource_type": "software",
            }
        )

    items.append(
        f'<li><a href="{github_release_url}">PUDL {version_tag} release on '
        f"GitHub</a></li>"
    )
    related_identifiers.append(
        {
            "identifier": github_release_url,
            "relation": "isSupplementedBy",
            "resource_type": "software",
        }
    )

    footer_html = (
        f"<h1><strong>Other PUDL {version_tag} Resources</strong></h1>\n"
        "<ul>\n" + "\n".join(items) + "\n</ul>"
    )
    return footer_html, related_identifiers

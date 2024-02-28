"""Script for converting a .bib file to a .csv file for Sloan Reports."""
from pathlib import Path

import bibtexparser
import pandas as pd

OUTPUT_TYPE = {
    "article": "Journal Article",
    "book": "Book",
    "misc": "Miscellaneous",
    "techreport": "Technical Report",
}


def convert_bib_to_csv():
    """Convert a .bib file to a .csv file as specified by Sloan's reporting guidelines."""
    with Path("docs/catalyst_cites.bib").open() as bibtex_file:
        bib_database = bibtexparser.load(bibtex_file)
    entires = []
    for entry in bib_database.entries:
        first_author = entry.get("author").split(" and")[0]
        first_author_last_name = first_author.split(" ")[-1]
        first_author_first_name = " ".join(first_author.split(" ")[:-1])
        entires.append(
            pd.Series(
                {
                    "Output Type": OUTPUT_TYPE.get(entry.get("ENTRYTYPE"), "Unknown"),
                    "Author Last Name": first_author_last_name,
                    "Author First Name": first_author_first_name,
                    "Output Title": entry.get("title"),
                    "Output Publication": entry.get("journal"),
                    "Publication Year": entry.get("year"),
                    "Weblink or DOI": entry.get("doi"),
                }
            )
        )
    df = pd.concat(entires, axis=1).T
    df["Grant Number"] = "G-2021-14184"
    df.to_csv("devtools/catalyst_cites.csv", index=False)


if __name__ == "__main__":
    convert_bib_to_csv()

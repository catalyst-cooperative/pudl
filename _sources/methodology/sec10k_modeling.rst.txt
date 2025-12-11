SEC 10-K Ownership Data Extraction Modeling
===============================================================================

Overview
~~~~~~~~
Utilities are often part of a nested hierarchy of holding companies and their
subsidiaries which makes it difficult to understand the complex web of political
and economic incentives they are responding to. These subsidiary relationships
are reported in the SEC's Form 10-K, via an attachment called Exhibit 21, however
the format and contents of this attachment is neither standardized nor structured.
With support from The Mozilla Foundation, Catalyst built a machine learning model
to extract ownership data from this Ex. 21 attachment and structures it into a
table along with information about the 10-K filing companies. We also used
probabilistic record linkage to connect these owner and subsidiary companies
to utility companies that file with EIA. This connection in turn enables a
connection to data reported to FERC and EIA contained in PUDL. We only conducted
an initial round of modeling, so this dataset is a beta version and its
contents and connections to other datasets are probabilistic in nature.

The following output tables are created from this process:

* :ref:`out_sec10k__quarterly_filings`: contains information about the filings
  themselves
* :ref:`out_sec10k__quarterly_company_information`: contains attributes
  describing the companies which file 10-K's
* :ref:`out_sec10k__parents_and_subsidiaries`: contains ownership information
  about parent companies and their subsidiary companies
* :ref:`out_sec10k__changelog_company_name`: contains information about company
  name changes

Extracting Ownership Data From Exhibit 21 Attachments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ownership tables contained in the Exhibit 21 attachments are not consistently
formatted and lack standardization in content and layout. Extracting this
information required a machine learning model to generalize across the many
different layouts. We used `LayoutLMv3 <https://huggingface.co/microsoft/layoutlmv3-base>`__,
a pre-trained model from the Hugging Face
Transformers library designed for document layout-aware information extraction.
We fine-tuned LayoutLM for named entity recognition on 160 labeled Exhibit 21
documents, classifying each word in the documents as one of four
labels: subsidiary name, location of incorporation, ownership percentage, or other.
After classifying the word tokens, we applied a set of heuristics to organize
the data into structured tables. This rules-based model leveraged the position
of bounding box coordinates and the order of entity tags assigned by LayoutLM.
For example, one heuristic enforces that each row in the extracted table contains
only one subsidiary name, location, and ownership fraction, so if two subsidiary
names are adjacent in the extracted tokens, then they are placed on different rows
of the output table. We validated the final extracted tables against a
validation set of 72 manually transcribed tables.
The extracted data is structured into the
:ref:`core_sec10k__quarterly_exhibit_21_company_ownership`
table, and subsequently attributes about the subsidiary and parent companies are
merged on in the :ref:`out_sec10k__parents_and_subsidiaries` table.

Assigning ``subsidiary_company_id_sec10k`` to Extracted Subsidiary Companies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To track subsidiaries over time, we assign a unique identifier called
``subsidiary_company_id_sec10k`` to each subsidiary extracted from an
Exhibit 21 filing and reported in the
:ref:`core_sec10k__quarterly_exhibit_21_company_ownership` table. This identifier
is constructed from three components: the filer company’s Central Index Key (CIK),
the subsidiary company name, and the subsidiary’s location of incorporation.

As a result, the same subsidiary reported by the same filer across multiple
years will have a consistent ``subsidiary_company_id_sec10k``. However,
if the same subsidiary appears in filings from different parent companies,
it will receive a different identifier in each context.

In short, ``subsidiary_company_id_sec10k`` tracks subsidiaries consistently
within a single filer’s history but should not be used to link the same
subsidiary across different filers.

Matching Subsidiary Companies to a Central Index Key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Some subsidiary companies reported in Ex. 21 attachments also file
their own 10-K filing and thus have an implicit central index key.
This CIK is not reported in the Ex. 21 attachments, so we connected the
subsidiary companies in :ref:`core_sec10k__quarterly_exhibit_21_company_ownership`
to the filing companies in :ref:`core_sec10k__quarterly_company_information`.

To do this, we match the subsidiary companies to 10-K filers on company name.
If there are multiple matches with the same company name we choose
the pair with the most overlap in location of incorporation and then nearest
report years. This is a fairly conservative matching process, meaning that
many subsidiaries are not matched to their CIK, but there are unlikely
to be subsidiaries that are erroneously matched to a CIK. This process
produces the :ref:`core_sec10k__assn_exhibit_21_subsidiaries_and_filers` table.

In this table, 2% of unique ``subsidiary_company_id_sec10k`` are matched to
19% of the filers.

The fact that on average there are ~3.5 ``subsidiary_company_id_sec10k``
mapping to each ``central_index_key`` is due to the
``subsidiary_company_id_sec10k`` tracking the same company
across time, but not across different owners. Multiple
``subsidiary_company_id_sec10k`` are assigned to the same subsidiary under
different owners. Due to the ambiguities of ``subsidiary_company_id_sec10k``,
these percentages point to a couple interpretations:

* most subsidiary companies don't file their own 10-K filing (due to their
  relatively small size), so we have little information about subsidiary
  companies to work with in general.
* and/or a relatively high fraction of SEC 10-K filers are subsidiaries
  of other SEC 10-K filers -- 1/5 of all filers are showing up in another
  company's Ex. 21.

Matching SEC Filing Companies to EIA Utilities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The :ref:`core_sec10k__quarterly_company_information` table contains
attributes about SEC 10-K filing companies, such as
address information, that can be used to connect these companies
to the companies that report to EIA using probabilistic record linkage
We use a model built with the Python package
`Splink <https://github.com/moj-analytical-services/splink>`__
to connect the :ref:`core_sec10k__quarterly_company_information` to the
``out_eia__yearly_utilities`` table. The match between
``central_index_key`` and ``utility_id_eia`` is one-to-one and is not
allowed to change over time. In cases where there were multiple candidate
matches, the match with the highest probability is selected. This result
of this match can be found in the
:ref:`core_sec10k__assn_sec10k_filers_and_eia_utilities` table.

Matching SEC Subsidiary Companies to EIA Utilities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
After constructing the :ref:`core_sec10k__assn_sec10k_filers_and_eia_utilities`
table, we take the remaining EIA utilities which have not been matched
to an SEC filer and match them to subsidiary companies reported in Ex. 21
attachments. We don't have all the additional attributes about these
subsidiaries that we have about the filers, so we do this match
based solely on shared company names.
This matches an additional 1703 EIA utilities to Ex. 21
subsidiaries. .04% of ``subsidiary_company_id_sec10k`` are matched
to an EIA utility in this table.

Assumptions
~~~~~~~~~~~
Over the course of this process, we make several assumptions about the data:

* The filer company of an SEC 10-K filing is the parent company of the subsidiary
  companies listed in that filing's Ex. 21. Several sets of information about
  companies may be reported in the header of a 10-K filing, as the filer may
  report one 10-K with other companies under its umbrella. General Instruction
  I(2)(b) of Form 10-K seems to back up this assumption that the subsidiaries
  reported in the Ex. 21 are those of the filing company. Records
  across many tables can be traced back to a unique filename, so we can see the
  filing-level information that's associated with it in the
  :ref:`out_sec10k__quarterly_filings` table, including the CIK of the filer.
* When constructing ``core_sec10k__quarterly_company_information`` we assume
  that each block of company information in a 10-K header refers to a different
  company, and there should not be two different blocks of information about the
  same company within one 10-K filing. We have to drop 10 blocks of information
  (.002% of the data) which refer to the same company within one 10-K filing
  when creating this table.
  These duplicates have very minor differences (i.e. two blocks are exactly
  the same except they differ in their ``film_number`` attribute)
  and are interpreted as filing errors.

Future Improvements
~~~~~~~~~~~~~~~~~~~
* Due to limits with memory, record linkage between SEC 10-K filers and EIA
  utilities was initially only conducted on the most recent year of data.
  Future work will conduct the match on all years of data.
* The information extracted from Ex. 21 attachments is structured into
  tabular form using a set of heuristics. Future work will use a more
  generalized, robust model to structure this data into tabular form.
* While fine-tuning LayoutLMv3 doesn't require a large corpus of labeled
  documents for training and we got an accuracy score of 95% fine-tuning
  with 160 labeled documents, labeling more documents may improve the
  performance of the word token classification model.
* We didn't extract ownership data for the years 2018-2022 and plan to
  run the model to capture these years in the future.

"""This module contains the implementation of CompanyNameCleaner class from OS-Climate's financial-entity-cleaner package."""

import enum
import json
import logging
import re
from importlib.resources import files
from typing import Self

import pandas as pd
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

CLEANING_RULES_DICT = {
    "remove_email": [" ", r"\S*@\S*\s?"],
    "remove_url": [" ", r"https*\S+"],
    "remove_word_the_from_the_end": ["", r"\s+the$"],
    "remove_word_the_from_the_beginning": ["", r"^the\s+"],
    "remove_www_address": [" ", r"https?://[.\w]{3,}|www.[.\w]{3,}"],
    "enforce_single_space_between_words": [" ", r"\s+"],
    "replace_ampersand_in_spaces_by_AND": [" and ", r"\s+&\s+"],
    "replace_ampersand_by_AND": [" and ", r"\s*&\s*"],
    "add_space_between_ampersand": [" & ", r"&"],
    "add_space_before_opening_parentheses": [" (", r"\("],
    "add_space_after_closing_parentheses": [") ", r"\)"],
    "replace_hyphen_by_space": [" ", r"\s*-\s*"],
    "replace_underscore_by_space": [" ", r"\s*_\s*"],
    "remove_all_punctuation": [" ", r"([^\w\s])"],
    "remove_punctuation_except_dot": [" ", r"([^\w\s.])"],
    "remove_mentions": [" ", r"@\S+"],
    "remove_hashtags": [" ", r"#\S+"],
    "remove_numbers": [" ", r"\w*\d+\w*"],
    "remove_text_punctuation": ["", r'\;|\:|\,|\.|\?|\!|"|\''],
    "remove_text_punctuation_except_dot": ["", r'\;|\:|\,|\?|\!|"\''],
    "remove_math_symbols": [" ", r"\+|\-|\*|\>|\<|\=|\%"],
    "remove_math_symbols_except_dash": [" ", r"\+|\*|\>|\<|\=|\%"],
    "remove_parentheses": ["", r"\(|\)"],
    "remove_brackets": ["", r"\[|\]"],
    "remove_curly_brackets": ["", r"\{|\}"],
    "remove_single_quote_next_character": [" ", r"'\w+"],
    "remove_single_quote": [" ", r"'"],
    "remove_double_quote": [" ", r'"'],
    "remove_words_in_parentheses": [" ", r"\([^()]*\)"],
    "remove_words_between_slashes": [
        " ",
        r"/.*?/",
    ],  # commonly comes up in SEC company names
    "repeat_remove_words_in_parentheses": [" ", r"remove_words_in_parentheses"],
}

DEFAULT_CLEANING_RULES_LIST = [
    "remove_word_the_from_the_end",
    "remove_word_the_from_the_beginning",
    "replace_ampersand_by_AND",
    "replace_hyphen_by_space",
    "replace_underscore_by_space",
    "remove_all_punctuation",
    "remove_numbers",
    "remove_math_symbols",
    "remove_words_in_parentheses",
    "remove_parentheses",
    "remove_brackets",
    "remove_curly_brackets",
    "enforce_single_space_between_words",
]
NAME_LEGAL_TERMS_DICT_FILE = "us_legal_forms.json"
NAME_JSON_ENTRY_LEGAL_TERMS = "legal_forms"


class LegalTermLocation(enum.Enum):
    """The location of the legal terms within the name string."""

    AT_THE_END = 1
    ANYWHERE = 2


class Lettercase(enum.Enum):
    """Allowed cases for output strings."""

    LOWER = 1
    TITLE = 2
    UPPER = 3


class HandleLegalTerms(enum.Enum):
    """Whether to leave, remove, or normalize legal terms."""

    NORMALIZE = 3
    LEAVE_AS_IS = 1
    REMOVE = 2


def _get_legal_terms_dict() -> dict[str, list]:
    json_source = files("pudl.package_data.settings").joinpath(
        NAME_LEGAL_TERMS_DICT_FILE
    )
    with json_source.open() as json_file:
        legal_terms_dict = json.load(json_file)[NAME_JSON_ENTRY_LEGAL_TERMS]["en"]
    return legal_terms_dict


class CompanyNameCleaner(BaseModel):
    """Class to normalize/clean up text based company names."""

    cleaning_rules_list: list[str] = DEFAULT_CLEANING_RULES_LIST
    """A list of cleaning rules that the CompanyNameCleaner should apply.

    Will be validated to ensure rules comply to allowed cleaning functions.
    """
    handle_legal_terms: HandleLegalTerms = HandleLegalTerms.NORMALIZE
    """A flag to indicate how to habndle legal terms.

    Options are to remove, normalize, or keep them as is.
    """
    place_word_the_at_beginning: bool = False
    """A flag to indicate whether to move 'the' to the start of a string.

    If True, then if the word 'the' appears at the end of a string,
    remove it and place 'the' at the beginning of the string.
    """
    remove_unicode: bool = False
    """Define if unicode characters should be removed from text's name.

    This cleaning rule is treated separated from the regex rules because it
    depends on the language of the text's name. For instance, Russian or
    Japanese text's may contain unicode characters, while Portuguese and
    French companies may not.
    """
    output_lettercase: Lettercase = Lettercase.LOWER
    """Define the letter case of the cleaning output."""
    legal_term_location: LegalTermLocation = LegalTermLocation.AT_THE_END
    """Indicates where in the string legal terms are found."""
    remove_accents: bool = False
    """Flag to indicate whether to remove accents from strings.

    If True, replace letters with accents with non-accented ones.
    """
    legal_terms_dict: dict[str, list] = Field(default_factory=_get_legal_terms_dict)

    @model_validator(mode="after")
    def _validate_cleaning_rules(self) -> Self:
        cleaning_rules_list_valid = [
            rule for rule in self.cleaning_rules_list if rule in CLEANING_RULES_DICT
        ]
        invalid_rules = set(self.cleaning_rules_list) - set(cleaning_rules_list_valid)
        if len(invalid_rules) > 0:
            logger.warning(
                f"The following cleaning rules have not been implemented \
                        in the CompanyNameCleaner class and will have no effect: \
                        {invalid_rules}"
            )
        if ("remove_all_punctuation_except_dot" in cleaning_rules_list_valid) and (
            "remove_all_punctuation" in cleaning_rules_list_valid
        ):
            cleaning_rules_list_valid.remove("remove_all_punctuation")
        if ("remove_text_punctuation" in cleaning_rules_list_valid) and (
            "remove_text_punctuation_except_dot" in cleaning_rules_list_valid
        ):
            cleaning_rules_list_valid.remove("remove_text_punctuation")
        if ("remove_math_symbols" in cleaning_rules_list_valid) and (
            "remove_math_symbols_except_dash" in cleaning_rules_list_valid
        ):
            cleaning_rules_list_valid.remove("remove_math_symbols")
        self.cleaning_rules_list = cleaning_rules_list_valid
        return self

    def _apply_regex_rules(
        self, col: pd.Series, dict_regex_rules: dict[str, list[str]]
    ) -> pd.Series:
        r"""Applies several cleaning rules based on a custom dictionary.

        The dictionary must contain cleaning rules written in regex format.

        Arguments:
            col (pd.Series): The column that needs to be cleaned.
            dict_regex_rules (dict): a dictionary of cleaning rules written in regex with the format
                [rule name] : ['replacement', 'regex rule']

        Returns:
            (pd.Series): the modified/cleaned column.
        """
        clean_col = col
        # Iterate through the dictionary and apply each regex rule
        for _, cleaning_rule in dict_regex_rules.items():
            # First element is the replacement
            replacement = cleaning_rule[0]
            # Second element is the regex rule
            regex_rule = cleaning_rule[1]

            # Check if the regex rule is actually a reference to another regex rule.
            # By adding a name of another regex rule in the place of the rule itself allows the execution
            # of a regex rule twice
            if regex_rule in dict_regex_rules:
                replacement = dict_regex_rules[cleaning_rule[1]][0]
                regex_rule = dict_regex_rules[cleaning_rule[1]][1]

            # Make sure to use raw string
            regex_rule = rf"{regex_rule}"
            # Apply the regex rule
            clean_col = clean_col.str.replace(regex_rule, replacement, regex=True)

        return clean_col

    def _remove_unicode_chars(self, col: pd.Series) -> pd.Series:
        """Removes unicode characters that are unreadable in ASCII format.

        Arguments:
            col (pd.Series): series containing unicode characters.

        Returns:
            (pd.Series): the corresponding input series without unicode characters.
        """
        return col.str.encode("ascii", "ignore").str.decode("ascii")

    def _move_the_to_beginning(self, col: pd.Series) -> pd.Series:
        remove_the_from_end_regex_rule = CLEANING_RULES_DICT[
            "remove_word_the_from_the_end"
        ][1]
        remove_the_from_end_replacement = CLEANING_RULES_DICT[
            "remove_word_the_from_the_end"
        ][0]
        # find matches with the at end
        the_at_end_matches = col.str.contains(
            remove_the_from_end_regex_rule, regex=True
        )

        # remove the from end of strings
        clean_col = col.str.replace(
            remove_the_from_end_regex_rule,
            remove_the_from_end_replacement,
            regex=True,
        )
        clean_col = clean_col.where(~the_at_end_matches, "the " + clean_col)
        return clean_col

    def _apply_cleaning_rules(self, col: pd.Series) -> pd.Series:
        """Apply the cleaning rules from the dictionary of regex rules."""
        if self.place_word_the_at_beginning:
            col = self._move_the_to_beginning(col)

        cleaning_dict = {}
        for rule_name in self.cleaning_rules_list:
            cleaning_dict[rule_name] = CLEANING_RULES_DICT[rule_name]

        # Apply all the cleaning rules
        clean_col = self._apply_regex_rules(col, cleaning_dict)
        # Enforce single spaces again in case some where created
        clean_col = clean_col.str.replace(
            CLEANING_RULES_DICT["enforce_single_space_between_words"][1],
            CLEANING_RULES_DICT["enforce_single_space_between_words"][0],
            regex=True,
        )
        return clean_col

    def _apply_normalization_of_legal_terms(self, col: pd.Series) -> pd.Series:
        """Apply the normalization of legal terms according to dictionary of regex rules."""
        # Make sure to remove extra spaces, so legal terms can be found in the end (if requested)
        clean_col = col.str.strip()
        # Apply normalization for legal terms
        # Iterate through the dictionary of legal terms
        for replacement, legal_terms in self.legal_terms_dict.items():
            # Each replacement has a list of possible terms to be searched for
            replacement = " " + replacement.lower() + " "
            for legal_term in legal_terms:
                # Make sure to use raw string
                legal_term = legal_term.lower()
                # If the legal term has . (dots), then apply regex directly on the legal term
                # Otherwise, if it's a legal term with only letters in sequence, make sure
                # that regex find the legal term as a word (\\bLEGAL_TERM\\b)
                if legal_term.find(".") > -1:
                    legal_term = legal_term.replace(".", "\\.")
                else:
                    legal_term = "\\b" + legal_term + "\\b"
                # Check if the legal term should be found only at the end of the string
                if self.legal_term_location == LegalTermLocation.AT_THE_END:
                    legal_term = legal_term + "$"
                # ...and it's a raw string
                regex_rule = rf"{legal_term}"
                # Apply the replacement
                clean_col = clean_col.str.replace(regex_rule, replacement, regex=True)
        return clean_col

    def _apply_removal_of_legal_terms(self, col: pd.Series) -> pd.Series:
        """Remove legal terms from a string."""
        full_terms_list = list(self.legal_terms_dict.keys())
        for key in self.legal_terms_dict:
            full_terms_list += self.legal_terms_dict[key]
            full_terms_list += [key]
        clean_col = col.str.strip()
        regex_rule = (
            r"\b(?:" + "|".join(re.escape(word) for word in full_terms_list) + r")\b"
        )
        clean_col = clean_col.str.replace(regex_rule, "", regex=True)
        clean_col = clean_col.str.strip()
        # strip commas or other special chars that might be at the end of the name
        clean_col = clean_col.str.strip(".,!?()':;[]* \n\t")
        return clean_col

    def get_clean_data(self, col: pd.Series) -> pd.Series:
        """Clean names and normalize legal terms.

        Arguments:
            col (pd.Series): the column that is to be cleaned

        Returns:
            clean_col (pd.Series): the clean version of the column
        """
        # remove unicode characters
        clean_col = self._remove_unicode_chars(col) if self.remove_unicode else col

        clean_col = clean_col.str.strip().str.lower()
        clean_col = self._apply_cleaning_rules(clean_col)

        # Handle legal terms
        if self.handle_legal_terms == HandleLegalTerms.REMOVE:
            clean_col = self._apply_removal_of_legal_terms(clean_col)
        elif self.handle_legal_terms == HandleLegalTerms.NORMALIZE:
            clean_col = self._apply_normalization_of_legal_terms(clean_col)

        # Apply the letter case, if different from 'lower'
        if self.output_lettercase == Lettercase.UPPER:
            clean_col = clean_col.str.upper()
        elif self.output_lettercase == Lettercase.TITLE:
            clean_col = clean_col.str.title()

        # Remove excess of white space that might be introduced during previous cleaning
        clean_col = clean_col.str.strip()
        clean_col = clean_col.str.replace(r"\s+", " ", regex=True)

        return clean_col

    def apply_name_cleaning(
        self, df: pd.DataFrame, return_as_dframe: bool = False
    ) -> pd.DataFrame:
        """Clean up text names in a dataframe.

        Arguments:
            df (dataframe): the input dataframe that contains the text's name to be cleaned
            return_as_dframe (bool): whether to return the cleaned data as a dataframe or series.
                Useful to return as a dataframe if used in a cleaning pipeline with no
                vectorization step after name cleaning. If multiple columns are passed in for
                cleaning then output will be a dataframe regardless of this parameter.

        Returns:
            df (dataframe): the clean version of the input dataframe
        """
        if isinstance(df, pd.DataFrame) and len(df.columns) > 1:
            clean_df = pd.DataFrame()
            for col in df.columns:
                clean_df = pd.concat(
                    [clean_df, self.get_clean_data(clean_df[col])], axis=1
                )
            return clean_df
        out = self.get_clean_data(df.squeeze())
        if return_as_dframe:
            return out.to_frame()
        return out

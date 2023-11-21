"""This module contains the implementation of CompanyNameCleaner class from OS-Climate's financial-entity-cleaner package."""

import enum
import json
import logging
import re
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel

logger = logging.getLogger(__name__)

CLEANING_RULES_DICT = {
    "remove_email": [" ", r"\S*@\S*\s?"],
    "remove_url": [" ", r"https*\S+"],
    "remove_word_the_from_the_end": [" ", r"the$"],
    "place_word_the_at_the_beginning": [" ", r"the$"],
    "remove_www_address": [" ", r"https?://[.\w]{3,}|www.[.\w]{3,}"],
    "enforce_single_space_between_words": [" ", r"\s+"],
    "replace_amperstand_by_AND": [" and ", r"&"],
    "add_space_between_amperstand": [" & ", r"&"],
    "add_space_before_opening_parentheses": [" (", r"\("],
    "add_space_after_closing_parentheses": [") ", r"\)"],
    "replace_amperstand_between_space_by_AND": [" and ", r"\s+&\s+"],
    "replace_hyphen_by_space": [" ", r"-"],
    "replace_hyphen_between_spaces_by_single_space": [" ", r"\s+-\s+"],
    "replace_underscore_by_space": [" ", r"_"],
    "replace_underscore_between_spaces_by_single_space": [" ", r"\s+_\s+"],
    "remove_all_punctuation": [" ", r"([^\w\s])"],
    "remove_punctuation_except_dot": [" ", r"([^\w\s.])"],
    "remove_mentions": [" ", r"@\S+"],
    "remove_hashtags": [" ", r"#\S+"],
    "remove_numbers": [" ", r"\w*\d+\w*"],
    "remove_text_puctuation": [" ", r'\;|\:|\,|\.|\?|\!|"'],
    "remove_text_puctuation_except_dot": [" ", r'\;|\:|\,|\?|\!|"'],
    "remove_math_symbols": [" ", r"\+|\-|\*|\>|\<|\=|\%"],
    "remove_math_symbols_except_dash": [" ", r"\+|\*|\>|\<|\=|\%"],
    "remove_parentheses": ["", r"\(|\)"],
    "remove_brackets": ["", r"\[|\]"],
    "remove_curly_brackets": ["", r"\{|\}"],
    "remove_single_quote_next_character": [" ", r"'\w+"],
    "remove_single_quote": [" ", r"'"],
    "remove_double_quote": [" ", r'"'],
    "remove_words_in_parentheses": [" ", r"\([^()]*\)"],
    "repeat_remove_words_in_parentheses": [" ", r"remove_words_in_parentheses"],
}


class CleaningRules(BaseModel):
    """Configuration for CompanyNameCleaner class."""

    input_column: str
    output_column: str | None = None
    cleaning_rules: list[str] = [
        "replace_amperstand_between_space_by_AND",
        "replace_hyphen_between_spaces_by_single_space",
        "replace_underscore_by_space",
        "replace_underscore_between_spaces_by_single_space",
        "remove_text_puctuation_except_dot",
        "remove_math_symbols",
        "remove_words_in_parentheses",
        "remove_parentheses",
        "remove_brackets",
        "remove_curly_brackets",
        "enforce_single_space_between_words",
    ]

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply ComanyNameCleaner to input DataFrame with rules specified in cleaning_rules."""
        df = CompanyNameCleaner(cleaning_rules=self.cleaning_rules).get_clean_df(
            df, self.input_column, self.output_column
        )
        return df


class LegalTermLocation(enum.Enum):
    """The location of the legal terms within the name string."""

    AT_THE_END = 1
    ANYWHERE = 2


class CompanyNameCleaner:
    """Class to normalize/clean up text based company names.

    Attributes:
        _cleaning_rules_dict (dict): the dictionary of cleaning rules loaded from a json file. The cleaning rules
                    are written in regex format and can be easily updated or incremented by changing the file.
        _cleaning_rules_list (list): a list of cleaning rules to be applied. The dictionary of
                    cleaning rules may contain rules that are not needed. Therefore, the _cleaning_rules_list
                    allows the user to select only the cleaning rules necessary of interest. This list is also
                    read from a json file and can be easily updated by changing the file or setting up the
                    correspondent class property.
        _normalize_legal_terms (bool): a flag to indicate if the cleaning process must normalize
                    text's legal terms. e.g. LTD => LIMITED.
        _dict_legal_terms (dict): a subset of the legal terms dictionary filtered by language and country.
                    This will be the legal term dictionary to be applied during cleaning. The user can call the
                    set_current_legal_term_dict() method to change the dictionary to another language/country.
        _output_lettercase (str): indicates the letter case (lower, by default) as the result of the cleaning
                    Other options are: 'upper' and 'title'.
        _legal_term_location (enum): Where in the string legal terms are found.
        _remove_unicode (bool): indicates if the unicode character should be removed or not, which may depend
                    on the language of the text's name.
        _remove_accents (bool): indicates if letters with accents should be replaced with non-accented ones.
    """

    # Constants used internally by the class
    __NAME_LEGAL_TERMS_DICT_FILE = "us_legal_forms.json"
    __NAME_JSON_ENTRY_LEGAL_TERMS = "legal_forms"

    def __init__(
        self,
        cleaning_rules: list[str],
        cleaning_rules_definitions_dict: dict = CLEANING_RULES_DICT,
    ) -> None:
        """Constructor method.

        Arguments:
            cleaning_rules_list: A list of the rules to apply for cleaning. By default is
                DEFAULT_COMPANY_CLEANING_RULES
            cleaning_rules_definitions_dict: A dictionary of cleaning rules where the keys
                are the name of the cleaning rule and the value is the rule. By default is
                CLEANING_RULES_DICT

        Returns:
            CompanyNameCleaner (object)
        """
        # The dictionary of cleaning rules define which regex functions to apply to the data
        # A default set of regex rules is defined, but it can be changed by the user.
        self._cleaning_rules_dict = cleaning_rules_definitions_dict
        self._cleaning_rules_list = cleaning_rules

        self._normalize_legal_terms = (
            True  # indicates if legal terms need to be normalized
        )
        # The dictionary of legal terms define how to normalize the text's legal form abreviations
        json_source = files("pudl.package_data.settings").joinpath(
            self.__NAME_LEGAL_TERMS_DICT_FILE
        )
        with as_file(json_source) as json_file_path:
            self._dict_legal_terms = self._load_json_file(json_file_path)[
                self.__NAME_JSON_ENTRY_LEGAL_TERMS
            ]["en"]

        # Define the letter case of the cleaning output
        self._output_lettercase = "lower"

        # Where in the string are legal terms found
        self._legal_term_location = LegalTermLocation.AT_THE_END

        # Define if unicode characters should be removed from text's name
        # This cleaning rule is treated separated from the regex rules because it depends on the
        # language of the text's name. For instance, russian or japanese text's may contain
        # unicode characters, while portuguese and french companies may not.
        self._remove_unicode = False

        # Define if the letters with accents are replaced with non-accented ones
        self._remove_accents = False

    def _load_json_file(self, file_to_read: Path) -> Any:
        """Reads a json file and returns its content as a python dictionary.

        Args:
            file_to_read (str): complete path and name of the json file to read.

        Returns:
            (dict): the content of the json file as a python dictionary.
        """
        json_file = file_to_read.open()
        dict_content = json.load(json_file)
        return dict_content

    def _apply_regex_rules(
        self, str_value: str, dict_regex_rules: dict[str, list[str]]
    ) -> str:
        r"""Applies several cleaning rules based on a custom dictionary.

        The dictionary must contain cleaning rules written in regex format.

        Arguments:
            str_value (str): any value as string to be cleaned up.
            dict_regex_rules (dict): a dictionary of cleaning rules writen in regex with the format
                [rule name] : ['replacement', 'regex rule']

        Returns:
            (str): the modified/cleaned value.
        """
        clean_value = str_value
        # Iterate through the dictionary and apply each regex rule
        for name_rule, cleaning_rule in dict_regex_rules.items():
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

            # Treat the special case of the word THE at the end of a text's name
            found_the_word_the = None
            if name_rule == "place_word_the_at_the_beginning":
                found_the_word_the = re.search(regex_rule, clean_value)

            # Apply the regex rule
            clean_value = re.sub(regex_rule, replacement, clean_value)

            # Adjust the name for the case of rule <place_word_the_at_the_beginning>
            if found_the_word_the is not None:
                clean_value = "the " + clean_value

        return clean_value

    def _remove_unicode_chars(self, value: str) -> str:
        """Removes unicode character that is unreadable when converted to ASCII format.

        Arguments:
            value (str): any string containing unicode characters.

        Returns:
            (str): the corresponding input string without unicode characters.
        """
        # Remove all unicode characters if any
        clean_value = value.encode("ascii", "ignore").decode()
        return clean_value

    def _apply_cleaning_rules(self, company_name: str) -> str:
        """Apply the cleaning rules from the dictionary of regex rules."""
        cleaning_dict = {}
        for rule_name in self._cleaning_rules_list:
            cleaning_dict[rule_name] = self._cleaning_rules_dict[rule_name]

        # Apply all the cleaning rules
        clean_company_name = self._apply_regex_rules(company_name, cleaning_dict)
        return clean_company_name

    def _apply_normalization_of_legal_terms(self, company_name: str) -> str:
        """Apply the normalization of legal terms according to dictionary of regex rules."""
        # Make sure to remove extra spaces, so legal terms can be found in the end (if requested)
        clean_company_name = company_name.strip()

        # Apply normalization for legal terms
        # Iterate through the dictionary of legal terms
        for replacement, legal_terms in self._dict_legal_terms.items():
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
                if self._legal_term_location == LegalTermLocation.AT_THE_END:
                    legal_term = legal_term + "$"
                # ...and it's a raw string
                regex_rule = rf"{legal_term}"
                # Apply the replacement
                clean_company_name = re.sub(regex_rule, replacement, clean_company_name)
        return clean_company_name

    def get_clean_data(self, company_name: str) -> str:
        """Clean a name and normalize legal terms.

        If ``company_name`` is null or not a string value, pd.NA
        will be returned.

        Arguments:
            company_name (str): the original text

        Returns:
            clean_company_name (str): the clean version of the text
        """
        if not isinstance(company_name, str):
            if company_name is not pd.NA:
                logger.warning(f"{company_name} is not a string.")
            return pd.NA

        # Remove all unicode characters in the text's name, if requested
        if self._remove_unicode:
            clean_company_name = self._remove_unicode_chars(company_name)
        else:
            clean_company_name = company_name

        # Remove space in the beginning and in the end and convert it to lower case
        clean_company_name = clean_company_name.strip().lower()

        # Apply all the cleaning rules
        clean_company_name = self._apply_cleaning_rules(clean_company_name)

        # Apply normalization for legal terms
        if self._normalize_legal_terms:
            clean_company_name = self._apply_normalization_of_legal_terms(
                clean_company_name
            )

        # Apply the letter case, if different from 'lower'
        if self._output_lettercase == "upper":
            clean_company_name = clean_company_name.upper()
        elif self._output_lettercase == "title":
            clean_company_name = clean_company_name.title()

        # Remove excess of white space that might be introduced during previous cleaning
        clean_company_name = clean_company_name.strip()
        clean_company_name = re.sub(r"\s+", " ", clean_company_name)

        return clean_company_name

    def get_clean_df(
        self,
        df: pd.DataFrame,
        in_company_name_attribute: str | list,
        out_company_name_attribute: str | list | None,
    ) -> pd.DataFrame:
        """Clean up text names in a dataframe.

        Arguments:
            df (dataframe): the input dataframe that contains the text's name to be cleaned
            in_company_name_attribute (str): the attribute in the dataframe that contains the names
            out_company_name_attribute (str): the attribute to be created for the clean version of
                the text's name

        Returns:
            df (dataframe): the clean version of the input dataframe
        """
        # Check if the company_name attribute exists in the dataframe
        if in_company_name_attribute not in df.columns:
            raise KeyError(f"Column {in_company_name_attribute} not in dataframe.")
        if out_company_name_attribute is None:
            out_company_name_attribute = in_company_name_attribute

        # Make a copy so not to change the original dataframe
        new_df = df.copy()

        # Creates the new output attribute that will have the clean version of the text's name
        new_df[out_company_name_attribute] = new_df[in_company_name_attribute].apply(
            self.get_clean_data
        )

        return new_df

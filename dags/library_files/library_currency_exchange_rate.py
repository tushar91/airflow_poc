# -*- coding: utf-8 -*-
import csv
from dateutil.parser import parse
from dags.library_files.library_csv import write_rows_to_csv
import logging


def dimension_currency_to_csv(in_filepath: str, out_filepath: str):
    """
    Function to parse raw CSV file & prepare data for dimension currency.
    Following data is required:
     - Currency code (cur_code)
     - One Euro Value (one_euro_value)
     - Last Updated date (last_updated_date)
     - Serial code (serial_code)

    :param in_filepath: (str) - Path of the raw CSV file to parse
    :param out_filepath: (str) - Path of the final CSV

    Note:
        Modified date: 08-04-2021
        Author: TB
    """
    # initialise variables
    required_rows = []
    total_rows_written = 0

    # read file to get only the rows required for dimension currency
    with open(in_filepath, 'r') as csvfile:
        in_reader = csv.reader(csvfile, delimiter=';')
        for i, row in enumerate(in_reader):
            clean_values = []
            # 1: Row associated with Currency Serial Code
            # 2: Row associated with Currency name
            # > 5: Row associated with Last updated of Currency & Value
            if i == 1:
                serial_code = row
            if i == 2:
                currency_name = row
            if i > 6:
                if len(row) == len(currency_name):
                    # clean the file rows to get the data
                    for j in range(1, len(currency_name)):
                        currency_value = row[j].replace(",", ".")
                        if currency_value and currency_value != '-':
                            clean_values.append((
                                extract_value_from_brackets(currency_name[j]),
                                currency_value,
                                parse(row[0], dayfirst=True).date(),
                                serial_code[j]
                            ))

                    # Write results to file
                    if clean_values:
                        rows_written = write_rows_to_csv(clean_values,
                                                         out_filepath,
                                                         'a')
                        logging.info(f"Nb {i}: {rows_written} rows written")
                        total_rows_written += rows_written

    logging.info(f"Total files written: {total_rows_written}")


def exchange_rate_history_to_csv(in_filepath: str, out_filepath: str):
    """
    Function to parse raw CSV file & prepare data for Fact Exchange rate
     history.
    Following data is required:
        - History date (history_date)
        - From Currecny code (from_cur_code)
        - To Currency code (to_cur_code)
        - Exchange rate (exchange_rate)
            - from_cur_code * exchange_rate = to_cur_code

    :param in_filepath: (str) - Path of the raw CSV file to parse
    :param out_filepath: (str) - Path of the final CSV

    """
    # initialise variables
    total_rows_written = 0
    headers = None

    # read file & prepare the data for fact table
    with open(in_filepath, 'r') as csvfile:
        in_reader = csv.reader(csvfile, delimiter=';')

        for i, row in enumerate(in_reader):
            # 3rd row acts as header
            if i == 2:
                headers = row

            if i > 6:
                # Calculate exchange rate for the currencies in row
                clean_values = calculate_exchange_rate_history(headers, row)

                if clean_values:
                    # Write results to file
                    rows_written = write_rows_to_csv(clean_values,
                                                     out_filepath, 'a')

                    logging.info(f"Nb {i}: {rows_written} rows written: ")
                    total_rows_written += rows_written

    logging.info(f"Total files written: {total_rows_written}")


def calculate_exchange_rate_history(headers: list, value_row: list) -> list:
    """
    Function to calculate Exchange rate from a row of rates of different
    currencies
    :param headers: (list) - List of headers or currency codes
    :param value_row: (list) - List of CSV rows conatining raw data
    :return: (list) - Clean Exchange rate row, which is written to CSV

    Note:
        Modified date: 08-04-2021
        Author: TB
    """
    clean_values = []
    if len(headers) == len(value_row):

        for j in range(1, len(headers)):
            for k in range(1, len(headers)):
                # skip when both currency are same
                if headers[j] == headers[k]:
                    continue

                from_value = value_row[j].replace(",", ".")
                to_value = value_row[k].replace(",", ".")

                if from_value and from_value != '-' and to_value\
                        and to_value != '-':
                    clean_values.append(
                        (
                            parse(value_row[0], dayfirst=True).date(),
                            extract_value_from_brackets(headers[j]),
                            extract_value_from_brackets(headers[k]),
                            float(to_value) / float(from_value)
                        )
                    )

    return clean_values


def extract_value_from_brackets(raw_input: str) -> str:
    """
    Function which extracts value from between brackets (from string)

    Eg: Input = This is (bracket)
        Output = bracket
    :param raw_input: (str) - Raw input string with value in bracket

    :return: (str) - Value between brackets

    Note:
        Modified date: 08-04-2021
        Author: TB
    """
    if not raw_input:
        return None
    return raw_input[raw_input.find("(") + 1:raw_input.find(")")]

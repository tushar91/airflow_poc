# -*- coding: utf-8 -*-
import csv
import requests
import logging


def download_csv_from_url(csv_url: str, local_filepath: str) -> None:
    """
    Download file from URL & store on local in the target filepath

    :param csv_url: (str) - Web URL for the CSV to download
    :param local_filepath: (str) - Filepath to store on local
    :return: None

    Note:
        Modified date: 08-04-2021
        Author: TB
    """
    response = requests.get(csv_url, stream=True)
    response.raise_for_status()
    with open(local_filepath, 'w') as f:
        writer = csv.writer(f)
        for line in response.iter_lines():
            clean_row = ['{}'.format(x) for x in
                         list(csv.reader([line.decode('utf-8')],
                                         delimiter=',',
                                         quotechar='"'))[0]]
            writer.writerow(clean_row)


def write_rows_to_csv(rows_to_write: list, dest_filepath: str,
                      write_mode: str = "w") -> int:
    """
    Method to write list of lists to a csv

    :param rows_to_write: (list) - Rows of data to write
    :param dest_filepath: (str) - Path of the file to write data to
    :param write_mode: (str) - Mode to open file in
    :return: (int) - Number of rows written to CSV

    Note:
        Modified date: 08-04-2021
        Author: TB
    """
    if rows_to_write:
        total_rows = len(rows_to_write)
        if total_rows > 0:
            with open(dest_filepath, write_mode, newline="") as fout:
                writer = csv.writer(fout)
                writer.writerows(rows_to_write)
            logging.info(f"{total_rows} rows have been written")
            return total_rows
    else:
        logging.error("There's no rows to write")

"""
GoogleSheetsConnector(): Connection between Google Sheets and SQLite
"""

import json
import logging
from typing import List

import gspread
import pandas as pd

from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger('GoogleSheetsConnector')

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']


class GoogleSheetsConnector:
    """
    - Extract Data from Google Sheets and load to SQLite
    - Write Data to Google Sheets
    """
    def __init__(self, config, db_conn):
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(config.get('gsheets_credentials')), scope)
        self.__client = gspread.authorize(credentials)
        self.__sheet = self.__client.open(config.get('sheet_name'))
        self.__dbconn = db_conn
        logger.info('Google Sheets connection established.')

    def create_tables(self, file_path: str) -> None:
        """
        Creates DB tables
        :param file_path: absolute path to the .sql file
        :return: None
        """
        ddl_sql = open(file_path, 'r').read()
        self.__dbconn.executescript(ddl_sql)

    def update_cell(self, worksheet: str, cell: str, new_value: any) -> None:
        """
        Update a particular cell of google sheet
        :param worksheet: name of the worksheet
        :param cell: cell to be updated
        :param new_value: new value of the cell
        :return: None
        """
        self.__sheet.worksheet(worksheet).update_acell(cell, new_value)

    def clear_worksheet(self, worksheet: str) -> None:
        """
        Clear a worksheet completely
        :param worksheet: name of the worksheet
        :return: None
        """
        self.__sheet.worksheet(worksheet).clear()

    def extract_data(self) -> None:
        """
        Extracts data from Google sheets
        :return: None
        """
        worksheet_list = self.__sheet.worksheets()

        logger.info('Extracting mappings from Google sheet.')

        for worksheet in worksheet_list:
            logger.info('Extracting %s', worksheet.title)

            data = self.__sheet.worksheet(worksheet.title)
            df = pd.DataFrame(data.get_all_records())
            logger.info('%s %s extracted', len(df.index), worksheet.title)
            if len(df.index):
                df.to_sql(worksheet.title, self.__dbconn, if_exists='append', index=False)

        logger.info('Mappings were successfully extracted from google sheets.')

    def update_in_range(self, worksheet: str, start_cell: str, end_cell: str, cell_values: List[str]) -> None:
        """
        Update Range in batch
        :param worksheet: name of the worksheet
        :param start_cell: start cell
        :param end_cell: end cell
        :param cell_values: list of values to be updated
        :return: None
        """
        cell_list = self.__sheet.worksheet(worksheet).range(('%s:%s', start_cell, end_cell))

        for i, val in enumerate(cell_values):
            cell_list[i].value = val

        self.__sheet.worksheet(worksheet).update_cells(cell_list)

    def create_worksheet(self, title: str) -> None:
        """
        :param title: title of the new worksheet.
        :return: None
        """
        if title in [ws.title for ws in self.__sheet.worksheets()]:
            logger.error("Worksheet '%s' already exists", title)
        else:
            logger.info('Creating Worksheet')
            self.__sheet.add_worksheet(title, rows=None, cols=None)
            logger.info('Worksheet created')

    def write_to_sheet(self, sheet_range: str, params: dict, body: dict) -> None:
        """
        :param sheet_range: The `A1 notation <https://developers.google.com/sheets/api/guides/concepts#a1_notation>`_
                          of a range to search for a logical table of data. Values will be appended after the last row of the table.
        :param params: `Query parameters <https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append#query-parameters>`_.
        :param body: `Request body <https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append#request-body>`_.
        :return: None
        """
        logger.info('Writing Data to sheet')
        self.__sheet.values_append(sheet_range, params, body)
        logger.info('Data successfully uploaded')

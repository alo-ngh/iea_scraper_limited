import logging
import re
from datetime import datetime
from ftplib import FTP, error_perm
from pathlib import Path, PureWindowsPath
from time import sleep
from typing import NoReturn

import pandas as pd
from pandas.errors import EmptyDataError
from pyodbc import ProgrammingError
from sqlalchemy import create_engine

from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.settings import ARGUS_FTP_USERNAME, ARGUS_FTP_PASSWORD, FILE_STORE_PATH, EXT_DB_STR

logger = logging.getLogger(__name__)


class ArgusPricesJob(ExtDbApiJob):
    """
    Scraper for bulk loading all Argus prices model.pi
    Source: ftp.argusmedia.com
    Argus prices model details: https://www.argusmedia.com/en/methodology/data-documentation
    """
    title: str = 'Argus Media - Prices (FTP)'

    argus_host = "ftp.argusmedia.com"
    root_dir = FILE_STORE_PATH / 'com_argusmedia' / 'FTP'
    path_to_ignore = Path('ftpFiles') / 'DATA'

    table_schema = 'argus_prices'

    chunk_size = 10000
    datasets = ['argus_prices', 'forward_curves']
    key_columns = {'argus_prices': ['Code', 'TS Type', 'Cont Fwd', 'PT Code', 'Date'],
                   'forward_curves': ['Product', 'Market', 'ValuationType', 'NatGasLocationReference',
                                      'OptionStrikePrice', 'Term', 'PromptIndicator', 'ForwardPeriod', 'Year',
                                      'TradeDate', 'Unit', 'FCRepositoryId']}
    parse_dates = {'argus_prices': ['Date'],
                   'forward_curves': ['TradeDate']}

    # list of modules not to load in data table
    doc_dir = 'DOCUMENTATION'
    fcdoc_dir = 'FCDOC'
    fwd_prefix = 'FWD'
    doc_glob_pattern = '**/latest*.csv'

    # list of files to not load
    files_blocked_list = []

    const_min_date = datetime(1, 1, 1)

    def __init__(self, files_to_load_mask='latest*.csv', skip_docs=False, **kwargs):
        """
        Constructor.
        Overrides parent to set self.data as dict.
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.data = dict()
        self.local_files_stats = None
        self.downloaded_files = []
        self.download = None
        # file pattern to load
        self.files_to_load_mask = files_to_load_mask
        # converts a file blob expression '<pattern>*.csv' to regex expression '<pattern>(.*).csv'
        # this will allow us to extract the expression between parenthesis (argus FileName)
        split = self.files_to_load_mask.split('*')
        re_prefix = split[0]
        re_suffix = split[1]
        self.file_name_pattern = f'{re_prefix}(.*){re_suffix}'

        # skip loading of documentation tables if True
        self.skip_docs = skip_docs

    def get_sources(self) -> NoReturn:
        """
        Lists all files to download.
        :return: NoReturn
        """
        pass

    def transform(self):
        """
        This reloads all documentation tables.
        Also loads all data files into data table.
        :return: NoReturn
        """
        self.load_doc_tables()
        # argus prices: neither DOCUMENTATION nor FWD* folders
        folders_to_load = (mod for mod in self.root_dir.glob('*')
                           if mod.is_dir()
                           and self.fwd_prefix not in mod.name
                           and mod.name not in [self.doc_dir, self.fcdoc_dir, 'ftpFiles'])
        self.load_data_table(self.datasets[0], folders_to_load)
        # forward curves
        folders_to_load = (mod for mod in self.root_dir.glob(f'{self.fwd_prefix}*'))
        self.load_data_table(self.datasets[1], folders_to_load)

    def load_doc_tables(self):
        """
        Truncates and reloads each of documentation tables.
        One table for each .csv in DOCUMENTATION folder.
        :return:
        """
        if self.skip_docs:
            logger.info(f'skip_docs parameter is True. Skipping loading of documentation tables.')
            return

        logger.info(f'Loading documentation tables.')
        doc_path = self.root_dir / self.doc_dir
        fcdoc_path = self.root_dir / self.fcdoc_dir
        logger.debug(f'Documentation path: {doc_path} FC documentation path: {fcdoc_path}')

        # if self.download, keep only doc files that were downloaded, load all local otherwise
        files_to_load = (f for f in self.downloaded_files
                         if (f.parent.samefile(doc_path) or f.parent.samefile(fcdoc_path))
                         and f.match(self.doc_glob_pattern)) \
            if self.download \
            else list(doc_path.glob(self.doc_glob_pattern)) + list(fcdoc_path.glob(self.doc_glob_pattern))

        for file in files_to_load:
            logger.debug(f'File to load: {file}')
            table_name = file.stem

            df = pd.read_csv(file)
            logger.debug(f'{len(df)} rows loaded from file {file.name}')

            if file.name == "latestCategory.csv":
                logger.info(f'Splitting categories in {file.name}')
                subcategories = df["Category"].str.split("->", expand=True)
                logger.info(f'Number of subcategories: {len(subcategories.columns)}')
                for i in range(len(subcategories.columns) - 1):
                    df["Category" + str(i)] = subcategories[i]

                df = df.drop(['Category0'], axis=1)
                logger.debug(f"Columns in df after transformation: {','.join(df.columns)}")

            engine = create_engine(EXT_DB_STR, fast_executemany=True)

            with engine.connect().execution_options(autocommit=True) as con:

                logger.info(f'Loading {len(df)} rows to table {self.table_schema}.{table_name}')
                df.to_sql(table_name,
                          con=engine,
                          schema=self.table_schema,
                          if_exists='replace',
                          index=False,
                          chunksize=self.chunk_size)
                try:
                    con.execute(f'GRANT SELECT ON {self.table_schema}.{table_name} TO [IEA_EXTERNAL-DB_READ]')
                except ProgrammingError:
                    logger.warning(f'Could not grant select to [IEA_EXTERNAL-DB_READ] on '
                                   f'{self.table_schema}.{table_name}')
                    pass

    def upsert(self) -> NoReturn:
        """
        Overrides parent method to bypass API and write results directly to specific table.
        if self.full_load = True, it removes all rows from this provider and loads the content of self.data into it,,
        It loads self.data into a temporary table and runs a merge with final table otherwise.
        :return: NoReturn
        """
        logger.info("Writing to database.")
        for dataset in self.datasets:
            logger.info(f"Loading {dataset} into database.")

            final_table: str = f'{dataset}_data'
            temp_table: str = f'#{dataset}_temp'
            key_columns: list = self.key_columns[dataset]
            data: pd.DataFrame = self.data[dataset]

            if data is None or len(data) == 0:
                logger.info(f"{dataset}: no data to load into the database.")
                continue

            logger.debug(f'Database: {EXT_DB_STR}')
            engine = create_engine(EXT_DB_STR, fast_executemany=True)
            # engine.begin() starts a transaction

            if self.full_load:
                with engine.begin() as con:
                    self._truncate_table(con, self.table_schema, final_table)

                    logger.info(f'Loading {len(data)} rows to database.')
                    data['date_created'] = datetime.now()
                    data.to_sql(final_table,
                                con=con,
                                schema=self.table_schema,
                                index=False,
                                if_exists='append',
                                chunksize=self.chunk_size
                                )
            else:
                with engine.begin() as con:
                    logger.info(f'Loading {len(data)} rows to temporary table {temp_table}')
                    data.to_sql(temp_table,
                                con=con,
                                schema=self.table_schema,
                                index=False,
                                if_exists='append',
                                chunksize=self.chunk_size
                                )

                    logger.info(f'Merging data from {temp_table} into {final_table}')
                    merge_query = self.build_merge_query(data,
                                                         key_columns,
                                                         self.table_schema,
                                                         temp_table,
                                                         final_table)
                    con.execute(merge_query)

                # new transaction for index reorganisation
                with engine.begin() as con:
                    cci_index: str = f'cci_{final_table}'
                    logger.info(f'Reorganising clustered columnstore index: {cci_index}')
                    reorg_cci_query = f'ALTER INDEX {cci_index} ON {self.table_schema}.{final_table} REORGANIZE;'
                    con.execute(reorg_cci_query)

            logger.info(f'{dataset}: data successfully loaded to database.')

    @staticmethod
    def _truncate_table(connection, schema, table):
        """
        Truncates the table.
        :param connection: connection object.
        :param schema: SQL Server schema name.
        :param table: table name.
        :return: NoReturn
        """
        logger.debug('Sending truncate table statement')
        cleanup_statement = (f"IF OBJECT_ID('{schema}.{table}','U') is not null "
                             f"TRUNCATE TABLE {schema}.{table}")
        connection.execute(cleanup_statement)

    def load_data_table(self, dataset: str, folders_to_load):
        """
        Reads all .csv files in folders different than DOCUMENTATION and load them into data table.
        @param dataset: table to load. It translates to the key in the self.data dict, data frame loaded as value.
        @param folders_to_load: an Iterable determining the files to load into this table.
        :return:
        """
        logger.info(f'Loading {dataset} dataset.')
        module_dfs = []

        for folder in folders_to_load:
            folder_name = folder.name
            logger.debug(f'Loading folder: {folder_name}')
            dfs = []
            # if self.download, load only downloaded files from module respecting the mask and not in blocked list
            # load local files respecting the mask and not in blocked list otherwise
            files_to_load = (f for f in self.downloaded_files
                             if f.parent.name == folder_name
                             and f.match(self.files_to_load_mask)
                             and f.name not in self.files_blocked_list) \
                if self.download \
                else (f for f in folder.glob(self.files_to_load_mask)
                      if f.name not in self.files_blocked_list)

            for file in files_to_load:
                try:
                    logger.debug(f'Processing {file}')
                    df = pd.read_csv(file, parse_dates=self.parse_dates[dataset])
                    logger.debug(f'{len(df)} rows loaded from {file}.')
                    # if file is not empty...
                    if len(df) > 0:
                        df['Source'] = file.name
                        # calculate FileName as '<pattern>(FileName).csv'
                        argus_file_name = re.search(self.file_name_pattern, file.name, re.IGNORECASE).group(1)
                        df['FileName'] = argus_file_name
                        dfs.append(df)
                except pd.errors.EmptyDataError:
                    logger.warning(f"Error reading {file}: no data. Ignoring file.")
            # if module is not empty...
            if len(dfs) > 0:
                df = pd.concat(dfs)
                df['Folder'] = folder.name
                logger.debug(f'Folder {folder_name}: {len(df)} rows before removing duplicates.')
                df.drop_duplicates(subset=self.key_columns[dataset], keep='first', inplace=True)
                logger.info(f'Folder {folder_name}: {len(df)} rows loaded.')
                module_dfs.append(df)
            else:
                logger.info(f'Folder {folder_name}: no data to load in folder.')

        if len(module_dfs) > 0:
            df = pd.concat(module_dfs)
            logger.debug(f'All folders: {len(df)} rows before removing duplicates.')
            df.drop_duplicates(subset=self.key_columns[dataset], keep='first', inplace=True)
            logger.info(f'All folders   : {len(df)} rows loaded.')
            self.data[dataset] = df
        else:
            self.data[dataset] = pd.DataFrame()
        logger.info(f'Finished {dataset}: {len(self.data[dataset])} rows loaded.')

    @staticmethod
    def extract_file_stats(file: Path):
        file_stat = file.stat()
        return {'mtime': datetime.fromtimestamp(file_stat.st_mtime),
                'ctime': datetime.fromtimestamp(file_stat.st_ctime)}

    def download_all(self, download=True) -> NoReturn:
        """
        Downloads all files available in the Argus FTP site.
        :return: NoReturn
        """
        # little dirty thing to keep track whether we downloaded or not
        # it will be used later on on load_doc_tables() and load_data_table()
        self.download = download

        if not download:
            return

        logger.debug('Collecting file stats from local file store.')
        self.local_files_stats = {file.name: self.extract_file_stats(file)
                                  for file in self.root_dir.glob('**/*') if file.is_file()}

        logger.debug(f'Connecting to {self.argus_host} via FTP.')
        ftps = FTP(self.argus_host, user=ARGUS_FTP_USERNAME, passwd=ARGUS_FTP_PASSWORD)
        ftps.set_pasv(True)

        element = ('.', {'type': 'dir'})
        logger.info(f'Downloading all files from {self.argus_host}')

        self.recursive_download(ftps, element)
        logger.info(f'Download from {self.argus_host} finished.')

    def recursive_download(self, ftp_con, element) -> NoReturn:
        """
        Recursively downloads from FTP host, creating directories when needed.
        :param ftp_con: object: an ftplib.FTP object
        :param element: a tuple having a string in first position and a dictionary on the second.
        :return: NoReturn
        """

        if element[1]['type'] == 'dir':
            if element[0] != '.':
                dir_path = Path(element[0])
                local_path = self.root_dir / dir_path

                logger.debug(f'Directory: {dir_path} locally: {local_path}')
                try:
                    local_path.mkdir(parents=True)
                    logger.debug(f'{local_path} created.')
                except FileExistsError:
                    pass
            try:
                # FTP's mlsd lists files under a directory. It returns a list of tuples containing:
                # first element: file path
                # second element: dictionary with mtime, ctime and type
                for o in ftp_con.mlsd(path=element[0]):
                    attempt = 1
                    while attempt < 4:
                        try:
                            self.recursive_download(ftp_con, o)
                            break
                        except OSError:
                            sleep(3)
                            logger.exception(f'Failed to download {o} attempt #{attempt}')
                            attempt += 1
                    if attempt == 4:
                        logger.warning(f'Unable to download file {o}')
            except error_perm:
                logger.exception(f'Error while trying to read {element[0]}. Ignoring it for now.')

        else:
            # this block treats files, not directories
            file = PureWindowsPath(element[0])
            try:
                # the line below may raise ValueError if file is not relative to self.path_to_ignore
                local_file = self.root_dir / file.relative_to(self.path_to_ignore)

                mtime = datetime.strptime(element[1]['modify'], '%Y%m%d%H%M%S')
                ctime = datetime.strptime(element[1]['create'], '%Y%m%d%H%M%S')
                max_change_remote = max([ctime, mtime])

                local_stats = self.local_files_stats.get(local_file.name)

                max_change_local = max([local_stats['ctime'], local_stats['mtime']]) \
                    if local_stats is not None else self.const_min_date

                if local_stats is None or (max_change_remote > max_change_local):
                    logger.debug(f'Downloading file: {file} (remote: {max_change_remote} '
                                 f"local: {max_change_local if max_change_local != self.const_min_date else 'no file'}) "
                                 f' to: {local_file}')
                    with local_file.open('wb') as fp:
                        res = ftp_con.retrbinary(f'RETR {file.relative_to(self.path_to_ignore)}', fp.write)
                        if not res.startswith('226 Transfer complete'):
                            logger.debug('Download failed')
                            try:
                                local_file.unlink()
                                logger.debug(f'{local_file} removed.')
                            except FileNotFoundError:
                                pass
                        else:
                            self.downloaded_files.append(local_file)
                else:
                    logger.debug(f'Skipping {file}')
            except ValueError as e:
                logger.warning(f"File {file} not relative to {self.path_to_ignore}: skipping it.")

    def run(self, download=True, parallel_download=True):
        """
        Overriding to avoid using parent's version.
        :param download:
        :param parallel_download: no effect here.
        :return:
        """
        self.download_all(download)
        self.transform()
        self.upsert()

    @staticmethod
    def build_merge_query(df: pd.DataFrame,
                          key_columns: list,
                          schema: str,
                          temp_table: str,
                          final_table: str):
        """
        Build the merge query.
        @param: df: pd.DataFrame with data to load.
        @param: key_columns: list of key columns to consider.
        @param: temp_table: str with name of temporary table.
        @param: final_table: str with name of final table.
        :return: str with the merge query.
        """
        # columns to compare (ignore key columns)
        all_columns = list(df.columns)
        cols_to_compare = [col for col in all_columns if col not in key_columns]
        # columns to insert in the table
        cols_to_insert = all_columns
        # expression for matching keys
        merge_on = [f'target.[{key}] = source.[{key}]'
                    for key in key_columns]
        # expression for finding differences in non-key columns
        merge_difference = [f'target.[{col}] <> source.[{col}]'
                            for col in cols_to_compare]
        # update expression (on non-key columns)
        merge_update = [f'target.[{col}] = source.[{col}]'
                        for col in cols_to_compare]
        # insert expression
        merge_insert_cols = [f'[{col}]' for col in cols_to_insert]
        merge_insert_values = [f'source.[{col}]' for col in cols_to_insert]
        # final query
        query = (f'MERGE {schema}.{final_table} target \n'
                 f'USING {schema}.{temp_table} as source \n'
                 f"ON ({' AND '.join(merge_on)}) \n"
                 f"WHEN MATCHED AND ({' OR '.join(merge_difference)})\n"
                 f"THEN UPDATE SET {', '.join(merge_update)}, target.[date_modified] = GETDATE()\n"
                 f'WHEN NOT MATCHED \n'
                 f"THEN INSERT ({', '.join(merge_insert_cols)}, [date_created]) \n"
                 f"VALUES ({', '.join(merge_insert_values)}, GETDATE());")

        logger.debug(f'Merge query: {query}')
        return query

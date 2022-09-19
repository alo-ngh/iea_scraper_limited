import datetime
import logging
import socket
import time
import enum
import traceback
from calendar import monthrange
from typing import List, Dict, NoReturn
from unittest.mock import patch
from sqlalchemy import create_engine

import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.remote.webdriver import BaseWebDriver

from iea_scraper.core import factory
from iea_scraper.core.ts import mapping
from iea_scraper.core.utils import get_dimension_db_data, send_message
from iea_scraper.settings import WEBDRIVER_PATH, FILE_STORE_PATH, MAIL_RECIPIENT, EXT_DB_STR, MAIL_DEFAULT_SENDER

logger = logging.getLogger(__name__)


def scrape_and_report(provider_code: str,
                      source_code: str,
                      mail_from=MAIL_DEFAULT_SENDER,
                      mail_to=MAIL_RECIPIENT) -> Dict[str, object]:
    """
    Run the scraper and report the result.

    :param provider_code: the provider code. It should correspond to the package name of the scraper job.
    :param source_code: the source code. It should correspond to the module name of the scraper job.
    :param mail_from: sender email.
    :param mail_to: list of mail recipients.
    :return: a dictionary with information about the job execution.
    """
    status = dict()
    try:
        # The factory will load the module and instantiate scraper class accordingly
        ts_begin = datetime.datetime.now()
        job = factory.get_scraper_job(provider_code, source_code)
        logger.info(f"Start Job {job.title}")
        job.run()
        ts_end = datetime.datetime.now()

        status["timestamp"] = ts_begin.isoformat()
        status["process"] = job.title
        status["status"] = "OK"
        status["duration"] = str(ts_end - ts_begin)

    except Exception as e:
        ts_end = datetime.datetime.now()
        exception_description = traceback.format_exc()

        logger.exception(f"Error when running scraper {provider_code} - {source_code}")

        process = f"{provider_code} - {source_code}"

        status["timestamp"] = ts_begin.isoformat()
        status["process"] = process
        status["status"] = "ERROR"
        status["duration"] = str(ts_end - ts_begin)
        send_message(f"{process.upper()} ERROR", exception_description, 
                     mail_to=mail_to,  mail_from=mail_from)
    finally:
        return status


def send_report(report: List[Dict[str, object]],
                mail_subject: str,
                html_template: str,
                mail_from=MAIL_DEFAULT_SENDER,
                mail_to: List[str] = MAIL_RECIPIENT) -> NoReturn:
    """
    This sends a HTML report by e-mai
    :param report: a list of a dictionary containing a report of a scraper
    :param mail_subject: the mail subject
    :param html_template: the template in which we will glue the report
    :param mail_from: sender email.
    :param mail_to: list of recipients for the message. Defaults to settings.MAIL_RECIPIENT.
    :return: NoReturn
    """
    df = pd.DataFrame(report)
    df = df[["timestamp", "process", "status", "duration"]]
    table = df.to_html(index=False)
    html_content = html_template.replace("@@@TABLE@@@", table)
    send_message(subject=mail_subject,
                 mail_to=mail_to,
                 mail_from=mail_from,
                 html_content=html_content)


def enable_download_in_headless_chrome(driver, download_dir):
    """
    Enables download in headless mode in chrome.
    :param driver: the browser selenium driver
    :param download_dir: the directory where downloaded files will be saved.
    """
    # add missing support for chrome "send_command"  to selenium webdriver
    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')

    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
    command_result = driver.execute("send_command", params)


class BrowserType(enum.Enum):
    Chrome = 1
    Firefox = 2


def get_driver(headless=True, browser=BrowserType.Chrome) -> BaseWebDriver:
    """
    :param: headless: if True (default), browser opened in background without visible windows.
    :param: browser: defines the browser to use . Options are listed in BrowserType enumeration.
                     BrowserType.Chrome is the default.
    :return: an instance of selenium Chrome webdriver.
    """
    logger.debug(f'headless: {headless} browser choice: {browser} ')
    driver: BaseWebDriver = None

    if browser == BrowserType.Chrome:
        chrome_options = webdriver.chrome.options.Options()
        # uncomment line below to hide browser window
        if headless:
            chrome_options.add_argument("--headless")
        # low memory option
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("single-process")
        # enables clicks
        chrome_options.add_argument('window-size=1920x1480')
        # options to avoid being blocked by website
        chrome_options.add_argument("disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_argument("disable-infobars")
        chrome_options.add_argument(
            '--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')

        # change default download directory
        prefs = {"download.default_directory": str(FILE_STORE_PATH)}
        chrome_options.add_experimental_option("prefs", prefs)

        from iea_scraper.settings import CHROME_DRIVER
        driver_path = WEBDRIVER_PATH / CHROME_DRIVER

        driver = webdriver.Chrome(executable_path=str(driver_path),
                                  options=chrome_options)
        enable_download_in_headless_chrome(driver, str(FILE_STORE_PATH))
    elif browser == BrowserType.Firefox:
        # Creating firefox profile FirefoxProfile
        profile = webdriver.firefox.firefox_profile.FirefoxProfile()

        # Instructing firefox to use custom download location
        profile.set_preference("browser.download.folderList", 2)

        # Avoid asking
        profile.set_preference("browser.download.manager.showWhenStarting", False)

        # Setting custom download directory
        profile.set_preference("browser.download.dir", str(FILE_STORE_PATH))

        profile.set_preference("browser.helperApps.neverAsk.openFile",
                               "text/csv,"
                               "text/x-csv,"
                               "application/x-msexcel,"
                               "application/excel,"
                               "application/vnd.ms-excel,"
                               "application/vnd.microsoft.portable-executable")

        # Skipping Save As dialog box for types of files with their MIME
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk",
                               "text/csv,"
                               "text/x-csv,"
                               "application/x-msexcel,"
                               "application/excel,"
                               "application/vnd.ms-excel,"
                               "application/vnd.microsoft.portable-executable")

        profile.set_preference("browser.helperApps.alwaysAsk.force", False)
        profile.set_preference("browser.download.manager.focusWhenStarting", False)
        profile.set_preference("browser.download.manager.useWindow", False)
        profile.set_preference("browser.download.manager.showAlertOnComplete", False)

        # Creating FirefoxOptions to set profile
        option = webdriver.firefox.options.Options()
        option.add_argument("--private")
        if headless:
            option.add_argument("--headless")

        from iea_scraper.settings import FIREFOX_DRIVER
        driver_path = WEBDRIVER_PATH / FIREFOX_DRIVER

        # Launching browser
        driver = webdriver.firefox.webdriver.WebDriver(executable_path=driver_path,
                                                       options=option,
                                                       firefox_profile=profile)

    return driver


def wait_file(final_path, wait_step, timeout):
    """
    Wait for a file to be downloaded.
    :param final_path: the Path to the final file (when download is finished)
    :param wait_step: the time to sleep (in seconds) until new check
    :param timeout: timeout period in seconds
    """
    waiting = 0

    logger.info(f'Waiting for download of file {final_path}...')

    # calculate temporary filename during download
    temp_path = final_path.with_name(f"{final_path.name}.part")

    while waiting < timeout:
        if temp_path.is_file():
            time.sleep(wait_step)
            waiting += wait_step
        elif final_path.is_file():
            logger.info(f"File {final_path} available. Waited {waiting} seconds.")
            return
        else:
            time.sleep(wait_step)
            waiting += wait_step

    raise Exception("File not downloaded.")


def rename_file(current_path, new_path):
    """
    Renames a file, raises exception if it does not exists, overrides if existing target.
    :param current_path: path to the file
    :param new_path: new path
    """
    try:
        path = current_path.resolve(strict=True)
        path.replace(new_path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Error when renaming {str(current_path)} to {str(new_path)}: timeout exceeded and no file found.")


def to_detail_format(df):
    dico = df.to_dict('records')
    dico = [_separate_main_cols(x) for x in dico]
    return dico


def _separate_main_cols(x: dict) -> dict:
    dico = {'code': x['code'], 'category': x['category'], 'description': x['description']}
    del x['code'], x['category'], x['description']
    dico['json'] = x
    return dico


def convert_bbl_to_kbd(value: float, year: int, month: int):
    d_in_month = monthrange(year, month)[1]
    value = round(value / 1000 / d_in_month, 3)
    return value


def convert_m3_to_kbd(value: float, year: int, month: int):
    d_in_month = monthrange(year, month)[1]
    value = round(value * 6.289811 / 1000 / d_in_month, 3)
    return value


def map_dimension(df, column, dimension):
    to_map = pd.unique(df[column])
    _map = mapping(to_map, [dimension])
    _map = [m.get(dimension, 'None') for m in _map]
    _map = {k: v for k, v in zip(to_map, _map)}
    df[column] = df[column].map(_map)
    return df


def map_and_scale_unit(df, current, code, scale=1):
    df.loc[df['unit'] == current, 'value'] = df.loc[df['unit'] == current, 'value'] * scale
    df.loc[df['unit'] == current, 'unit'] = code
    return df


def convert_area_to_code(df, iso='iso_alpha_3', to_area=False):
    column = 'area'
    if to_area:
        column = 'to_area'
    areas = get_dimension_db_data('area')
    areas = pd.DataFrame(areas)[['code', iso]]
    df = pd.merge(df, areas, left_on=column, right_on=iso, how='left')
    del df[iso], df[column]
    df.loc[df['code'].isnull(), 'code'] = None
    df.rename(columns={'code': column}, inplace=True)
    return df


def ftp_fetch_file_through_http_proxy(host, user, password, remote_filepath, http_proxy, output_filepath):
    """
    Source: https://stackoverflow.com/questions/1293518/proxies-in-python-ftp-application

    This function let us to make a FTP RETR query through a HTTP proxy that does NOT support CONNECT tunneling.
    It is equivalent to: curl -x $HTTP_PROXY --user $USER:$PASSWORD ftp://$FTP_HOST/path/to/file
    It returns the 'Last-Modified' HTTP header value from the response.

    More precisely, this function sends the following HTTP request to $HTTP_PROXY:
        GET ftp://$USER:$PASSWORD@$FTP_HOST/path/to/file HTTP/1.1
    Note that in doing so, the host in the request line does NOT match the host we send this packet to.

    Python `requests` lib does not let us easily "cheat" like this.
    In order to achieve what we want, we need:
    - to mock urllib3.poolmanager.parse_url so that it returns a (host,port) pair indicating to send the request
      to the proxy
    - to register a connection adapter to the 'ftp://' prefix. This is basically a HTTP adapter but it uses the
      FULL url of the resource to build the request line, instead of only its relative path.

      :param host: FTP host server
      :param user: FTP user
      :param password: FTP password
      :param remote_filepath: FTP remote path to file
      :param http_proxy: HTTP proxy address in format 'server_addres:port'
      :param output_filepath: output file path
      :return: last-modified value from response header
    """
    url = f'ftp://{user}:{password}@{host}/{remote_filepath}'
    proxy_host, proxy_port = http_proxy.split(':')

    def parse_url_mock(url):
        return requests.packages.urllib3.util.url.parse_url(url)._replace(host=proxy_host, port=proxy_port,
                                                                          scheme='http')

    class FTPWrappedInFTPAdapter(requests.adapters.HTTPAdapter):
        def request_url(self, request, _):
            return request.url

    logger.debug(f'downloading {url} through HTTP proxy {http_proxy}')
    with open(output_filepath, 'w+b') as output_file, patch('requests.packages.urllib3.poolmanager.parse_url',
                                                            new=parse_url_mock):
        session = requests.session()
        session.mount('ftp://', FTPWrappedInFTPAdapter())
        response = session.get(url)
        response.raise_for_status()
        output_file.write(response.content)
        logger.debug(f'download of {url} completed')
        return response.headers['last-modified']


def split_url(url):
    """
    Splits an URL into its components: protocol, host, remote filepath.
    :param url: a string in format '<protocol>://<host>/<remote filepath>
    :return: (protocol, host, remote_filepath)
    """
    protocol, url_address = url.split('://')

    sep_index = url_address.find('/')
    host = url_address[:sep_index]
    remote_filepath = url_address[sep_index + 1:]

    return protocol, host, remote_filepath


class ProxySock(object):
    """
    Class that wraps a real socket and changes it to a HTTP tunnel
    whenever a connection is asked via the "connect" method.
    """

    def __init__(self, socket, proxy_host, proxy_port):

        # First, use the socket, without any change
        self.socket = socket

        # Create socket (use real one)
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port

        # Copy attributes
        self.family = socket.family
        self.type = socket.type
        self.proto = socket.proto

    def connect(self, address):

        # Store the real remote address
        (self.host, self.port) = address

        # Try to connect to the proxy
        for (family, socktype, proto, canonname, sockaddr) in socket.getaddrinfo(
                self.proxy_host,
                self.proxy_port,
                0, 0, socket.SOL_TCP):
            try:
                # Replace the socket by a connection to the proxy
                self.socket = socket.socket_formal(family, socktype, proto)
                self.socket.connect(sockaddr)

            except socket.error:
                if self.socket:
                    self.socket.close()
                self.socket = None
                continue
            break
        if not self.socket:
            raise socket.error

            # Ask him to create a tunnel connection to the target host/port
        host_port = f"{self.host}:{self.port}"
        proxy_stmt = (f"CONNECT {host_port} HTTP/1.1\r\n"
                      f"Host: {host_port}\r\n\r\n")
        logger.debug(f'Proxy statement: {proxy_stmt}')
        command: bytes = proxy_stmt.encode()
        self.socket.send(command)

        # Get the response
        resp = self.socket.recv(4096)

        # Parse the response
        parts = resp.split()

        # Not 200 ?
        if parts[1] != "200":
            raise Exception(f"Error response from Proxy server : {resp}")

    def __getattr__(self, name):
        """
        Automatically wrap methods and attributes for socket object.
        Wrap all methods of inner socket, without any change.
        """
        return getattr(self.socket, name)

    def getpeername(self):
        """Return the (host, port) of the actual target, not the proxy gateway"""
        return self.host, self.port


def setup_http_proxy(proxy_host, proxy_port: int):
    """
    Install a proxy, by changing the method socket.socket().
    """

    def socket_proxy(af, socktype, proto):
        """
        New socket constructor that returns a ProxySock, wrapping a real socket.
        """
        # Create a socket, old school :
        logger.debug('Creating formal socket')
        sock = socket.socket_formal(af, socktype, proto)

        # Wrap it within a proxy socket
        logger.debug(f'Wrapping formal socket within our proxy socket {proxy_host}, {proxy_port}')
        return ProxySock(
            sock,
            proxy_host,
            proxy_port)

    # Replace the "socket" method by our custom one
    socket.socket_formal = socket.socket
    socket.socket = socket_proxy


def reorganize_cci(db_schema: str, table_name: str) -> NoReturn:
    """
    This function runs a rebuild index statement a given table.
    Assumes cci index name as table name with a prefix cci_.
    :param: db_schema: str: the database schema of the table.
    :param: table_name: str: the table name.
    :return: NoReturn
    """
    cci_index: str = f'cci_{table_name}'
    logger.info(f'Reorganizing index {cci_index}...')
    cci_query = f'ALTER INDEX {cci_index} ON {db_schema}.{table_name} REORGANIZE;'
    logger.debug(f'Reorganize index statement: {cci_query}')
    engine = create_engine(EXT_DB_STR)
    logger.debug(f'Opening connection: {EXT_DB_STR}')
    with engine.begin() as con:
        con.execute(cci_query)
    logger.info(f'Reorganisation of {cci_index} finished successfully.')

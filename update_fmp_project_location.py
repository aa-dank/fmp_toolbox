import fmrest
import logging
import os
import psycopg2
import re
import requests
import warnings
import pandas as pd
from datetime import datetime
from creds import FILEMAKER_USERNAME, FILEMAKER_PASSWORD, DB_ACCOUNT_USERNAME, DB_ACCOUNT_PASSWORD

# Suppress SSL verification warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATABASE_NAME = "UCPPC"
PROJECTS_LAYOUT = "projects_table"
FILE_SERVER_LOCATION = "N:\\PPDO\\Records\\"
IDX_FIELD_NAME = "ID_Primary"
RECORD_ID_FIELD_NAME = "recordId" # Filemaker official index field name
PROJECT_NUMBER_FIELD = "ProjectNumber"
PROJECT_LOCATION_FIELD = "FileServerLocation"


DB_CONN_PARAMS = {
    'dbname': 'archives',
    'user': DB_ACCOUNT_USERNAME,
    'password': DB_ACCOUNT_PASSWORD,
    'host': '128.114.128.27',
    'port': '5432',
    'sslmode': 'require'
    #'sslrootcert': root_cert_filepath
}

# Create a custom logger
logger = logging.getLogger(__name__)

def setup_logger(log_to_file=False):
    
    # Set the log level to INFO
    logger.setLevel(logging.INFO)

    # Create handlers
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.INFO)

    # Create formatters and add it to handlers
    format_str = '%(asctime)s - %(levelname)s - %(message)s'
    c_format = logging.Formatter(format_str)
    c_handler.setFormatter(c_format)

    # Add handlers to the logger
    if not logger.handlers:
        logger.addHandler(c_handler)
        if log_to_file:
            f_handler = logging.FileHandler(f'logfile_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            f_handler.setLevel(logging.INFO)
            f_handler.setFormatter(c_format)
            logger.addHandler(f_handler)

setup_logger(log_to_file=True)


def split_path(path):
    """
    Split a path into a list of directories/files/mount points. It is built to accomodate Splitting both Windows and Linux paths
    on linux systems. (It will not necessarily work to process linux paths on Windows systems)
    :param path: The path to split.
    """

    def detect_filepath_type(filepath):
        """
        Detects the cooresponding OS of the filepath. (Windows, Linux, or Unknown)
        :param filepath: The filepath to detect.
        :return: The OS of the filepath. (Windows, Linux, or Unknown)
        """
        windows_pattern = r"^[A-Za-z]:\\(.+)$"
        linux_pattern = r"^/([^/]+/)*[^/]+$"

        if re.match(windows_pattern, filepath):
            return "Windows"
        elif re.match(linux_pattern, filepath):
            return "Linux"
        else:
            return "Unknown"
        
    def split_windows_path(filepath):
        """"""
        parts = []
        curr_part = ""
        is_absolute = False

        if filepath.startswith("\\\\"):
            # UNC path
            parts.append(filepath[:2])
            filepath = filepath[2:]
        elif len(filepath) >= 2 and filepath[1] == ":":
            # Absolute path
            parts.append(filepath[:2])
            filepath = filepath[2:]
            is_absolute = True

        for char in filepath:
            if (char == "\\"):
                if curr_part:
                    parts.append(curr_part)
                    curr_part = ""
            else:
                curr_part += char

        if curr_part:
            parts.append(curr_part)

        if not is_absolute and not parts:
            # Relative path with a single directory or filename
            parts.append(curr_part)

        return parts
    
    def split_other_path(path):

        allparts = []
        while True:
            parts = os.path.split(path)
            if parts[0] == path:  # sentinel for absolute paths
                allparts.insert(0, parts[0]) 
                break
            elif parts[1] == path:  # sentinel for relative paths
                allparts.insert(0, parts[1])
                break
            else:
                path = parts[0]
                allparts.insert(0, parts[1])
        return allparts

    path = str(path)
    path_type = detect_filepath_type(path)
    
    if path_type == "Windows":
        return split_windows_path(path)
    
    return split_other_path(path)


def db_path_to_user_path(db_path):
    """
    Converts the directories from the database to the user's file server path.
    """
    path_list = [FILE_SERVER_LOCATION] + split_path(db_path)
    return os.path.join(*path_list)


def retrieve_project_location_df(conn_params=DB_CONN_PARAMS):
    """
    Retrieve the project location data from the database.
    """
    # select all projects with a value in the location field
    q = """
    SELECT *
    FROM projects
    WHERE file_server_location IS NOT NULL
    """
    
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(q)
            return pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])



class ProjectChangingClerk:
    def __init__(self, url, db_name = DATABASE_NAME, username = FILEMAKER_USERNAME, password = FILEMAKER_PASSWORD):
        self.default_layout = PROJECTS_LAYOUT
        self.auto_login_attempts = 3
        self.fm_url = url
        self.fm_server = fmrest.Server(url,
                                       database=db_name,
                                       layout=self.default_layout,
                                       user=username,
                                       password=password,
                                       api_version='v1',
                                       verify_ssl=False)
        try: 
            self.fm_server.login()
            
        except Exception as e:
            # test url visibility
            try:
                requests.get(url, verify=False)
            except requests.exceptions.ConnectionError:
                # test internet connection
                try:
                    requests.get("https://www.google.com")
                except requests.exceptions.ConnectionError:
                    raise ValueError("No internet connection available?")
                
                raise ValueError("Could not connect to the FileMaker server at {}".format(url))
            

    def _auto_relogin_fm(self, server_method, *args, **kwargs):
        attempted = 0
        while True:
            try:
                return server_method(*args, **kwargs)
            except Exception as e:
                attempted += 1
                if attempted >= self.auto_login_attempts:
                    raise e
                if '952' in str(e):
                    self.fm_server.login()
                
                if '401' in str(e):
                    warnings.warn("FileMaker Server returned error 401, No records match the request")
                    return None
                else:
                    raise e
                
    def projects_queried_by_number(self, number):
        project_number_query = [{PROJECT_NUMBER_FIELD: str(number)}]
        project_foundset = self._auto_relogin_fm(self.fm_server.find, query=project_number_query)
        return project_foundset
    
    def projects_queried_by_id(self, project_id):
        project_id_query = [{IDX_FIELD_NAME: str(project_id)}]
        project_foundset = self._auto_relogin_fm(self.fm_server.find, query=project_id_query)
        return project_foundset
    
    def update_record(self, record):
        update_result = self._auto_relogin_fm(self.fm_server.edit, record=record)
        return update_result
    
    def make_change_to_project_data(self, project_record_id, change_dict):
        edit_results = self._auto_relogin_fm(self.fm_server.edit_record, record_id=project_record_id, field_data=change_dict)
        return edit_results


class UpdateStatus:
    __attrs__ = ['project_locations_updated', 'projects_not_in_fmp', 'projects_not_in_db', 'multiple_projects_in_fmp', 'ids_not_found_in_fmp', 'projects_modified']

    def __init__(self):
        self.project_locations_updated = 0
        self.projects_not_in_fmp = 0
        self.projects_not_in_db = 0
        self.multiple_projects_in_fmp = 0
        self.ids_not_found_in_fmp = 0
        # put project numbers in this list
        self.projects_modified = []
    
    def __str__(self):
        return f"Project Locations Updated: {self.project_locations_updated}\n" + \
               f"Projects Not Found in FileMaker: {self.projects_not_in_fmp}\n" + \
               f"Projects Not Found in Database: {self.projects_not_in_db}\n" + \
               f"Multiple Projects Found in FileMaker: {self.multiple_projects_in_fmp}\n" + \
               f"IDs Not Found in FileMaker: {self.ids_not_found_in_fmp}\n"
    



if __name__ == "__main__":
    #fmp_server_url = r'https://pp-prd-fm-2.au.ucsc.edu/'
    fmp_server_url = r'https://pp-dev-fm-1.au.ucsc.edu/'
    clerk = ProjectChangingClerk(url=fmp_server_url)
    status = UpdateStatus()
    project_location_df = retrieve_project_location_df()

    for idx, row in project_location_df.iterrows():
        try:
            proj_number = row['number']
            proj_db_location = row['file_server_location']
            fmp_proj_num_foundset = clerk.projects_queried_by_number(proj_number)
            if not fmp_proj_num_foundset:
                logger.warning(f"Project {proj_number} not found in FileMaker.")
                status.projects_not_in_fmp += 1
                continue

            # turn foundset into df, get the row exactly matching the project number
            fmp_proj_num_df = fmp_proj_num_foundset.to_df()
            # PROJECT_NUMBER_FIELD column cast to string and stripped of whitespace
            fmp_proj_num_df[PROJECT_NUMBER_FIELD] = fmp_proj_num_df[PROJECT_NUMBER_FIELD].astype(str).str.strip()
            fmp_proj_num_row = fmp_proj_num_df[fmp_proj_num_df[PROJECT_NUMBER_FIELD] == proj_number]
            if fmp_proj_num_row.empty:
                logger.warning(f"Project {proj_number} not found in FileMaker.")
                status.projects_not_in_fmp += 1
                continue
            
            if len(fmp_proj_num_row) > 1:
                logger.warning(f"Multiple projects found in FileMaker for project number {proj_number}.")
                status.multiple_projects_in_fmp += 1
                continue

            # get the project id from the row
            proj_id = fmp_proj_num_row[IDX_FIELD_NAME].iloc[0]
            fmp_proj_id_foundset = clerk.projects_queried_by_id(proj_id)
            
            # check only one project was found
            if not fmp_proj_id_foundset or len(fmp_proj_id_foundset.to_df()) > 1:
                logger.warning(f"Project {proj_number} not found in FileMaker using ID, {proj_id}.")
                status.ids_not_found_in_fmp += 1
                continue
            
            user_location = db_path_to_user_path(proj_db_location)
            for record in fmp_proj_id_foundset:
                record_id = record[RECORD_ID_FIELD_NAME]
                change_dict = {PROJECT_LOCATION_FIELD: user_location}
                updated = clerk.make_change_to_project_data(record_id, change_dict)
                if updated:
                    status.project_locations_updated += 1
                    status.projects_modified.append(proj_number)
                break

        except Exception as e:
            logger.error(f"Error processing project {proj_number}: {e}")
            continue

    logger.info("Update process completed. Status summary:")
    logger.info(str(status))
    logger.info(f"Modified projects: {', '.join(map(str, status.projects_modified))}")
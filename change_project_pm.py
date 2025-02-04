import fmrest
import requests
import warnings
import pandas as pd
import os
import sys

from datetime import datetime

from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text
from creds import FILEMAKER_USERNAME, FILEMAKER_PASSWORD

warnings.filterwarnings('ignore')  # suppress all warnings

DATABASE_NAME = "UCPPC"

# Layout for accessing project data
PROJECTS_LAYOUT = "projects_table"

# Layout for accessing people data
PEOPLE_LAYOUT = "people_table"

# Field names for the project and people data
PEOPLE_FIRST_NAME_FIELD = "NameFirst"
PEOPLE_LAST_NAME_FIELD = "NameLast"
PEOPLE_ACTIVE_FIELD = "Active_c" # Field name for the active status of the person
PROJECT_NAME_FIELD = "ProjectName"
PROJECT_NUMBER_FIELD = "ProjectNumber"
PROJECT_NOTES_FIELD = "Notes"
PROJECT_PM_ID_FIELD = "ID_ProjectManager" # Field name for the project manager id which is a foreign key to the people table

# Field name for the primary keys used across the database
IDX_FIELD_NAME = "ID_Primary" # custom field name for the primary key used in table relationships
RECORD_ID_FIELD_NAME = "recordId" # Filemaker official index field name

def dataframe_to_rich_table(df: pd.DataFrame, include_index: bool = True, 
                            row1_style: str = "deep_sky_blue4", row2_style: str = "grey74") -> Table:
    table = Table(show_lines=True)  # Add horizontal lines between rows
    
    # Add columns
    if include_index:
        table.add_column("Index", justify="right", style="cyan", no_wrap=True)
    for column in df.columns:
        table.add_column(column)
    
    # Add rows with alternating styles
    for idx, row in df.iterrows():
        row_values = []
        if include_index:
            row_values.append(str(idx))
        row_values.extend([str(row[column]) for column in df.columns])
        
        # Apply alternating row styles
        row_style = row1_style if idx % 2 == 0 else row2_style
        table.add_row(*row_values, style=row_style)
    
    return table

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

    def projects_queried_by_number_df(self, number):
        project_number_query = [{PROJECT_NUMBER_FIELD: str(number)}]
        project_foundset = self._auto_relogin_fm(self.fm_server.find, query=project_number_query)
        if not project_foundset:
            return pd.DataFrame()
        
        return project_foundset.to_df()
    
    def get_project_by_idx(self, idx):
        project_query = [{IDX_FIELD_NAME: str(idx)}]
        project_foundset = self._auto_relogin_fm(self.fm_server.find, query=project_query)
        if not project_foundset:
            return None
        return project_foundset[0]
    
    def get_most_recent_pms(self, n):
        """
        Retrieves the n most recent projects (by database idx) to get the most recent project managers.
        Returns a list of sets of project manager names and project manager ids.
        """
        sort_specs = [{'fieldName': IDX_FIELD_NAME, 'sortOrder': 'descend'}]
        retrieved_project_set = self._auto_relogin_fm(self.fm_server.get_records, limit=n, sort=sort_specs)
        recent_projects_df = retrieved_project_set.to_df()
        recent_pm_ids = recent_projects_df[PROJECT_PM_ID_FIELD].dropna().astype(str).unique()
        recent_pm_query = [{IDX_FIELD_NAME: int(pm_id)} for pm_id in recent_pm_ids if pm_id]
        recent_pm_foundset = self._auto_relogin_fm(self.fm_server.find, query=recent_pm_query, request_layout=PEOPLE_LAYOUT)
        if not recent_pm_foundset:
            raise ValueError("No project managers found to select from.")
        recent_pm_df = recent_pm_foundset.to_df()

        # for any set of rows with the same ID_Primary, keep only the active row (1 in PEOPLE_ACTIVE_FIELD)
        recent_pm_df = recent_pm_df.sort_values([IDX_FIELD_NAME, PEOPLE_ACTIVE_FIELD], ascending=[True, False]).drop_duplicates(subset=IDX_FIELD_NAME)

        return recent_pm_df[[IDX_FIELD_NAME, PEOPLE_FIRST_NAME_FIELD, PEOPLE_LAST_NAME_FIELD]]
    
    def get_pm_by_id(self, pm_id):
        pm_query = [{IDX_FIELD_NAME: pm_id}]
        pm_foundset = self._auto_relogin_fm(self.fm_server.find, query=pm_query, request_layout=PEOPLE_LAYOUT)
        if not pm_foundset:
            raise ValueError("No project manager found with the id {}".format(pm_id))
        
        pm = pm_foundset[0]
        return pm

    def get_project_pm(self, project_number = "", project_idx = ""):
        
        if not project_number and not project_idx:
            raise ValueError("Either project_number or project_idx must be provided.")
        
        pm_idx = None
        if project_number:
            projects_foundset_df = self.projects_queried_by_number_df(project_number)
            if projects_foundset_df.empty:
                return None
            
            if len(projects_foundset_df) > 1:
                raise ValueError("Multiple projects found with the same project number{}".format(project_number))
            
            pm_idx = projects_foundset_df[PROJECT_PM_ID_FIELD].iloc[0]
        
        else:
            project_query = [{IDX_FIELD_NAME: str(project_idx)}]
            project_foundset = self._auto_relogin_fm(self.fm_server.find, query=project_query)
            if not project_foundset:
                return None
            pm_idx = project_foundset[0][PROJECT_PM_ID_FIELD]
        
        pm = self.get_pm_by_id(pm_idx)
        return pm
    
    def make_change_to_project_data(self, project_record_id, change_dict):
        edit_results = self._auto_relogin_fm(self.fm_server.edit_record, record_id=project_record_id, field_data=change_dict)
        return edit_results

    
class PmChangeService:
    def __init__(self, url):
        self.clerk = ProjectChangingClerk(url)
        self.project_to_change = None
        self.target_pm = None
        self.console = Console()
        # adjust this number to change the number of projects to look back for project managers
        self.project_lookback_num = 100

    def introduction(self):
        intro_text = Text(f"This service allows you to change the project manager of a project to one of the PMs used in the previous {self.project_lookback_num} projects. Use ctrl-c to exit at any time.", style="bold blue")
        self.console.print(intro_text)

    def _ask(self, prompt_str):
        try:
            prompt_text = Text(prompt_str, style="bold yellow")
            return Prompt.ask(prompt_text, console=self.console)
        except KeyboardInterrupt:
            self.console.print("\nExiting program", style="bold red")
            sys.exit()
    
    def _generate_notes_str(self, old_pm_name, new_pm_name):
        change_note_template = "Project PM {} {} on {}"
        datestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if old_pm_name:
            change_str = f"changed from {old_pm_name} to"
        else:
            change_str = "set to"
        return change_note_template.format(change_str, new_pm_name, datestamp)


    def elicit_project_to_change(self):

        project_idx = None
        user_table_cols = [PROJECT_NAME_FIELD, PROJECT_NUMBER_FIELD]
        project_number = None
        project_foundset_df = None
        while True:
            try:
                project_number = self._ask("Enter the project number for the project to change" )
                project_foundset_df = self.clerk.projects_queried_by_number_df(project_number)
                if  project_foundset_df.empty:
                    self.console.print("No projects found with the project number {}. Please enter a valid project number.".format(project_number),
                                        style="bold red")
                    continue

            except Exception as e:
                # failing to find any projects creates an error:
                # fmrest.exceptions.FileMakerError: FileMaker Server returned error 401, No records match the request
                # this is caught here and the user is asked to enter a valid project number
                if type(e) == fmrest.exceptions.FileMakerError:
                    self.console.print("No projects found with the project number {}. Please enter a valid project number.".format(project_number),
                                        style="bold red")
                    continue
                else:
                    raise e
            
            if project_foundset_df.empty:
                self.console.print("No projects found with the project number {}. Please enter a valid project number.".format(project_number),
                                    style="bold red")
                continue

            break


        #if the foundset is more than one but less than foundset_size_limit, show the foundset and ask the user to choose one 
        if len(project_foundset_df) > 1:
            
            user_table_df = project_foundset_df
            self.console.print("Multiple projects found with using the project number {}. Please choose one from this table".format(project_number),
                                style="bold blue")
            self.console.print(dataframe_to_rich_table(user_table_df[user_table_cols]))
            while str(project_idx) not in project_foundset_df[IDX_FIELD_NAME].astype(str).values:
                user_table_idx = self._ask("Enter the index of the project to change")
                
                # if the entered index is not a valid index of the foundset dataframe, ask the user to enter a valid index
                if not (user_table_idx.isdigit() and int(user_table_idx) in user_table_df.index):
                    self.console.print("Invalid index. Please enter a valid index.", style="bold red")
                    continue
                project_idx = user_table_df.loc[int(user_table_idx), IDX_FIELD_NAME]
        else:
            project_idx = project_foundset_df[IDX_FIELD_NAME].iloc[0]

        project = self.clerk.get_project_by_idx(project_idx)
        self.project_to_change = project
        self.console.clear()
        return project
    
    def elicit_new_pm(self):
        pm_choices_df = self.clerk.get_most_recent_pms(self.project_lookback_num)
        
        # combine the first and last names of the project managers: Last, First
        combine_names = lambda row: f"{row[PEOPLE_LAST_NAME_FIELD]}, {row[PEOPLE_FIRST_NAME_FIELD]}"
        pm_choices_df["Name"] = pm_choices_df.apply(combine_names, axis=1)

        #sort dataframe on name
        pm_choices_df = pm_choices_df.sort_values("Name").reset_index(drop=True)
        self.console.print("Select the new project manager from the following table", style="bold blue")
        self.console.print(dataframe_to_rich_table(pm_choices_df[[IDX_FIELD_NAME, "Name"]], include_index=False))
        pm_idx = None
        while pm_idx not in pm_choices_df[IDX_FIELD_NAME].astype(str).values:
            pm_idx = self._ask("Enter the id (ID_primary) of the new project manager")
            if pm_idx not in pm_choices_df[IDX_FIELD_NAME].astype(str).values:
                print("Invalid index. Please enter a valid index.")
        pm = self.clerk.get_pm_by_id(pm_idx)
        self.target_pm = pm
        self.console.clear()
        return pm
    
    def change_project_pm(self):
        if not self.project_to_change:
            self.elicit_project_to_change()
        change_messaage_str = "Changing project manager for project: {}, {}".format(self.project_to_change[PROJECT_NUMBER_FIELD], self.project_to_change[PROJECT_NAME_FIELD])
        self.console.print(change_messaage_str,
                           style="bold blue")
        old_pm = None
        old_pm_id = self.project_to_change[PROJECT_PM_ID_FIELD]
        if old_pm_id:
            old_pm = self.clerk.get_pm_by_id(old_pm_id)
            old_pm_name = f"{old_pm[PEOPLE_FIRST_NAME_FIELD]} {old_pm[PEOPLE_LAST_NAME_FIELD]}"
            self.console.print(f"Current project manager: {old_pm_name}", style="bold blue")
        else:
            self.console.print(f"No previous project manager found for {self.project_to_change[PROJECT_NUMBER_FIELD]}.", style="bold blue")
            old_pm_name = None
        self.console.print("\n\n")
        
        if not self.target_pm:
            # border terminal clear operations to prevent overlap
            os.system("cls" if os.name == "nt" else "clear")
            self.elicit_new_pm()
            os.system("cls" if os.name == "nt" else "clear")
        
        proj_notes = self.project_to_change[PROJECT_NOTES_FIELD]
        target_pm_name = f"{self.target_pm[PEOPLE_FIRST_NAME_FIELD]} {self.target_pm[PEOPLE_LAST_NAME_FIELD]}"
        new_notes = self._generate_notes_str(old_pm_name, target_pm_name)
        if proj_notes:
            new_notes = proj_notes + "\n" + new_notes
        changes_dict = {PROJECT_NOTES_FIELD: new_notes, PROJECT_PM_ID_FIELD: self.target_pm[IDX_FIELD_NAME]}
        edit_results = self.clerk.make_change_to_project_data(self.project_to_change[RECORD_ID_FIELD_NAME], changes_dict)

        completion_text = Text(self._generate_notes_str(old_pm_name, target_pm_name), style="bold green")
        self.console.print(completion_text)
        self.console.print("\n\n")
        return edit_results



if __name__ == "__main__":
    while True:
        #fmp_server_url = r'https://pp-prd-fm-2.au.ucsc.edu/'
        fmp_server_url = r'https://pp-dev-fm-1.au.ucsc.edu/'
        service = PmChangeService(fmp_server_url)
        service.introduction()
        service.change_project_pm()










# Import required libraries
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
import os

# Read MNav templates CSV file
mnav_templates = pd.read_csv(r'/content/sample_data/MNav Templates.csv')

# Read past validation report
past_report = pd.read_csv(r'/content/sample_data/Past Report.csv')

# Read JIRA tickets (Collected using query)
jira_tickets = pd.read_csv(r'/content/sample_data/UCJIRA.csv', low_memory=False)

# Read JIRA tickets (Collected from past validation report)
past_report_jira = pd.read_csv(r'/content/sample_data/Past Report JIRA.csv')

# Top 80 templates list
templates_list = [
    "AEPUniversalTemplateProvider", "AESIndianaTemplateProvider", "AESOhioTemplateProvider",
    "AlliantEnergyTemplateProvider", "AmerenIPTemplateProvider", "AmericanWaterTemplateProvider",
    "ArizonaPublicServiceTemplateProvider", "AtmosEnergyTemplateProvider", "BaltimoreGasAndElectricTemplateProvider",
    "BencoElectricCooperativeMNTemplateProvider", "BerkeleyElectricCooperativeTemplateProvider",
    "CalpineEnergySolutionsCATemplateProvider", "CanalDeIsabelIIESPTemplateProvider", "CenterPointEnergyTemplateProvider",
    "CentralMaineTemplateProvider", "CityOfAugustaGATemplateProvider", "CityofAustinTXTemplateProvider",
    "CityOfColumbiaSCTemplateProvider", "CityOfKansasTemplateProvider", "CityOfMankatoMNTemplateProvider",
    "CityOfTallahasseeTemplateProvider", "ColumbiaGasTemplateProvider", "ComEdTemplateProvider",
    "ConstellationEnergyManagerTemplateProvider", "ConsumerEnergyTemplateProvider", "DelmarvaPowerTemplateProvider",
    "DirectEnergyTemplateProvider", "DominionEnergyTemplateProvider", "DTEEnergyTemplateProvider",
    "DukeEnergyNewTemplateProvider", "DuquesneLightTemplateProvider", "EnbridgeGasCanadaTemplateProvider",
    "EngiePowerUKTemplateProvider", "EntergyCorporationTemplateProvider", "EvergyTemplateProvider",
    "EverSourceTemplateProvider", "FirstEnergyCorporationTemplateProvider",
    "FloridaPowerAndLightCompanyMyAccountTemplateProvider", "FourCountyElectricPowerAssociationTemplateProvider",
    "GallatinDepartmentofElectricityTNTemplateProvider", "GeorgiaNaturalGasTemplateProvider", "GesternovaESPTemplateProvider",
    "HuntsvilleUtilitiesTemplateProvider", "HydroOneTemplateProvider", "HydroQuebecTemplateProvider",
    "LansingBoardOfWaterAndLightMITemplateProvider", "LosAngelesDeptOfWaterAndPowerTemplateProvider",
    "LouisvilleGasAndElectricTemplateProvider", "MemphisTemplateProvider", "MidAmericanEnergyTemplateProvider",
    "NationalGridBusinessAccountTemplateProvider", "NewRiverLightAndPowerNCTemplateProvider", "NicorGasTemplateProvider",
    "NRGEnergyTemplateProvider", "NVEnergyTemplateProvider", "OklahomaGasAndElectricTemplateProvider",
    "PacificGasAndElectricTemplateProvider", "PECONewTemplateProvider", "PepcoTemplateProvider",
    "PiedmontNaturalGasTemplateProvider", "PowerNewMexicoTemplateProvider", "PPLElectricUtilitiesTemplateProvider",
    "PublicServiceEnterpriseGroupTemplateProvider", "PugetSoundEnergyTemplateProvider", "ReliantEnergyTemplateProvider",
    "RepublicServicesOnlineTemplateProvider", "RhodeIslandEnergyTemplateProvider", "SanDiegoGasAndElectricTemplateProvider",
    "SCETemplateProvider", "SouthernCaliforniaEdisonTemplateProvider", "SouthernCompanyTemplateProvider", "SpireTemplateProvider",
    "SymmetryEnergySolutionsMyCESTemplateProvider", "TecoTemplateProvider", "TownOfBooneNCTemplateProvider",
    "WashingtonSuburbanSanitaryCommissionTemplateProvider", "WasteManagementTemplateProvider", "WeEnergiesTemplateProvider",
    "WisconsinPublicServiceCorporationTemplateProvider", "XcelEnergyPremisesAsMultiMeterScenarioBillTemplateProvider"
]

# Scan and get ZIP folders in directory
def scan_directory_for_zips(directory_path):
    zip_folders = {}
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.zip'):
                folder_name = os.path.splitext(file)[0]
                zip_folders[folder_name] = os.path.join(root, file)
    return zip_folders

# Enter directory path
directory_path = r'/content/sample_data'

# Call the function to scan the directory for zip folders
zip_folders = scan_directory_for_zips(directory_path)

def custom_sort(key):
    month_order = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                   'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
    month_date = key.split('_')[2:]  # Extract the month and date components
    month = month_date[0]  # Extract month
    date = month_date[1]   # Extract date
    return -month_order[month], -int(date)  # Negative values for both month and date for descending order

# Sort the dictionary based on keys using custom sorting function
zip_folders = dict(sorted(zip_folders.items(), key=lambda x: custom_sort(x[0])))

print(f'List of {len(zip_folders)} Credentials report uploaded:')

# Print sorted dictionary in descending order
for folder_name, value in zip_folders.items():
    print(f'{folder_name}')

# Loop into zip_folders
def read_zip_files(zip_folders):
    # Create a dictionary to store dataframes
    dataframe_dict = {}
    for folder_name, folder_path in zip_folders.items():
        # Create an empty DataFrame to store the contents of the zip folder
        df = pd.DataFrame()

        # Read the CSV file into a dataframe
        df = pd.read_csv(folder_path, sep = '\t', compression = 'zip', low_memory=False)
        print(f'Successfully accessed and read csv file inside ',folder_name, ' folder')

        # Store the dataframe in the dictionary with the ZIP folder as the key
        dataframe_dict[folder_name] = df

    return dataframe_dict

# Call the function to read the contents of the zip folders and store them in a DataFrame dictionary
dataframes = read_zip_files(zip_folders)

# Define a function to apply conditions
def apply_conditions(df, condition_funcs, *args, **kwargs):
    for func in condition_funcs:
        if func.__name__ == 'condition_MNavtemplates':
            df = func(df, *args, **kwargs)
        else:
            df = func(df)
    return df

# Define condition functions to filter DataFrame
# Remove data of 'ATest' organization from DataFrame
def condition_ATest(df):
    return df[df['organization_name'] != 'ATest']

# Remove unsubscribed credentials from DataFrame
def condition_Subscribed(df):
    return df[df['subscribed'] == True]

# Remove credentials with credential_status as 'OK' and 'ACTION_REQUIRED' from DataFrame
def condition_PENDING(df):
    return df[df['credential_status'] == 'PENDING']

# Remove credentials with MNav templates
def remove_rows_with_templates(df, mnav_templates):
    # Merge df with mnav_templates on 'acquisition_template_id'
    merged_df = pd.merge(df, mnav_templates, how='left', left_on='acquisition_template_id', right_on='templateName')

    # Filter out rows where 'templateName' is not null (i.e., template details match)
    filtered_df = merged_df[merged_df['templateName'].isnull()].copy()  # Create a copy

    # Drop the 'templateName' column from the copied DataFrame
    filtered_df.drop(columns=['templateName'], inplace=True)

    return filtered_df

# Define a function for condition related to MNav templates
def condition_MNavtemplates(df, mnav_templates):
    return remove_rows_with_templates(df, mnav_templates)

# Assuming dataframe_dict is your dictionary
latest_report = next(iter(dataframes))
print(f"Latest Credentials Report: {latest_report}")

dataframes[latest_report].columns = dataframes[latest_report].columns.str.strip()

# List of conditions
conditions = [condition_ATest, condition_Subscribed, condition_PENDING, condition_MNavtemplates]

# Create filtered main DataFrame with required variables
main_df = apply_conditions(dataframes[latest_report], conditions, mnav_templates)
main_df = main_df[['organization_name', 'provider_alias', 'acquisition_template_id', 'system_status', 'completion_status', 'completion_status_detail', 'onboarding_classification', 'credential_id', 'credential_status']]

# Merge the DataFrames based on the 'credential_id' column
main_df = pd.merge(main_df, past_report[['credential_id', 'Final Status', 'Ticket', 'Aging Assignee', 'Aging RCA', 'Aging RCA Category', 'Aging Ticket', 'ETA', 'Aging RCA Description', 'Reason', 'Action Owner', 'Accurate/In Accurate']], on='credential_id', how='left')

# Convert Created column to datetime type
jira_tickets['Created'] = pd.to_datetime(jira_tickets['Created'], format='%d/%b/%y %I:%M %p')

# Sort DataFrame by Created column in ascending order
jira_tickets = jira_tickets.sort_values(by='Created', ascending=True)

# Create new columns in main_df
main_df['Catalyst'] = ''
main_df['Ticket Status'] = ''

# Convert credential_id to string in main_df for comparison
main_df['credential_id'] = main_df['credential_id'].astype(str)

# Iterate over rows of jira_tickets
for index, row in jira_tickets.iterrows():
    credential_ids = str(row['Custom field (CredentialIds)']).split(',')
    for credential_id in credential_ids:
        # Check if credential_id exists in main_df
        if credential_id in main_df['credential_id'].values:
            # Update Catalyst column in main_df
            main_df.loc[main_df['credential_id'] == credential_id, 'Catalyst'] = row['Custom field (Catalyst (migrated))']
            # Update Ticket Status column in main_df
            main_df.loc[main_df['credential_id'] == credential_id, 'Ticket Status'] = row['Status']
            # Update Ticket column in main_df
            main_df.loc[main_df['credential_id'] == credential_id, 'Ticket'] = row['Issue key']

for index, row in main_df.iterrows():
    ticket_key = row['Ticket']
    matching_row = past_report_jira[past_report_jira['Issue key'] == ticket_key]
    if not matching_row.empty:
        main_df.at[index, 'Catalyst'] = matching_row['Custom field (Catalyst (migrated))'].iloc[0]
        main_df.at[index, 'Ticket Status'] = matching_row['Status'].iloc[0]

# Define function to update Final Status based on Catalyst value
def update_final_status(row):
    if row['Catalyst'] in ['Bill_Change', 'Bill_Change_History', 'Bill_Change_Major', 'Zendesk']:
        return 'Extraction_Failure'
    elif row['Catalyst'] in ['Website_Change', 'Website_Change_History', 'NEW_URL_MAPPING', 'Feature_Implement']:
        return 'Navigation_Failure'
    elif row['Catalyst'] == 'NAVIGATION_EVENT_TRACKING' and row['Final Status'] not in ['Tracking_Not_Possible', 'Website_Down']:
        return 'Event_Tracking'
    else:
        return row['Final Status']

# Apply function to update Final Status column for rows where Ticket starts with "FS"
mask = main_df['Ticket'].str.startswith('FS') & ~main_df['Ticket'].isnull()
main_df.loc[mask, 'Final Status'] = main_df[mask].apply(update_final_status, axis=1)

# List of values to convert to lowercase
values_to_replace = ['Temporary_Job_Failure', 'Ok', 'OK', 'Action_Required', 'ACTION_REQUIRED', 'Job_Still_Running', 'Autosolve_Captcha_Failure', 'AutoSolve_Captcha_Failure']

# Filter the DataFrame to include only rows with specified values
filtered_df = main_df[main_df['Final Status'].isin(values_to_replace)]

# Count the occurrences of specified values in the filtered DataFrame
counts = filtered_df['Final Status'].value_counts()

# Display the counts
print("Counts of specified values:")
print(counts)

# Convert specified values to lowercase and replace them with None (NULL)
main_df.loc[main_df['Final Status'].isin(values_to_replace), 'Final Status'] = None

# Define a function to apply the logic to create the new column
def calculate_volume_template(template_id):
    if template_id in templates_list:
        return 'Top 80 Template'
    else:
        return 'Remaining Template'

# Apply the function to create the new column
main_df['Template Category'] = main_df['acquisition_template_id'].apply(calculate_volume_template)

# Create new column 'History Required' based on conditions
main_df['History Required'] = main_df['organization_name'].apply(lambda x: 'No' if x == 'SimpleBills Test' else 'Yes')

# Define a function to assign support levels
def assign_support_level(org_name):
    if org_name in ['Arbor', 'EnergyCap', 'Google POC Trial', 'Mantis Innovation', 'My Utility Cabinet']:
        return 'Gold'
    elif org_name in ['RealPage', 'Siemens']:
        return 'Platinum'
    else:
        return 'Silver'

# Apply the function to create the Support Level column
main_df['Support Level'] = main_df['organization_name'].apply(assign_support_level)

# Create an empty DataFrame to store the output
output_df = pd.DataFrame()

# Assign output DataFrame with main DataFrame values
output_df = main_df

# Loop through the dictionary starting from the second key-value pair
for key, df in list(dataframes.items())[1:]:
    # List of conditions
    conditions = [condition_ATest, condition_Subscribed, condition_MNavtemplates]

    # Create filtered main DataFrame with required variables
    df = apply_conditions(df, conditions, mnav_templates)

# Loop through the dictionary starting from the second key-value pair
for key, df in list(dataframes.items())[1:]:
    # Filter the DataFrame for credential_id values that exist in the first DataFrame
    filtered_df = df[df['credential_id'].isin(main_df['credential_id'])]

    # Extract only the credential_status column
    filtered_df = filtered_df[['credential_id', 'credential_status']]

    # Create a DataFrame containing credential_ids not found in the current DataFrame
    missing_ids = main_df[~main_df['credential_id'].isin(filtered_df['credential_id'])]
    missing_status = pd.DataFrame({'credential_id': missing_ids['credential_id'], 'credential_status': 'NEW'})

    # Concatenate the filtered DataFrame with the DataFrame containing missing credential_ids
    filtered_df = pd.concat([filtered_df, missing_status], ignore_index=True)

    # Rename the credential_status column to the key name
    filtered_df.rename(columns={'credential_status': key}, inplace=True)

    # Append the filtered DataFrame to the output DataFrame
    if output_df.empty:
        output_df = filtered_df
    else:
        output_df = pd.merge(output_df, filtered_df, on='credential_id', how='left')

    print(f'Appended {key} DataFrame, with output DataFrame')

# Getting columns list for calculating Aging
zip_folders_keys = list(zip_folders.keys())
zip_folders_keys[0] = 'credential_status'
print(f'Columns for calculating aging: \n{zip_folders_keys}')

# Define a list of columns to count 'PENDING' from
columns_to_count = zip_folders_keys

# Define a custom function to count 'PENDING' until encountering 'OK', 'ACTION_REQUIRED', or 'NEW' in specified columns
def count_pending_until_break(row):
    pending_count = 0
    for col in columns_to_count:
        status = row[col]
        if status == 'PENDING':
            pending_count += 1
        elif status in ['OK', 'ACTION_REQUIRED', 'NEW']:
            break
    return pending_count

# Apply the custom function to each row to calculate the 'Aging' count
output_df['Aging'] = output_df.apply(count_pending_until_break, axis=1)

# Define a function to map Aging values to categories
def map_aging_category(value):
    if value <= 2:
        return 'a. < 3 Days'
    elif value <= 5:
        return 'b. >= 3 Days'
    elif value <= 10:
        return 'c. > 5 Days'
    elif value <= 15:
        return 'd. > 10 Days'
    elif value <= 19:
        return 'e. > 15 Days'
    else:
        return 'f. > 20 Days'

# Apply the function to create the 'Aging Category' column
output_df['Aging Category'] = output_df['Aging'].apply(map_aging_category)

# Count the occurrences of each category in the 'Aging Category' column
category_counts = output_df['Aging Category'].value_counts()

# Sort the category counts by index in alphabetical order
category_counts = category_counts.sort_index()

# Extracting category labels and counts
categories = category_counts.index
counts = category_counts.values

# Create a colormap
cmap = cm.coolwarm

# Create a bar plot
plt.figure(figsize=(8, 6))
bars = plt.bar(category_counts.index, category_counts.values, color=cmap(category_counts.values / max(category_counts.values)))

# Adding titles and labels
plt.title('Distribution of Aging Categories')
plt.xlabel('Aging Category')
plt.ylabel('Count')

# Rotating x-axis labels for better readability if needed
plt.xticks(rotation=45, ha='right')

# Annotate each bar with its count
for i, count in enumerate(category_counts.values):
    plt.text(bars[i].get_x() + bars[i].get_width() / 2, bars[i].get_height() + 0.05, str(count), ha='center', va='bottom')

# Show plot
plt.tight_layout()
plt.show()

# Info of output DataFrame
print(f"Total number of Credential Ids for Validation: ", len(output_df), "\n")

# Add new columns with default values
output_df['Retry Assignee'] = ''
output_df['Retry Status'] = ''

# Reorder the columns if needed
output_df = output_df[['organization_name', 'Support Level', 'provider_alias', 'acquisition_template_id', 'Template Category',
                       'system_status', 'completion_status', 'completion_status_detail', 'onboarding_classification',
                       'History Required', 'credential_id', 'credential_status', 'Aging', 'Aging Category', 'Final Status', 'Ticket',
                       'Catalyst', 'Ticket Status', 'Retry Assignee', 'Retry Status', 'Aging Assignee', 'Aging RCA', 'Aging RCA Category',
                       'Aging Ticket', 'ETA', 'Aging RCA Description', 'Reason', 'Action Owner', 'Accurate/In Accurate']]

# Export retry assignment output DataFrame as an CSV file
output_df.to_csv('retry_output.csv', index=False)
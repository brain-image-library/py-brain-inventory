import requests
import pandas as pd
import json
from datetime import date


def today():
    """
	Get today's snapshot of Brain Image Library.
	"""

    server = "https://download.brainimagelibrary.org/inventory/daily/reports/"
    filename = "today.json"

    response = requests.get(f"{server}{filename}")

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON data into a dictionary
        data = json.loads(response.text)
        data = pd.DataFrame(data)
        return data

    else:
        print("Error: Failed to fetch JSON data")
        return pd.DataFrame()


def __get_number_of_datasets(df):
    return len(df)


def __get_completeness_score(df):
    return df["score"].sum() / len(df)


def __is_reachable(url):
    response = requests.get(url)

    if response.status_code == 200:
        return True
    else:
        return False


def __get_metadata_version(df):
    return df["metadata_version"].value_counts().to_dict()


def __get_contributor(df):
    return df["contributor"].value_counts().to_dict()


def __get_affilation(df):
    return df["affiliation"].value_counts().to_dict()

def __get_awards(df):
    return df["award_number"].unique()

def __get_award_number(df):
    return df["award_number"].value_counts().to_dict()


def __get_species(df):
    return df["species"].value_counts().to_dict()


def __get_cnbtaxonomy(df):
    return df["cnbtaxonomy"].value_counts().to_dict()


def __get_samplelocalid(df):
    return df["samplelocalid"].value_counts().to_dict()


def __get_genotype(df):
    return df["genotype"].value_counts().to_dict()


def __get_generalmodality(df):
    return df["generalmodality"].value_counts().to_dict()


def __get_technique(df):
    return df["technique"].value_counts().to_dict()


def __get_locations(df):
    return df["locations"].value_counts().to_dict()

def __get_contributors(df):
    """
    This returns an array of contributor names from the contributorname column.
    """
    return df["contributorname"].unique()


def __get_project_names(df):
	'''
	Gets the unique list of project names.

    Input: dataframe
    Output: list 
    '''
	return df['project'].unique()

def __get_list_of_projects(df):
    '''
    Get the list of names for unique projects

    Input parameter: dataframe
    Output:  list of projects
    '''
    
    return df['project'].unique().to_dict()

def __get_number_of_projects(df):
    '''
    Get the number of unique projects

    Input parameter: dataframe
    Output:  number of projects
    '''
    
    return len(df['project'].unique())

def report():
    # Get today's date
    tdate = date.today()

    # Convert date to string
    tdate = tdate.strftime("%Y-%m-%d")

    df = today()

    report = {}
    report["date"] = tdate
    report["number_of_datasets"] = __get_number_of_datasets(df)
    report["completeness_score"] = __get_completeness_score(df)
    report["metadata_version"] = __get_metadata_version(df)
    report["contributor"] = __get_contributor(df)
    report["affiliation"] = __get_affilation(df)
    report["award_number"] = __get_award_number(df)
    report["species"] = __get_species(df)
    report["cnbtaxonomy"] = __get_cnbtaxonomy(df)
    report["samplelocalid"] = __get_samplelocalid(df)
    report["genotype"] = __get_genotype(df)
    report["generalmodality"] = __get_generalmodality(df)
    report["technique"] = __get_technique(df)
    report["locations"] = __get_locations(df)
    report["is_reachable"] = df["URL"].apply(__is_reachable)

    return report

#Need to combine the universities so the pie chart shows a single university's total samples under the same area. 
#right now there is one area of the pie chart that says 'Allen Institute for Brain Science' and 'Allen Instititute for Brain Science ' (with a space!)
#we need it to recognize that the Allen Institue for Brain Science has contributed a total number of samples equal to the sum of both those areas in the pie chart  
Allen = df[df['affiliation'] == 'Allen Institute for Brain Science' ]
print(len(Allen))

#^we set a variable 'Allen' equal to the dataframe limited to the rows where it said 'Allen Institute for Brain Science and asked for the count of those rows
#below, we did the same thing but with rows that had Allen with a space

Allen_with_space = df[df['affiliation'] == 'Allen Institute for Brain Science ' ]
print(len(Allen_with_space))

accurate_Allen = len(Allen) + len(Allen_with_space)
print(accurate_Allen)

del affiliations['Allen Institute for Brain Science ']
print(affiliations)

#Now we can add the counts we had before. (We deleted Allen with a space, but we still have the number of Allen with a space)
#reassign the Allen Institute for Brain Science variable to the actual total number (4715)

affiliations['Allen Institute for Brain Science'] = len(Allen) + len(Allen_with_space)
print(affiliations)

#Now we need to do the same thing with UCLA
# 1) limit the dictionary to ones that read 'University of California, Los Angeles' and the one that says 'University of California, Los Angeles (UCLA)'

No_UCLA = affiliations['University of California, Los Angeles']
print(No_UCLA)

UCLA_present = affiliations['University of California, Los Angeles (UCLA)']
print(UCLA_present)

# 2) Now we found the counts of both. We have to set the college equal to the sum of the category with no UCLA and the catgory with UCLA and delete the one we don't want.

#del affiliations['University of California, Los Angeles (UCLA)']
#print(len(affiliations))

accurate_Uni = affiliations['University of California, Los Angeles'] + affiliations['University of California, Los Angeles (UCLA)']
print(accurate_Uni)

del affiliations['University of California, Los Angeles (UCLA)']
print(affiliations)

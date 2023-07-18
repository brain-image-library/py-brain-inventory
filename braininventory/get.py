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

import pandas as pd
import urllib.request
import geoip2.database
from geopy.geocoders import Nominatim
import folium
'''
Import modules that will be used to create the world map, find coordinates of affiliations, and 
''' 

url = 'https://download.brainimagelibrary.org/inventory/daily/reports/today.json'
file_path, _ = urllib.request.urlretrieve(url)
df = pd.read_json(file_path)
df
"""
Geopy - Input: University Output: Address, lat, lon
Folium - visual map creator 
"""

map = folium.Map()

from tqdm import tqdm

for index, row in tqdm(df.iterrows()):
  city = row['city']
  lat = row['lat']
  lon = row['lng']
  folium.Marker([lat, lon], popup=city).add_to(map)
map.save('project_map.html')

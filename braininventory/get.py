import requests
import pandas as pd
import json
from datetime import date
import squarify
import matplotlib.pyplot as plt


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


def __get_genotypes(df):
    """
    Write documentation here.
    """
    return df["genotype"].unique()


def __get_genotype_frequency(df):
    """
    Write documentation here.
    """
    return df["genotypes"].value_counts().to_dict()


def __get_generalmodality(df):
    return df["generalmodality"].value_counts().to_dict()


def __get_techniques(df):
    """
    Write documentation here.
    """
    return df["technique"].unique().to_dict()


def techniques_frequency(df):
    """
    Write documentation here.
    """
    return df["technique"].value_counts().to_dict()


def __get_locations(df):
    return df["locations"].value_counts().to_dict()


def __get_contributors(df):
    """
    This returns an array of contributor names from the contributorname column.
    """
    return df["contributorname"].unique()


def __get_list_of_projects(df):
    """
    Get the list of names for unique projects

    Input parameter: dataframe
    Output:  list of projects
    """

    return df["project"].unique().to_dict()


def __get_number_of_projects(df):
    """
    Get the number of unique projects

    Input parameter: dataframe
    Output:  number of projects
    """

    return len(df["project"].unique())


def get_projects_treemap(df):
    """
    Created a code for the visualization of projects frequency 

    Input: project values 
    Output: treemap graph of projects frequency
    """

    df = df["project"].value_counts().to_dict()
    sizes_list = list(df.values())
    names_list = list(df.keys())
    squarify.plot(sizes_list)

    filename = f'treemap-projects-{datetime.now().strftime("%Y%m%d")}.png'
    plt.savefig("path/to/save/plot.png")


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

    # plots
    get_projects_treemap(df)

    return report

# Creates a segmented bar graph that shows the proportion of general modalities over the years. 
# Dropped the null values (no creation dates)
df[~df['creation_date'].isnull()]
df['dates'] = pd.to_datetime(df['creation_date'])
df['year'] = df['dates'].dt.year
df = df.dropna(subset=['year'])
df['year'] = df['year'].astype(int)
grouped = df.groupby(df['dates'].dt.year)

import matplotlib.pyplot as plt

grouped = df.groupby(['year', 'generalmodality']).size().reset_index(name='count')

pivot_df = grouped.pivot(index='year', columns='generalmodality', values='count').fillna(0)

pivot_df.plot(kind='bar', stacked=True)

plt.title('General Modalities')
plt.ylabel('Number of Datasets')
plt.xlabel('Year')
plt.xticks(rotation=45)
plt.tight_layout()

plt.show()

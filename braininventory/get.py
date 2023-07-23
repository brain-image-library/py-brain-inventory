import requests
import pandas as pd
import json
from datetime import date
from pandarallel import pandarallel

pandarallel.initialize(nb_workers=8, progress_bar=True)


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


def __is_it_reachable(url):
    response = requests.get(url)

    if response.status_code == 200:
        return True
    else:
        return False


def __are_reachable(df):
    print("Computing what datasets are reachable")
    df["is_reachable"] = df["URL"].parallel_apply(__is_it_reachable)
    return df["is_reachable"].sum() / len(df)


def __get_metadata_version(df):
    return df["metadata_version"].value_counts().to_dict()


def __get_genotypes(df):
    return df["genotype"].value_counts().to_dict()


def __get_modalities(df):
    return df["generalmodality"].value_counts().to_dict()


def __get_techniques(df):
    return df["technique"].value_counts().to_dict()


def __get_award_numbers(df):
    return df["award_number"].value_counts().to_dict()


def __get_affiliations(df):
    return df["affiliation"].value_counts().to_dict()


def __get_contributors(df):
    return df["contributorname"].value_counts().to_dict()


def __get_projects(df):
    return df["project"].value_counts().to_dict()


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
    # report['are_reachable'] = __are_reachable(df)
    report["genotypes"] = __get_genotypes(df)
    report["modalities"] = __get_modalities(df)
    report["award_numbers"] = __get_award_numbers(df)
    report["tecniques"] = __get_techniques(df)
    report["affiliations"] = __get_affiliations(df)
    report["contributors"] = __get_contributors(df)
    report["projects"] = __get_projects(df)

    return report

import json
from datetime import date

import humanize
import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sb
import squarify
from pandarallel import pandarallel

pandarallel.initialize(nb_workers=8, progress_bar=True)
import matplotlib.pyplot as plt
import squarify


def get_random_sample(df):
    """
    Returns a random sample from the dataframe from a dataset with non-zero score.

    Input: dataframe
    Output:open the json file that was located in datasets Brain Image Library dataframe
    """

    isNotZero = df[df["score"] != 0.0]  # only have files with the correct data
    randomRow = isNotZero.iloc[
        random.randint(0, len(isNotZero))
    ]  # select a random row of random index
    jsonFileLink = randomRow.json_file.replace(
        "/bil/data", "https://download.brainimagelibrary.org", 1
    )  # create the link
    result = requests.get(jsonFileLink)

    return result.json()


def __get_lable_dict(name_lst):
    """
    input: a list of University names
    output: a dictionary with the names as keys and abbreviations that include the first letter of each University name
    """
    return {
        uni_name: "".join(word[0].upper() for word in uni_name.split())
        for uni_name in name_lst
    }


def __get_general_modality_treemap(df):
    """
    input: dataframe
    output: tree map that displays the frequencies of "generalmodality" that occur in dataframe
    """
    modality_counts = df["generalmodality"].value_counts().to_dict()
    plt.figure(figsize=(14, 10))
    values = list(modality_counts.values())
    name = list(modality_counts.keys())
    abbrName = __get_lable_dict(name)
    colors = sb.color_palette("ocean", len(values))

    num_labels = len(df.keys())
    print(num_labels)

    ax = squarify.plot(sizes=values, color=colors, label=abbrName.values(), alpha=0.8)
    ax.axis("off")
    ax.invert_xaxis()
    ax.set_aspect("equal")

    legend_patches = [plt.Rectangle((0, 0), 1, 1, fc=color) for color in colors]
    plt.legend(
        legend_patches, name, loc="upper left", bbox_to_anchor=(1, 1), fontsize="medium"
    )
    plt.show()


def __get_pretty_size_statistics(df):
    """
    Pretty version of __get_size_statistics

    Input: dataframe
    Output: list of strings
    """
    size_stats = __get_size_statistics(df)

    return [
        humanize.naturalsize(size_stats[0]),
        humanize.naturalsize(size_stats[1]),
        humanize.naturalsize(size_stats[2]),
        humanize.naturalsize(size_stats[3]),
    ]


def __get_size_statistics(df):
    """
    Helper method that returns size statistics from size column.

    Input: dataframe
    Output: list of numbers
    """

    min = df["size"].min()
    max = df["size"].max()
    average = df["size"].mean()
    std = df["size"].std()

    return [min, max, average, std]


def today():
    """
    Get today's snapshot of Brain Image Library.
    """

    # if file can be found locally, then load from disk
    directory = "/bil/data/inventory/daily/reports/"
    if Path(directory).exists():
        data = json.loads(f"{directory}/today.json")
        data = pd.DataFrame(data)
        return data

    # else get file from the web
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


def __clean_affiliations(df):
    # Need to combine the universities so the pie chart shows a single university's total samples under the same area.
    # right now there is one area of the pie chart that says 'Allen Institute for Brain Science' and 'Allen Instititute for Brain Science ' (with a space!)
    # we need it to recognize that the Allen Institue for Brain Science has contributed a total number of samples equal to the sum of both those areas in the pie chart
    Allen = df[df["affiliation"] == "Allen Institute for Brain Science"]

    # ^we set a variable 'Allen' equal to the dataframe limited to the rows where it said 'Allen Institute for Brain Science and asked for the count of those rows
    # below, we did the same thing but with rows that had Allen with a space

    Allen_with_space = df[df["affiliation"] == "Allen Institute for Brain Science "]

    accurate_Allen = len(Allen) + len(Allen_with_space)

    del affiliations["Allen Institute for Brain Science "]

    # Now we can add the counts we had before. (We deleted Allen with a space, but we still have the number of Allen with a space)
    # reassign the Allen Institute for Brain Science variable to the actual total number (4715)

    affiliations["Allen Institute for Brain Science"] = len(Allen) + len(
        Allen_with_space
    )

    # Now we need to do the same thing with UCLA
    # 1) limit the dictionary to ones that read 'University of California, Los Angeles' and the one that says 'University of California, Los Angeles (UCLA)'

    No_UCLA = affiliations["University of California, Los Angeles"]

    UCLA_present = affiliations["University of California, Los Angeles (UCLA)"]

    # 2) Now we found the counts of both. We have to set the college equal to the sum of the category with no UCLA and the catgory with UCLA and delete the one we don't want.

    # del affiliations['University of California, Los Angeles (UCLA)']
    # print(len(affiliations))

    accurate_Uni = (
        affiliations["University of California, Los Angeles"]
        + affiliations["University of California, Los Angeles (UCLA)"]
    )

    del affiliations["University of California, Los Angeles (UCLA)"]
    return affiliations


def __get_affiliation_frequency(df):
    """
    Get affiliation frequency.

    Input: dataframe
    Output: a frequency dictionary
    """
    return df["affiliation"].value_counts().to_dict()


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


def __are_reachable(df):
    print("Computing what datasets are reachable")
    df["is_reachable"] = df["URL"].parallel_apply(__is_it_reachable)
    return df["is_reachable"].sum() / len(df)


def __get_metadata_version(df):
    return df["metadata_version"].value_counts().to_dict()


def __get_genotypes(df):
    return df["genotype"].value_counts().to_dict()


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


def __get_genotype_frequency(df):
    """
    Write documentation here.
    """
    return df["genotypes"].value_counts().to_dict()


def __get_generalmodality(df):
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


def __get_project_names(df):
    """
          Gets the unique list of project names.

    Input: dataframe
    Output: list
    """
    return df["project"].unique()


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


def __get__percentage_of_metadata_version_1(df):
    """
    Get the percentage/ratio of metadata version 1 from all datasets

    Input: dataframe
    Output: an integer
    """
    return len(df[df["metadata_version"] == 1]) / len(df)


def report():
    # Get today's date
    tdate = date.today()

    # Convert date to string
    tdate = tdate.strftime("%Y%m%d")

    # Get today's data info
    df = today()

    # Build report
    report = {}
    report["date"] = tdate
    report["number_of_datasets"] = __get_number_of_datasets(df)
    report["number_of_project"] = __get_number_of_projects(df)
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
    report["percentage_of_version_1"] = __get__percentage_of_metadata_version_1(df)
    # report["is_reachable"] = df["URL"].apply(__is_reachable)

    # plots
    get_projects_treemap(df)

    return report

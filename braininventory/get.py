import calendar
import json
import random
from datetime import date
from pathlib import Path

import folium
import geoip2.database
import humanize
import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sb
import squarify
from geopy.geocoders import Nominatim
from pandarallel import pandarallel

pandarallel.initialize(nb_workers=8, progress_bar=True)
import matplotlib.pyplot as plt
import squarify


def get_species(df):
    """
    Calculate the number of unique species in the given DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame containing the data.

    Returns:
        int: The number of unique species present in the 'species' column of the DataFrame.
    """
    return len(df["species"].unique())


def __get_md5_coverage(df):
    """
    Calculate the average MD5256 coverage from a DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame containing the 'md5_coverage' column.

    Returns:
        float: The average MD5 coverage.

    Raises:
        KeyError: If the 'sha256_coverage' column is not present in the DataFrame.
    Example:
        >>> data = {'md5_coverage': [0.75, 0.82, 0.91, 0.68, 0.79]}
        >>> df = pd.DataFrame(data)
        >>> __get_md5_coverage(df)
        0.79
    """
    average = df["md5_coverage"].mean()
    return average


def __get_sha256_coverage(df):
    """
    Calculate the average SHA256 coverage from a DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame containing the 'sha256_coverage' column.

    Returns:
        float: The average SHA256 coverage.

    Raises:
        KeyError: If the 'sha256_coverage' column is not present in the DataFrame.

    Example:
        >>> data = {'sha256_coverage': [0.75, 0.82, 0.91, 0.68, 0.79]}
        >>> df = pd.DataFrame(data)
        >>> __get_sha256_coverage(df)
        0.79
    """
    average = df["sha256_coverage"].mean()
    return average


def __create_general_modality_plot(df):
    """
    Create a bar plot to visualize the frequency of general modalities.

    This function takes a pandas DataFrame as input and generates a bar plot to display the frequency
    of different general modalities in the 'generalmodality' column of the DataFrame.

    Parameters:
        df (pandas.DataFrame): The input DataFrame containing the 'generalmodality' column.

    Returns:
        None: The function generates a bar plot but does not return any value. The plot is saved as an
            image file with a filename in the format 'general-modality-YYYYMMDD.png', where 'YYYYMMDD'
            represents the current date in year-month-day format.
    """

    modality_counts = df["generalmodality"].value_counts()

    plt.figure(figsize=(10, 6))
    color = plt.cm.tab20c.colors

    ax = modality_counts.plot(kind="bar", color=color, edgecolor="black")
    plt.xlabel("General Modality", fontsize=12)
    plt.ylabel("Frequency", fontsize=12)
    plt.title("Frequency of General Modality", fontsize=14)
    plt.xticks(rotation=45, ha="right", fontsize=10)
    plt.yticks(fontsize=10)

    plt.tight_layout()

    filename = f'general-modality-{datetime.now().strftime("%Y%m%d")}.png'
    plt.savefig(filename)


def get_random_sample(df):
    """
    Retrieve a random JSON file from the DataFrame.

    This function takes a pandas DataFrame as input and filters it to keep only the rows that have
    a non-zero 'score' value. From the filtered rows, it selects a random row using the 'random.randint()'
    function. It then generates a valid link to the JSON file by replacing '/bil/data' with
    'https://download.brainimagelibrary.org' in the 'json_file' column of the selected row. The function
    performs an HTTP GET request to download the JSON file from the generated link, and it returns the
    JSON data as a Python dictionary.

    Parameters:
        df (pandas.DataFrame): The input DataFrame containing the 'score' and 'json_file' columns.

    Returns:
        dict: A Python dictionary containing the JSON data retrieved from a random row's JSON file.
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
    Generate a dictionary of labeled names based on a list of university names.

    This function takes a list of university names `name_lst` as input and generates a dictionary
    where each university name is the key, and the corresponding value is a labeled version of the name.
    The labeled version consists of the uppercase initials of each word in the university name.

    Parameters:
    -----------
    name_lst : list
        A list of strings representing university names.

    Returns:
    --------
    dict
        A dictionary where the keys are university names, and the values are their corresponding labeled versions.

    Note:
    -----
    The function takes each university name in the input list `name_lst`, splits it into individual words,
    and extracts the first letter of each word as uppercase. It then concatenates these uppercase letters
    to create a labeled version of the university name. The result is returned as a dictionary where the
    original university names are the keys and the labeled versions are the values.
    """
    return {
        uni_name: "".join(word[0].upper() for word in uni_name.split())
        for uni_name in name_lst
    }


def __create_general_modality_treemap(df):
    """
    Create a treemap visualization for the general modality data.

    This function takes a pandas DataFrame as input and generates a treemap visualization based on
    the counts of different modalities in the 'generalmodality' column of the DataFrame. The function
    utilizes the Squarify library to create the treemap.

    Parameters:
        df (pandas.DataFrame): The input DataFrame containing the 'generalmodality' column.

    Returns:
        None: The function generates a treemap and saves it as an image file, but it does not return any value.
            The treemap is saved with a filename in the format 'treemap-general-modality-YYYYMMDD.png', where
            'YYYYMMDD' represents the current date in year-month-day format.
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

    filename = f'treemap-general-modality-{datetime.now().strftime("%Y%m%d")}.png'
    plt.savefig(filename)


def __get_pretty_size_statistics(df):
    """
    Get human-readable size statistics from the DataFrame.

    This method takes a pandas DataFrame as input and calculates size statistics using the '__get_size_statistics()'
    method. The statistics include the minimum, maximum, mean, and total size of the data in the DataFrame.

    Parameters:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        list: A list containing human-readable size statistics in the following order:
            - Human-readable minimum size.
            - Human-readable maximum size.
            - Human-readable mean size.
            - Human-readable total size.
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
    Calculate basic size statistics from the DataFrame.

    This method takes a pandas DataFrame as input and calculates basic size statistics, including the minimum,
    maximum, mean, and standard deviation of the 'size' column in the DataFrame.

    Parameters:
        df (pandas.DataFrame): The input DataFrame containing the 'size' column.

    Returns:
        list: A list containing the size statistics in the following order:
            - Minimum size.
            - Maximum size.
            - Mean size.
            - Standard deviation of sizes.
    """
    min = df["size"].min()
    max = df["size"].max()
    average = df["size"].mean()
    std = df["size"].std()

    return [min, max, average, std]


def get_jsonFile(df):
    """
    Extract and format the date from the DataFrame.

    This function takes a pandas DataFrame as input and extracts the creation date information from
    the associated JSON file using the 'get_jsonFile()' function. It then processes the date information,
    reformatting it into the 'year-day-month' format (e.g., '2023-24-Jul').

    Parameters:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        str: A string representing the formatted date in the 'year-day-month' format.
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


def get_date(df):
    """
    Get unique genotypes from the DataFrame.

    This function takes a pandas DataFrame as input and extracts the unique values from the 'genotype'
    column of the DataFrame. It returns an array containing the unique genotypes present in the 'genotype'
    column.

    Parameters:
        df (pandas.DataFrame): The input DataFrame containing the 'genotype' column.

    Returns:
        numpy.ndarray: An array containing the unique genotypes found in the 'genotype' column.
    """

    jsonFile = get_jsonFile(
        df
    )  # get the jsonFile information with get_jsonFile() function
    dateList = jsonFile["creation_date"].split()  # get creation_date
    mntList = dict(
        (month, index) for index, month in enumerate(calendar.month_abbr) if month
    )  # month abbr to number
    yr = dateList[4]  # get year
    mnt = mntList[dateList[1]]  # get month
    day = dateList[2]  # get day
    return f"{yr}-{day}-{mnt}"  # format in year-day-month


def __get_affiliations(df):
    return df["affiliation"].value_counts().keys()


def today():
    """
    Get the daily inventory report data for today.

    This function attempts to fetch the daily inventory report data either from a local file
    or from the web. If the data is available locally, it loads the data from the disk.
    Otherwise, it makes a request to the server to fetch the data in JSON format, which is then
    parsed into a pandas DataFrame.

    Returns:
    --------
    pandas DataFrame
        A pandas DataFrame containing the daily inventory report data for today. If the data
        cannot be fetched, an empty DataFrame is returned.

    Note:
    -----
    The function first checks if the daily report data is available locally by looking in the
    directory "/bil/data/inventory/daily/reports/". If the data exists locally, it is loaded from
    the disk into a pandas DataFrame. If the data is not available locally, the function makes a
    request to the web server "https://download.brainimagelibrary.org/inventory/daily/reports/"
    to fetch the "today.json" file. If the request is successful (status code 200), the JSON data
    is parsed into a dictionary and converted into a pandas DataFrame. If the request fails, an
    error message is printed, and an empty DataFrame is returned.
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
    """
    Clean and aggregate the affiliation data in the input DataFrame.

    This function takes a pandas DataFrame `df` as input and performs some data cleaning
    and aggregation operations on the "affiliation" column. It addresses issues where there
    may be multiple variations of the same affiliation name (e.g., with or without trailing
    spaces) and combines the counts of such variations to have a single representation in
    the final result.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing affiliation information.

    Returns:
    --------
    pandas Series or DataFrame
        A pandas Series or DataFrame with cleaned and aggregated affiliation data,
        depending on the specific operations performed on the input DataFrame.

    Note:
    -----
    The function aims to address cases where there are multiple variations of the same affiliation name
    and combines their counts to provide a more accurate representation in the final result. The specific
    operations and the final result may vary depending on the input DataFrame's content.
    """
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
    Get a dictionary containing the count of occurrences of each unique affiliation.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "affiliation" column. The result is returned as a dictionary, where
    the keys represent unique affiliations, and the values represent the count of occurrences
    for each affiliation.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "affiliation" with affiliation information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique affiliations, and the values represent
        the count of occurrences for each affiliation.

    Note:
    -----
    The input DataFrame `df` should have a column named "affiliation" containing categorical data
    representing different affiliations. The function counts the occurrences of each unique affiliation
    and returns the result as a dictionary.
    """
    return df["affiliation"].value_counts().to_dict()


def __get_number_of_datasets(df):
    """
    Get the total number of datasets in the input DataFrame.

    This function takes a pandas DataFrame `df` as input and returns the total number of datasets
    present in the DataFrame.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing dataset information.

    Returns:
    --------
    int
        The total number of datasets in the DataFrame.

    Note:
    -----
    The function simply returns the length of the DataFrame `df`, which corresponds to the total number
    of datasets present in the DataFrame.
    """
    return len(df)


def __get_completeness_score(df):
    """
    Calculate the completeness score for a given DataFrame.

    The completeness score is computed as the sum of the values in the "score" column
    divided by the total number of rows in the DataFrame.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "score" which holds numerical values.

    Returns:
    --------
    float
        The completeness score of the DataFrame.

    Note:
    -----
    The input DataFrame `df` should have a column named "score" containing numerical values.
    The function calculates the completeness score as the sum of the values in the "score" column
    divided by the total number of rows in the DataFrame.
    """
    return df["score"].sum() / len(df)


def __is_reachable(url):
    """
    Check if the given URL is reachable and returns a boolean value indicating its reachability.

    Parameters:
    -----------
    url : str
        The URL to be checked for reachability.

    Returns:
    --------
    bool
        True if the URL is reachable (status code 200), False otherwise.

    Note:
    -----
    This function uses the `requests` library to send an HTTP GET request to the specified URL.
    It checks the status code of the response, and if the status code is 200, it returns True,
    indicating that the URL is reachable. Otherwise, it returns False to indicate that the URL
    is not reachable or encountered an error.
    """
    response = requests.get(url)

    if response.status_code == 200:
        return True
    else:
        return False


def __are_reachable(df):
    """
    Compute the reachability of datasets specified in the DataFrame.

    This method checks the reachability of each dataset URL in the "URL" column of the DataFrame
    by invoking the __is_it_reachable function in parallel using the pandas parallel_apply method.
    The reachability of each URL is stored in a new column named "is_reachable" in the DataFrame.
    The method then returns the ratio of reachable datasets to the total number of datasets.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "URL" with dataset URLs to be checked.

    Returns:
    --------
    float
        The ratio of reachable datasets to the total number of datasets.

    Note:
    -----
    This function requires the __is_it_reachable function to be defined separately, which checks
    the reachability of a single URL. The DataFrame `df` should have a column named "URL" with the
    dataset URLs to be checked for reachability. The method computes the ratio of reachable datasets
    to the total number of datasets and returns this value as a float.
    """
    print("Computing what datasets are reachable")
    df["is_reachable"] = df["URL"].parallel_apply(__is_it_reachable)
    return df["is_reachable"].sum() / len(df)


def __get_metadata_version(df):
    """
    Get a dictionary containing the count of occurrences of each unique metadata version.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "metadata_version" column. The result is returned as a dictionary,
    where the keys represent unique metadata versions, and the values represent the count
    of occurrences for each metadata version.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "metadata_version" with metadata version information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique metadata versions, and the values
        represent the count of occurrences for each metadata version.


    Note:
    -----
    The input DataFrame `df` should have a column named "metadata_version" containing
    categorical data representing different versions of metadata. The function counts
    the occurrences of each unique metadata version and returns the result as a dictionary.
    """

    return df["metadata_version"].value_counts().to_dict()


def __get_genotypes(df):
    """
    Get a dictionary containing the count of occurrences of each unique genotype.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "genotype" column. The result is returned as a dictionary, where the
    keys represent unique genotypes, and the values represent the count of occurrences for
    each genotype.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "genotype" with genotype information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique genotypes, and the values represent
        the count of occurrences for each genotype.

    Note:
    -----
    The input DataFrame `df` should have a column named "genotype" containing categorical data
    representing different genotypes. The function counts the occurrences of each unique genotype
    and returns the result as a dictionary.
    """
    return df["genotype"].value_counts().to_dict()


def __get_contributor(df):
    """
    Get a dictionary containing the count of occurrences of each unique contributor.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "contributor" column. The result is returned as a dictionary, where
    the keys represent unique contributors, and the values represent the count of occurrences
    for each contributor.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "contributor" with contributor information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique contributors, and the values represent
        the count of occurrences for each contributor.

    Note:
    -----
    The input DataFrame `df` should have a column named "contributor" containing categorical data
    representing different contributors. The function counts the occurrences of each unique contributor
    and returns the result as a dictionary.
    """
    return df["contributor"].value_counts().to_dict()


def __get_affilation(df):
    """
    Get a dictionary containing the count of occurrences of each unique affiliation.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "affiliation" column. The result is returned as a dictionary, where
    the keys represent unique affiliations, and the values represent the count of occurrences
    for each affiliation.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "affiliation" with affiliation information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique affiliations, and the values represent
        the count of occurrences for each affiliation.

    Note:
    -----
    The input DataFrame `df` should have a column named "affiliation" containing categorical data
    representing different affiliations. The function counts the occurrences of each unique affiliation
    and returns the result as a dictionary.
    """
    return df["affiliation"].value_counts().to_dict()


def __get_awards(df):
    """
    Get an array containing unique award numbers.

    This function takes a pandas DataFrame `df` as input and returns an array containing
    the unique values found in the "award_number" column. Each value in the array represents
    a unique award number associated with the data in the DataFrame.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "award_number" with award number information.

    Returns:
    --------
    numpy.ndarray
        An array of unique award numbers.

    Note:
    -----
    The input DataFrame `df` should have a column named "award_number" containing categorical data
    representing different award numbers. The function extracts the unique values from the "award_number"
    column and returns them as an array.
    """
    return df["award_number"].unique()


def __get_award_number(df):
    """
    Get a dictionary containing the count of occurrences of each unique award number.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "award_number" column. The result is returned as a dictionary, where
    the keys represent unique award numbers, and the values represent the count of occurrences
    for each award number.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "award_number" with award number information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique award numbers, and the values represent
        the count of occurrences for each award number.

    Note:
    -----
    The input DataFrame `df` should have a column named "award_number" containing categorical data
    representing different award numbers. The function counts the occurrences of each unique award number
    and returns the result as a dictionary.
    """
    return df["award_number"].value_counts().to_dict()


def __get_species(df):
    """
    Get a dictionary containing the count of occurrences of each unique species.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "species" column. The result is returned as a dictionary, where the
    keys represent unique species, and the values represent the count of occurrences for
    each species.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "species" with species information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique species, and the values represent
        the count of occurrences for each species.

    Note:
    -----
    The input DataFrame `df` should have a column named "species" containing categorical data
    representing different species. The function counts the occurrences of each unique species
    and returns the result as a dictionary.
    """
    return df["species"].value_counts().to_dict()


def __get_ncbitaxonomy(df):
    """
    Get a dictionary containing the count of occurrences of each unique NCBI taxonomy.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "ncbitaxonomy" column. The result is returned as a dictionary, where
    the keys represent unique NCBI taxonomies, and the values represent the count of occurrences
    for each taxonomy.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "ncbitaxonomy" with NCBI taxonomy information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique NCBI taxonomies, and the values represent
        the count of occurrences for each taxonomy.

    Note:
    -----
    The input DataFrame `df` should have a column named "ncbitaxonomy" containing categorical data
    representing different NCBI taxonomies. The function counts the occurrences of each unique NCBI taxonomy
    and returns the result as a dictionary.
    """
    return df["ncbitaxonomy"].value_counts().to_dict()


def __get_genotypes(df):
    """
    Get unique genotypes from the DataFrame.

    This function takes a pandas DataFrame as input and extracts the unique values from the 'genotype'
    column of the DataFrame. It returns an array containing the unique genotypes present in the 'genotype'
    column.

    Parameters:
        df (pandas.DataFrame): The input DataFrame containing the 'genotype' column.

    Returns:
        numpy.ndarray: An array containing the unique genotypes found in the 'genotype' column.
    """
    return df["genotype"].unique()


def __get_genotype_frequency(df):
    """
    Get a dictionary containing the count of occurrences of each unique genotype.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "genotypes" column. The result is returned as a dictionary, where the
    keys represent unique genotypes, and the values represent the count of occurrences for
    each genotype.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "genotypes" with genotype information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique genotypes, and the values represent
        the count of occurrences for each genotype.

    Note:
    -----
    The input DataFrame `df` should have a column named "genotypes" containing categorical data
    representing different genotypes. The function counts the occurrences of each unique genotype
    and returns the result as a dictionary.
    """
    return df["genotypes"].value_counts().to_dict()


def __get_generalmodality(df):
    """
    Get a dictionary containing the count of occurrences of each unique general modality.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "generalmodality" column. The result is returned as a dictionary,
    where the keys represent unique general modalities, and the values represent the count
    of occurrences for each general modality.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "generalmodality" with general modality information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique general modalities, and the values represent
        the count of occurrences for each general modality.

    Note:
    -----
    The input DataFrame `df` should have a column named "generalmodality" containing categorical data
    representing different general modalities. The function counts the occurrences of each unique general
    modality and returns the result as a dictionary.
    """
    return df["generalmodality"].value_counts().to_dict()


def __get_techniques(df):
    """
    Get a dictionary containing the count of occurrences of each unique technique.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "technique" column. The result is returned as a dictionary, where the
    keys represent unique techniques, and the values represent the count of occurrences for
    each technique.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "technique" with technique information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique techniques, and the values represent
        the count of occurrences for each technique.
    Note:
    -----
    The input DataFrame `df` should have a column named "technique" containing categorical data
    representing different techniques. The function counts the occurrences of each unique technique
    and returns the result as a dictionary.
    """
    return df["technique"].value_counts().to_dict()


def __get_award_numbers(df):
    """
    Get a dictionary containing the count of occurrences of each unique award number.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "award_number" column. The result is returned as a dictionary, where
    the keys represent unique award numbers, and the values represent the count of occurrences
    for each award number.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "award_number" with award number information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique award numbers, and the values represent
        the count of occurrences for each award number.

    Note:
    -----
    The input DataFrame `df` should have a column named "award_number" containing categorical data
    representing different award numbers. The function counts the occurrences of each unique award number
    and returns the result as a dictionary.
    """
    return df["award_number"].value_counts().to_dict()


def __get_affiliations(df):
    """
    Return a dictionary containing the count of occurrences of each unique value
    in the "affiliation" column of the input DataFrame.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "affiliation" from which
        the unique values will be counted.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique values in the "affiliation"
        column, and the values represent the count of occurrences of each unique value.

    Note:
    -----
    The input DataFrame `df` should have a column named "affiliation" containing
    categorical data, where the function will count the occurrences of each unique value.
    """
    return df["affiliation"].value_counts().to_dict()


def __get_contributors(df):
    """
    Get a dictionary containing the count of occurrences of each unique contributor name.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "contributorname" column. The result is returned as a dictionary,
    where the keys represent unique contributor names, and the values represent the count
    of occurrences for each contributor name.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "contributorname" with contributor name information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique contributor names, and the values represent
        the count of occurrences for each contributor name.


    Note:
    -----
    The input DataFrame `df` should have a column named "contributorname" containing categorical data
    representing different contributor names. The function counts the occurrences of each unique
    contributor name and returns the result as a dictionary.
    """
    return df["contributorname"].value_counts().to_dict()


def __get_projects(df):
    """
    Get a dictionary containing the count of occurrences of each unique project.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "project" column. The result is returned as a dictionary, where the
    keys represent unique projects, and the values represent the count of occurrences for
    each project. Additionally, the unique values in the "technique" column are also added
    to the same dictionary as a separate entry.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "project" with project information.

    Returns:
    --------
    dict
        A dictionary containing two entries:
        - "project": A dictionary where the keys represent unique projects, and the values
                     represent the count of occurrences for each project.
        - "technique": A dictionary where the keys represent unique techniques, and the values
                       represent the count of occurrences for each technique.

    Note:
    -----
    The input DataFrame `df` should have columns named "project" and "technique" containing categorical data
    representing different projects and techniques, respectively. The function counts the occurrences of each
    unique project and technique and returns them as part of a single dictionary.
    """
    return df["technique"].unique().to_dict()


def techniques_frequency(df):
    """
    Get a dictionary containing the count of occurrences of each unique technique.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "technique" column. The result is returned as a dictionary, where the
    keys represent unique techniques, and the values represent the count of occurrences for
    each technique.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "technique" with technique information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique techniques, and the values represent
        the count of occurrences for each technique.


    Note:
    -----
    The input DataFrame `df` should have a column named "technique" containing categorical data
    representing different techniques. The function counts the occurrences of each unique technique
    and returns the result as a dictionary.
    """
    return df["technique"].value_counts().to_dict()


def __get_locations(df):
    """
    Get a dictionary containing the count of occurrences of each unique location.

    This function takes a pandas DataFrame `df` as input and counts the occurrences of each
    unique value in the "locations" column. The result is returned as a dictionary, where the
    keys represent unique locations, and the values represent the count of occurrences for
    each location.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing a column named "locations" with location information.

    Returns:
    --------
    dict
        A dictionary where the keys represent unique locations, and the values represent
        the count of occurrences for each location.


    Note:
    -----
    The input DataFrame `df` should have a column named "locations" containing categorical data
    representing different locations. The function counts the occurrences of each unique location
    and returns the result as a dictionary.
    """
    return df["locations"].value_counts().to_dict()


def __get_contributors(df):
    """
    Get a list of unique contributors from the input DataFrame.

    This function takes a pandas DataFrame `df` as input and extracts the unique values from the
    "contributorname" column. It returns a list containing the names of unique contributors.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing contributor information.

    Returns:
    --------
    list
        A list containing the names of unique contributors present in the DataFrame.

    Note:
    -----
    The input DataFrame `df` should have a column named "contributorname" containing names of contributors.
    The function extracts the unique names from the column and returns them as a list.
    """
    return df["contributorname"].unique()


def __get_project_names(df):
    """
    Gets the unique list of project names.

    This function takes a pandas DataFrame `df` as input and extracts the unique values from the
    "project" column. It returns a list containing the names of unique projects.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing project information.

    Returns:
    --------
    list
        A list containing the names of unique projects present in the DataFrame.

    Note:
    -----
    The input DataFrame `df` should have a column named "project" containing names of projects.
    The function extracts the unique project names from the column and returns them as a list.
    """
    return df["project"].unique()


def __get_list_of_projects(df):
    """
    Get a dictionary of unique project names from the input DataFrame.

    This function takes a pandas DataFrame `df` as input and extracts the unique values from the
    "project" column. It returns a dictionary where the keys represent the unique project names,
    and the values are set to None.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing project information.

    Returns:
    --------
    dict
        A dictionary containing the unique project names as keys and None as values.

    Note:
    -----
    The input DataFrame `df` should have a column named "project" containing names of projects.
    The function extracts the unique project names from the column and returns them as keys in
    the dictionary. The values for all keys are set to None.
    """
    return df["project"].unique().to_dict()


def __get_number_of_projects(df):
    """
    Get the number of unique projects from the input DataFrame.

    This function takes a pandas DataFrame `df` as input and calculates the number of unique
    project names present in the "project" column.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing project information.

    Returns:
    --------
    int
        The number of unique projects in the DataFrame.

    Note:
    -----
    The input DataFrame `df` should have a column named "project" containing names of projects.
    The function calculates the number of unique project names in the column and returns the count as an integer.
    """
    return len(df["project"].unique())


def get_projects_treemap(df):
    """
    Generate a treemap visualization for project counts in the input DataFrame.

    This function takes a pandas DataFrame `df` as input and creates a treemap visualization
    to represent the relative proportions of each project based on their counts in the DataFrame.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing project information.

    Returns:
    --------
    None
        The function displays the treemap visualization using matplotlib.pyplot.

    Note:
    -----
    The function calculates the counts of each unique project in the input DataFrame `df` and
    visualizes the relative proportions using a treemap plot. The size of each rectangle in the
    treemap is proportional to the count of the corresponding project. The treemap is displayed
    using the `squarify` and `matplotlib.pyplot` libraries. The plot is saved as a PNG file with a
    filename formatted as "treemap-projects-YYYYMMDD.png", where "YYYYMMDD" represents the current
    date when the function is executed.
    """
    df = df["project"].value_counts().to_dict()
    sizes_list = list(df.values())
    names_list = list(df.keys())
    squarify.plot(sizes_list)

    filename = f'treemap-projects-{datetime.now().strftime("%Y%m%d")}.png'
    plt.savefig(filename)


def __get_modalities(df):
    """
    Get the counts of different modalities from the DataFrame.

    This function takes a pandas DataFrame as input and extracts the counts of different modalities
    from the 'generalmodality' column of the DataFrame. It returns a dictionary where the keys
    represent the unique modalities, and the values represent their respective counts.

    Parameters:
        df (pandas.DataFrame): The input DataFrame containing the 'generalmodality' column.

    Returns:
        dict: A dictionary with modalities as keys and their corresponding counts as values.
    """
    return (df["generalmodality"].value_counts()).to_dict()


def __get__percentage_of_metadata_version_1(df):
    """
    Get the percentage of metadata version 1 samples in the input DataFrame.

    This function takes a pandas DataFrame `df` as input and calculates the percentage of samples
    in the DataFrame that have a metadata version equal to 1.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing metadata information.

    Returns:
    --------
    float
        The percentage of samples with metadata version 1 as a decimal value.

    Note:
    -----
    The input DataFrame `df` should have a column named "metadata_version" containing numeric values
    representing the metadata version for each sample. The function calculates the percentage of samples
    with metadata version 1 by dividing the count of samples with version 1 by the total number of samples.
    The result is returned as a decimal value representing the percentage.
    """
    return len(df[df["metadata_version"] == 1]) / len(df)


def __get__percentage_of_metadata_version_2(df):
    """
    Calculates the percentage of rows in the DataFrame that have 'metadata_version' equal to 2.

    Parameters:
        df (pandas.DataFrame): The input DataFrame containing the 'metadata_version' column.

    Returns:
        float: The percentage of rows with 'metadata_version' equal to 2 as a decimal value.
    """
    return len(df[df["metadata_version"] == 2]) / len(df)


def report():
    """
    Generate a report summarizing data statistics for today's datasets.

    This function generates a report summarizing various data statistics for today's datasets.
    It retrieves today's date, collects data information for today using the `today()` function,
    and computes various statistics related to the datasets. The statistics include the number
    of datasets, number of unique projects, completeness score, metadata versions count,
    contributor count, affiliation count, award numbers count, species count, NCBI taxonomy count,
    sample local ID count, genotype count, general modality count, technique count, location count,
    and the percentage of datasets with metadata version 1. Additionally, it creates a treemap
    visualization for project counts.

    Returns:
    --------
    dict
        A dictionary containing the generated report with various data statistics.

    Note:
    -----
    The function generates a report summarizing data statistics for today's datasets. It collects
    the required data using various helper functions and creates a dictionary containing the
    computed statistics. The report includes information on the number of datasets, number of unique
    projects, completeness score, metadata versions count, contributor count, affiliation count,
    award numbers count, species count, NCBI taxonomy count, sample local ID count, genotype count,
    general modality count, technique count, location count, and the percentage of datasets with
    metadata version 1. The treemap visualization for project counts is also generated as part of the
    report.
    """
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
    report["contributor"] = __get_contributors(df)
    report["affiliation"] = __get_affilation(df)
    report["award_number"] = __get_award_number(df)
    report["species"] = __get_species(df)
    report["ncbitaxonomy"] = __get_ncbitaxonomy(df)
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

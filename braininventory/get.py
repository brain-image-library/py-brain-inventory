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

def report():
    # Get today's date
	tdate = date.today()

	# Convert date to string
	tdate = tdate.strftime("%Y-%m-%d")
        
	df = today()

	report = {}
	report['date'] = tdate
	report['number_of_datasets'] = __get_number_of_datasets(df)

	return report

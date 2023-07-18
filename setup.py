import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="braininventory",
    version="1.0.0",
    author="Ivan Cao-Berg, Eduardo Figueroa",
    author_email="icaoberg@psc.edu",
    description="A basic inventory management package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/brain-image-library/py-bil-inventory",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
import pandas as pd
import urllib.request

url = 'https://download.brainimagelibrary.org/inventory/daily/reports/today.json'
file_path, _ = urllib.request.urlretrieve(url)
df = pd.read_json(file_path)

#get affiliations
affiliations = df['genotype'].value_counts().to_dict()
affiliations

!pip install kaleido

import plotly.graph_objects as go
from datetime import date

def create_tree_map(frequency_dict):
    labels = list(frequency_dict.keys())
    values = list(frequency_dict.values())

    fig = go.Figure(go.Treemap(
        labels=labels,
        parents=[''] * len(labels),
        values=values,
        textinfo='label+value'
    ))

    fig.update_layout(title='Genotypes')

    today = date.today()
    output_path = f'treemap-{today.strftime("%Y%m%d")}.png'
    fig.write_image(output_path)
    fig.show()

create_tree_map(affiliations)

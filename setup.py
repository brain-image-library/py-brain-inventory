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
    install_requires=[
        "squarify",
        "pandarallel",
        "humanize",
        "geoip2",
        "seaborn",
        "matplotlib",
        "folium",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)

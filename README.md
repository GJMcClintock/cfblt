# College Football Load & Transform (cfblt)

## Overview

A Python application that pulls data from the ESPN API providing information about college football games. This project currently writes to a local duckdb file. Roadmapped is to write to parquet and set up the
duckdb file manually so it can be stored on github and allow others to pull directly from the raw github
files.

## Features

* Pulls game data from ESPN's hidden in plain site API
* Leverages dlt (data load tool) for parallelized processing of data
* Accepts a few CLI arguments to customize the pipelines's behavior
* The default behavior will pull everything fromt he CFP Era to present.


## Installation

To install required libraries, run:
```pip install requirements.txt```

## Usage
To run the pipeline locally with default arguments, simply cd into the directory run:
```python cfblt.py```

You can also pass in a few arguments to customize the behavior of the pipeline. These are:

```python cfblt.py --start_year 2019 --end_year 2020 --years_to_fill 10 --load_year 2024```

`start_year` - The earliest year to pull data from. Defaults to 2014

`end_year` - The latest year to pull data from. Defaults to the current year

`years_to_fill` - How many years to fill in the database with. This defaults to none and can be used as an alternative to `end_year`

`load_year` - This takes precedence over all other params - and will load data from a specific single year. Defaults to None.

## dlt 
dlt is a pipeline management library that makes fetching data from APIs extremely easy. It can handle incremental loads, normalizing nested json into their own tables (hence the ugly names in the duckdb), and has a number of prebuilt pipelines for common SaaS products.

It removes all the boilerplate need to create tables, manage connections, track state, etc. Just give it a list or json object and it does its thing.

## duckdb
duckdb is an in-memory SQL database that is the OLAP equivalent of SQLite. It's pretty cool! If you want to run this pipeline on the cloud (for free!) you can modify this to use MotherDuck on their free tier. The output duckdb file of this project is added to the .gitignore file for the repo because of its size.

## dbt
dbt is the market leader for implementing transformations to your data warehouse. duckdb maintains an adapter and you can use this to save any sql you want to run against the warehouse we built.

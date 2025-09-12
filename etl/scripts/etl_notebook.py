"""
etl notebook
"""

import polars as pl
import pandas as pd
import fastexcel
import os
from ddf_utils.str import to_concept_id, format_float_digits
from functools import partial


source_file = "../source/Statistical Review of World Energy Data.xlsx"


reader = fastexcel.read_excel(source_file)


sheets = reader.sheet_names


# drop a few non data tables
sheets.remove("Contents")
sheets.remove("Approximate conversion factors")
sheets.remove("Definitions")

sheets


# # function to read the indicator name
# def read_indicator_name(sheet_name):
#     val = (
#         reader.load_sheet_by_name(sheet_name, n_rows=1, header_row=None, use_columns="A")
#         .to_polars()
#         .item()
#     )
#     return val.replace("*", "")


# read_indicator_name(sheets[1])


# now create a configuration dictionary, key is sheet name and value is parameter dictionary.
#
# first assign some default values
config_dict: dict[str, dict] = dict(
    [
        (
            x,
            {
                "indicator_name": x,
                "read_options": {"header_row": 2},
                "multiple_table": False,
                "last_col": "2024",
            },
        )
        for x in sheets
    ]
)

config_dict[sheets[0]]


# overwrite configs for sheets that have different layouts
config_dict["Oil - Proved reserves history"]["read_options"]["header_row"] = 4
config_dict["Oil - Proved reserves history"]["last_col"] = "2020"
config_dict["Gas - Proved reserves history "]["read_options"]["header_row"] = 4
config_dict["Gas - Proved reserves history "]["last_col"] = "2020"
config_dict["Solar Installed Capacity"]["read_options"]["header_row"] = 3
config_dict["Wind Installed Capacity"]["read_options"]["header_row"] = 3
config_dict["Biofuels production - kboed"]["multiple_table"] = True
config_dict["Biofuels production - PJ"]["multiple_table"] = True
config_dict["Biofuels Consumption - kboed"]["multiple_table"] = True
config_dict["Biofuels consumption - PJ"]["multiple_table"] = True

# drop some sheets we don't include
config_dict.pop("Oil - Regional Consumption")
config_dict.pop("Gas - H2 Production Capacity")


geo_name_map = {
    "US": "United States",
    "Viet Nam": "Vietnam",
    "USSR": "Russia",
    "India2": "India",
    "DR Congo": "Democratic Republic of Congo",
    "Brazil1": "Brazil",
    "S. & Cent. America": "South & Central America",
    "Total S. & Cent. America": "Total South & Central America",
    "Other S. & Cent. America": "Other South & Central America",
}


# function to read and preprocess a sheet.
def read_sheet_and_preprocess(sheet_name: str):
    """preprocessing the data:
    1. rename the first column to geo_name
    2. rename the geo_name to alphanumeric
    3. drop all empty lines and lines after 'total world'

    If multiple table is true for a sheet, then it will return a dictionary
    where key is the table name and value is the dataframe.

    Note: This function only applies to the tab with country as row index
    and year as column index.
    """
    config = config_dict[sheet_name]
    header_row = config["read_options"]["header_row"]
    multiple_table = config["multiple_table"]
    last_col = config["last_col"]
    data = reader.load_sheet_by_name(sheet_name, header_row=header_row).to_pandas()
    if multiple_table:
        if "Biofuel" not in sheet_name:
            raise NotImplementedError("multiple table only implemented for Biofuel sheets.")
        # rename first column
        fst_col = str(data.columns[0])
        data = data.rename(columns={fst_col: "geo_name"})
        data["geo_name"] = data["geo_name"].replace(geo_name_map)
        data.columns = data.columns.map(str)

        # drop rows that are all empty, and reset index
        data = data.dropna(how="all").reset_index(drop=True)

        # find split points
        # there are 3 tables in this sheet, we need to find the boundaries for each
        total_world_indices = data.index[data["geo_name"] == "Total World"].tolist()
        biogasoline_header_index = data.index[data["geo_name"] == "Biogasoline"].tolist()[0]
        biodiesel_header_index = data.index[data["geo_name"] == "Biodiesel"].tolist()[0]

        # now slice the data into 3 dataframes
        biofuel_df = data.iloc[: total_world_indices[0] + 1].copy()
        biogasoline_df = data.iloc[biogasoline_header_index + 1 : total_world_indices[1] + 1].copy()
        biodiesel_df = data.iloc[biodiesel_header_index + 1 : total_world_indices[2] + 1].copy()

        res = {}
        for name, df in [
            ("biofuel", biofuel_df),
            ("biogasoline", biogasoline_df),
            ("biodiesel", biodiesel_df),
        ]:
            df["geo"] = df["geo_name"].map(to_concept_id)
            df = df.set_index("geo")
            df = df.loc[:, :last_col]
            df = df.reset_index()
            res[name] = df
        return res
    else:
        fst_col = str(data.columns[0])
        data = data.rename(columns={fst_col: "geo_name"})
        data["geo_name"] = data["geo_name"].replace(geo_name_map)
        data.columns = data.columns.map(str)
        data["geo"] = data["geo_name"].map(to_concept_id)
        data = data.set_index("geo")
        data = data.dropna(how="all")
        try:
            data = data.loc[:"total_world", :last_col]
            data = data.reset_index()
            return data
        except KeyError as e:
            print(f"error: {e}")
            return None


def to_ddf_datapoint(df: pd.DataFrame, indicator_name: str):
    """convert a preprocessed dataframe to ddf datapoint format."""
    concept_id = to_concept_id(indicator_name)
    df_long = df.melt(id_vars=["geo", "geo_name"], var_name="year", value_name=concept_id)
    return df_long[["geo", "year", concept_id]]


# Initialize containers for concepts and entities
all_geos = {}
all_indicators = {}

# Loop through all sheets
for sheet_name in sheets:
    print(f"Processing sheet: {sheet_name}")

    if sheet_name not in config_dict:
        print(f"no config, skipped {sheet_name}")
        continue

    config = config_dict[sheet_name]
    res = read_sheet_and_preprocess(sheet_name)

    if res is None:
        print(f"Skipping sheet {sheet_name} due to processing error.")
        continue

    if config["multiple_table"]:
        # This is a biofuel sheet with multiple tables
        for subtype, df in res.items():
            # collect geos
            new_geos = df[["geo", "geo_name"]].drop_duplicates().set_index("geo")["geo_name"]
            all_geos.update(new_geos.to_dict())

            # Create a unique indicator name and concept_id
            indicator_name = f"{config['indicator_name']} {subtype}"
            concept_id = to_concept_id(indicator_name)

            # Store indicator info
            all_indicators[concept_id] = indicator_name

            # Convert to DDF datapoint format
            dp = to_ddf_datapoint(df, indicator_name)

            # Format and save
            if not dp.empty:
                dp[concept_id] = dp[concept_id].map(format_float_digits)
                dp = dp.dropna(subset=[concept_id])
                if not dp.empty:
                    filename = f"../../ddf--datapoints--{concept_id}--by--geo--year.csv"
                    dp.to_csv(filename, index=False)

    else:
        # This is a regular sheet with a single table
        df = res

        # collect geos
        new_geos = df[["geo", "geo_name"]].drop_duplicates().set_index("geo")["geo_name"]
        all_geos.update(new_geos.to_dict())

        indicator_name = config["indicator_name"]
        concept_id = to_concept_id(indicator_name)

        # Store indicator info
        all_indicators[concept_id] = indicator_name

        # Convert to DDF datapoint format
        dp = to_ddf_datapoint(df, indicator_name)

        # Format and save
        if not dp.empty:
            dp[concept_id] = dp[concept_id].map(format_float_digits)
            dp = dp.dropna(subset=[concept_id])
            if not dp.empty:
                filename = f"../../ddf--datapoints--{concept_id}--by--geo--year.csv"
                dp.to_csv(filename, index=False)

# Create and save geo entity file
geo_df = pd.DataFrame.from_dict(all_geos, orient="index", columns=["name"])
geo_df.index.name = "geo"
geo_df = geo_df.reset_index().dropna(subset="geo")
geo_df.to_csv("../../ddf--entities--geo.csv", index=False)

# Create and save concept files
# continuous concepts (indicators)
indicators_df = pd.DataFrame.from_dict(all_indicators, orient="index", columns=["name"])
indicators_df.index.name = "concept"
indicators_df = indicators_df.reset_index()
indicators_df["concept_type"] = "measure"
indicators_df = indicators_df[["concept", "concept_type", "name"]]
indicators_df.to_csv("../../ddf--concepts--continuous.csv", index=False)

# discrete concepts (primary concepts)
primary_concepts = pd.DataFrame(
    [
        {"concept": "geo", "name": "Geo", "concept_type": "entity_domain"},
        {"concept": "year", "name": "Year", "concept_type": "time"},
        {"concept": "name", "name": "Name", "concept_type": "string"},
        {"concept": "domain", "name": "Domain", "concept_type": "string"},
    ]
)
primary_concepts["domain"] = ""
primary_concepts = primary_concepts[["concept", "concept_type", "name", "domain"]]
primary_concepts.to_csv("../../ddf--concepts--discrete.csv", index=False)

print("Done.")

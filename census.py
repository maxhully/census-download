import multiprocessing

import geopandas
import pandas
import requests
from tqdm import tqdm

variables = [
    # pop
    "P005001",
    "P005003",
    "P005004",
    "P005005",
    "P005006",
    "P005007",
    "P005008",
    "P005009",
    "P005010",
    # vap
    "P011001",
    "P011002",
    "P011005",
    "P011006",
    "P011007",
    "P011008",
    "P011009",
    "P011010",
    "P011011",
]

keys = [
    # pop
    "TOTPOP",
    "NH_WHITE",
    "NH_BLACK",
    "NH_AMIN ",
    "NH_ASIAN",
    "NH_NHPI ",
    "NH_OTHER",
    "NH_2MORE",
    "HISP",
    # vap
    "VAP",
    "HVAP",
    "WVAP",
    "BVAP",
    "AMINVAP",
    "ASIANVAP",
    "NHPIVAP",
    "OTHERVAP",
    "2MOREVAP",
]


def counties(state_fips):
    resp = requests.get(
        "https://api.census.gov/data/2010/dec/sf1"
        "?get=NAME&for=county:*&in=state:{}".format(state_fips)
    )
    header, *rows = resp.json()
    county_column_index = header.index("county")
    county_fips_codes = set(row[county_column_index] for row in rows)
    return county_fips_codes


def block_data_for_county(state_fips, county_fips, variables=variables, keys=keys):
    url = (
        "https://api.census.gov/data/2010/dec/sf1"
        + "?get={},NAME&for=block:*".format(",".join(variables))
        + "&in=state:{}&in=county:{}&in=tract:*".format(state_fips, county_fips)
    )
    resp = requests.get(url)
    header, *rows = resp.json()
    variable_lookup = dict(zip(variables, keys))
    columns = [variable_lookup.get(column_name, column_name) for column_name in header]
    dtypes = {key: int for key in keys}
    dtypes.update({key: str for key in ["state", "county", "tract", "block"]})
    data = pandas.DataFrame.from_records(rows, columns=columns).astype(dtypes)
    data["geoid"] = data["state"] + data["county"] + data["tract"] + data["block"]
    return data


def block_data_for_state(state_fips):
    county_fips_codes = counties(state_fips)
    with multiprocessing.Pool(8) as p:
        county_data = p.starmap(
            block_data_for_county,
            [(state_fips, county_fips) for county_fips in county_fips_codes],
        )
    return pandas.concat(county_data)


def block_geometries_url(state_fips, year=2010):
    return (
        f"https://www2.census.gov/geo/tiger/TIGER{year}/TABBLOCK/{year}/"
        f"tl_{year}_{state_fips}_tabblock10.zip"
    )


def blocks_for_state(state_fips):
    url = block_geometries_url(state_fips)
    gdf = geopandas.read_file(url)
    data = block_data_for_state(state_fips)
    data.set_index("geoid", inplace=True)
    gdf.set_index("GEOID10", inplace=True, drop=False)
    return gdf.join(data)

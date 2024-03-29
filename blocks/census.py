import multiprocessing

import geopandas
import pandas
import requests

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
    "NH_AMIN",
    "NH_ASIAN",
    "NH_NHPI",
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
        "http://api.census.gov/data/2010/dec/sf1"
        "?get=NAME&for=county:*&in=state:{}&key={}".format(
            state_fips, "b3ea32a62428162150396ef79103b5e1b0ddf17a"
        )
    )
    assert resp.ok
    header, *rows = resp.json()
    county_column_index = header.index("county")
    county_fips_codes = set(row[county_column_index] for row in rows)
    return county_fips_codes


def data_for_county(
    state_fips, county_fips, units="block", variables=variables, keys=keys
):
    url = (
        "http://api.census.gov/data/2010/dec/sf1"
        + "?get={},NAME&for={}:*".format(
            ",".join(variables), requests.utils.quote(units)
        )
        + "&in=state:{}&in=county:{}&in=tract:*&key={}".format(
            state_fips, county_fips, "b3ea32a62428162150396ef79103b5e1b0ddf17a"
        )
    )
    resp = requests.get(url)
    header, *rows = resp.json()
    variable_lookup = dict(zip(variables, keys))
    columns = [variable_lookup.get(column_name, column_name) for column_name in header]
    dtypes = {key: int for key in keys}
    dtypes.update({key: str for key in ["state", "county", "tract", units]})
    data = pandas.DataFrame.from_records(rows, columns=columns).astype(dtypes)
    data["geoid"] = data["state"] + data["county"] + data["tract"] + data[units]
    return data


def data_for_state(state_fips, units="block"):
    county_fips_codes = counties(state_fips)
    with multiprocessing.Pool(8) as p:
        county_data = p.starmap(
            data_for_county,
            [(state_fips, county_fips, units) for county_fips in county_fips_codes],
        )
    return pandas.concat(county_data)


def block_geometries_url(state_fips, year=2010):
    return (
        f"http://www2.census.gov/geo/tiger/TIGER{year}/TABBLOCK/{year}/"
        f"tl_{year}_{state_fips}_tabblock10.zip"
    )


def blocks_for_state(state_fips, output_file=None):
    if output_file is None:
        output_file = f"{state_fips}.shp"

    url = block_geometries_url(state_fips)
    gdf = geopandas.read_file(url)

    data = data_for_state(state_fips, "block")

    data.set_index("geoid", inplace=True)
    gdf.set_index("GEOID10", inplace=True, drop=False)

    return gdf.join(data)

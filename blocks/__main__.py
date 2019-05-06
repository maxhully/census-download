import geopandas
import wasabi
import uuid
import sys

from .census import block_data_for_state, block_geometries_url


def main(state_fips, output_file=None):
    if output_file is None:
        output_file = f"{state_fips}_blocks.shp"
    printer = wasabi.Printer()

    with printer.loading("Downloading geometries..."):
        url = block_geometries_url(state_fips)
        gdf = geopandas.read_file(url)
    printer.good("Success!")
    with printer.loading("Downloading data..."):
        data = block_data_for_state(state_fips)
    printer.good("Success!")

    data.set_index("geoid", inplace=True)
    gdf.set_index("GEOID10", inplace=True, drop=False)

    gdf = gdf.join(data)

    try:
        gdf.to_file(output_file)
        printer.good(
            f"The shapefile with joined demographics is saved as {output_file}"
        )
    except Exception as e:
        printer.fail(f"Could not save the shapefile to the location {output_file}.")
        print(e)
        fallback = f"__output_{str(uuid.uuid4())[-8:]}.shp"
        printer.info(f"Saving to {fallback} as a fallback...")
        gdf.to_file(fallback)
        printer.good(f"The shapefile with joined demographics is saved as {fallback}")


if __name__ == "__main__":
    main(*sys.argv[1:])

import geopandas
import wasabi
import uuid
import us
import sys

from .census import block_data_for_state, block_geometries_url


from optparse import OptionParser

parser = OptionParser()
parser.add_option(
    "-o", "--output", dest="output_file", help="save shapefile to FILE", metavar="FILE"
)


def main(state_fips, year=2010, output_file=None):
    printer = wasabi.Printer()

    if state_fips is None:
        printer.fail("Must specify a valid US state or territory.")
        sys.exit(1)

    printer.info("Requested state: {}.".format(us.states.lookup(state_fips).name))
    printer.info("Tiger/LINE vintage: {}".format(year))

    if output_file is None:
        output_file = f"{state_fips}_blocks.shp"

    printer.info("Output filename: {}".format(output_file))

    with printer.loading("Downloading geometries..."):
        url = block_geometries_url(state_fips, year=year)
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
    (options, args) = parser.parse_args()
    state = args[0]
    state_fips = us.states.lookup(state).fips
    main(state_fips, output_file=options.output_file)

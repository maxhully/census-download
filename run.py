import geopandas
import gerrychain
from wasabi import Printer
import us
from enum import Enum
from blocks.census import data_for_state
import functools


def pull_state_graph(state, include_data=False):
    printer = Printer()
    fips = state.fips
    name = state.name
    with printer.loading(f"Downloading shapefile for {name}..."):
        df = geopandas.read_file(
            "http://www2.census.gov/geo/tiger/TIGER2010/BG/"
            f"2010/tl_2010_{fips}_bg10.zip"
        )
    df.set_index("GEOID10", inplace=True, drop=False)
    if include_data:
        with printer.loading(f"Downloading block group data for {name}..."):
            data = data_for_state(fips, "block group")
        data.set_index("geoid", inplace=True)
        df = df.join(data)
    with printer.loading(f"Creating graph for {name}..."):
        graph = gerrychain.Graph.from_geodataframe(df)
    return graph, df


class Result(Enum):
    Success = 0
    Failure = 1
    Error = 2


def catch_errors(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except Exception as e:
            print(e)
            return Result.Error
        return Result.Success

    return wrapped


def run_on_all_states(f, index_slice=None):
    if index_slice is not None:
        states = list(us.STATES)[index_slice]
    else:
        states = list(us.STATES)
    run_task = catch_errors(f)
    results = [run_task(state) for state in states]

    successes = sum(result is Result.Success for result in results)
    errors = sum(result is Result.Error for result in results)
    printer = Printer()
    printer.info("Final result:")
    printer.info(f"{successes} were created successfully. {errors} errored.")
    printer.table(
        list(
            zip(
                [name for name in states],
                [str(result) if result is not None else "Error" for result in results],
            )
        ),
        header=("State", "Created"),
        divider=True,
    )

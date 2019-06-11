import networkx
import sys
from wasabi import Printer
from run import run_on_all_states, pull_state_graph

printer = Printer()


def check_state_graph(graph, state):
    fips, name = state.fips, state.name
    graph.to_json(f"./states/{fips}.json")
    connected = networkx.is_connected(graph)
    if connected:
        printer.good(f"{name} is connected!")
        return True
    else:
        printer.fail(f"{name} is disconnected.")
        return False


def pull_and_check(state):
    return check_state_graph(pull_state_graph(state)[0], state)


if __name__ == "__main__":
    left, right = map(int, sys.argv[1:])
    run_on_all_states(pull_and_check, slice(left, right))

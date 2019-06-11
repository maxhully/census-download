import sys
from run import run_on_all_states, pull_state_graph


def pull_and_create(state):
    graph = pull_state_graph(state, include_data=True)
    graph.to_json(f"./states/{state.fips}.json")


if __name__ == "__main__":
    left, right = map(int, sys.argv[1:])
    run_on_all_states(pull_and_create, slice(left, right))

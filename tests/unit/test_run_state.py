from control_plane.run_state import TERMINAL_STATES, RunState


def test_terminal_states_are_a_subset_of_run_state():
    assert TERMINAL_STATES <= set(RunState)
    assert RunState.RUNNING not in TERMINAL_STATES
    assert RunState.SUCCEEDED in TERMINAL_STATES

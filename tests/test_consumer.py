"""
Unit tests for the consumer module, focusing on is_event_valid function.
"""

from hypothesis import given, strategies as st
from src.consumer import is_event_valid
from src.schemas import REQUIRED_EVENT_FIELDS

# Strategy for valid events: Always contains the required keys
valid_event_strategy = st.fixed_dictionaries({
    "event_id": st.uuids().map(str),
    "event_type": st.just("insert"),
    "produced_at": st.text(),
    "payload": st.dictionaries(st.text(), st.text())
})

@given(valid_event_strategy)
def test_is_event_valid_returns_true_for_correct_schema(event):
    """Ensures events with all required fields are marked as valid."""
    assert is_event_valid(event) is True

@given(st.dictionaries(st.text(), st.text()))
def test_is_event_valid_handles_missing_fields(event):
    """
    Hypothesis generates random dictionaries. 
    If a required field is missing, the function MUST return False.
    """
    # We only care about cases where at least one required field is missing
    if not all(field in event for field in ["event_id", "event_type", "produced_at", "payload"]):
        assert is_event_valid(event) is False

@given(st.one_of(st.lists(st.text()), st.text(), st.integers(), st.none()))
def test_is_event_valid_handles_non_dict_input(invalid_input):
    """Ensures the function doesn't crash if input is not a dictionary."""
    assert is_event_valid(invalid_input) is False
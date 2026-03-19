"""
Unit tests for the producer module, focusing on read_csv and build_event functions.
"""

import csv
import json
import uuid
from hypothesis import given, strategies as st, settings, HealthCheck
import pytest
from src.producer import read_csv, build_event

# Strategy: Generate a list of dictionaries with consistent keys (columns)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    st.lists(
        st.fixed_dictionaries({
            "show_id": st.integers(min_value=1).map(str),
            "type": st.sampled_from(["TV Show", "Movie"]),
            # Filter out control characters like \n and \r
            "title": st.text(alphabet=st.characters(blacklist_categories=("Cc", "Cs")), min_size=1)
        }),
        min_size=1,
        max_size=10
    )
)
def test_read_csv_content_integrity(tmp_path, data):
    """
    Ensures that data written to a CSV is exactly what read_csv yields.
    """
    file_path = tmp_path / "test_data.csv"
    
    keys = data[0].keys()
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

    # Convert generator to list to validate all rows
    result = list(read_csv(file_path))

    assert len(result) == len(data)
    assert result == data


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.lists(st.text()))
def test_read_csv_handles_weird_characters(tmp_path, random_strings):
    """
    Tests if the reader can handle malformed strings or special characters
    without raising unexpected exceptions.
    """
    file_path = tmp_path / "weird_data.csv"
    
    # Write random strings as a single-column CSV
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write("cpu_name\n")
        for s in random_strings:
            f.write(f'"{s}"\n')

    # If this raises a ValueError or UnicodeError, Hypothesis will catch it
    result = list(read_csv(file_path))
    assert len(result) == len(random_strings)


def test_read_csv_raises_error_on_invalid_extension(tmp_path):
    invalid_file = tmp_path / "data.txt"
    invalid_file.write_text("some content")
    
    with pytest.raises(ValueError, match="CSV file not found"):
        list(read_csv(invalid_file))


@given(st.dictionaries(
    keys=st.text(min_size=1), 
    values=st.one_of(st.text(), st.integers(), st.floats(), st.none())
))
def test_build_event_is_valid_json_bytes(row):
    """
    Ensures that for any dictionary input, the function returns 
    valid UTF-8 encoded JSON bytes with the correct structure.
    """
    # Execution
    result_bytes = build_event(row)
    
    # 1. Check type
    assert isinstance(result_bytes, bytes)
    
    # 2. Check if it's decodable and valid JSON
    decoded_json = json.loads(result_bytes.decode('utf-8'))
    
    # 3. Verify Structure
    assert "event_id" in decoded_json
    assert "event_type" in decoded_json
    assert decoded_json["event_type"] == "insert"
    assert decoded_json["payload"] == row
    
    # 4. Verify UUID format
    val = uuid.UUID(decoded_json["event_id"])
    assert str(val) == decoded_json["event_id"]
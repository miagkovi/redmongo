import csv
from pathlib import Path
from hypothesis import given, strategies as st, settings, HealthCheck
import pytest
from src.producer import read_csv

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
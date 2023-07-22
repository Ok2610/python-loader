import pytest
from click.testing import CliRunner
from cli import cli

def read_file(filename):
    with open(filename, "r") as file:
        content = file.read()
    return content

def test_cli():
    runner = CliRunner()
    # Reset DB and add some tagsets
    runner.invoke(cli, ['reset'], input="y\n")
    runner.invoke(cli, ['add', 'tagset', 'Location'])          # Alphanumerical
    runner.invoke(cli, ['add', 'tagset', 'TimeStamp', '2'])    # Timestamp format YYYY-MM-dd hh:mm:ss
    runner.invoke(cli, ['add', 'tagset', 'Time', '3'])         # Time format hh:mm:ss  
    runner.invoke(cli, ['add', 'tagset', 'Date', '4'])         # Date format YYYY-MM-dd
    runner.invoke(cli, ['add', 'tagset', 'Resolution', '5'])   # Numerical

    # Test 'get all tags' with empty DB
    result = runner.invoke(cli, ['get', 'tags'])
    assert result.output.strip() == "No results were fetched."
    
    # Test 'get tag' with id=1 with empty DB
    result = runner.invoke(cli, ['get', 'tag', '1'])
    assert result.output.strip() == "No results were fetched."

    # Test 'add tag' with correct types and values

    # alphanumerical tag
    result = runner.invoke(cli, ['add', 'tag', '1', '1', 'Paris'])
    assert result.output.strip() == """id: 1
tagSetId: 1
tagTypeId: 1
alphanumerical {
  value: "Paris"
}"""

    # numerical tag
    result = runner.invoke(cli, ['add', 'tag', '5', '5', '1080'])
    assert result.output.strip() == """id: 2
tagSetId: 5
tagTypeId: 5
numerical {
  value: 1080
}"""

    # add another tag to the same tagset (numerical)
    result = runner.invoke(cli, ['add', 'tag', '5', '5', '44100'])
    assert result.output.strip() == """id: 3
tagSetId: 5
tagTypeId: 5
numerical {
  value: 44100
}"""

    # timestamp tag
    result = runner.invoke(cli, ['add', 'tag', '2', '2', '1789-07-14/12:00:01'])
    assert result.output.strip() == """id: 4
tagSetId: 2
tagTypeId: 2
timestamp {
  value: "1789-07-14 12:00:01"
}"""

    result = runner.invoke(cli, ['add', 'tag', '3', '3', '12:00:01'])
    assert result.output.strip() == """id: 5
tagSetId: 3
tagTypeId: 3
time {
  value: "12:00:01"
}"""

    result = runner.invoke(cli, ['add', 'tag', '4', '4', '1789-07-14'])
    assert result.output.strip() == """id: 6
tagSetId: 4
tagTypeId: 4
date {
  value: "1789-07-14"
}"""

    # Add an already existent tag, should return its info
    result = runner.invoke(cli, ['add', 'tag', '4', '4', '1789-07-14'])
    assert result.output.strip() == """id: 6
tagSetId: 4
tagTypeId: 4
date {
  value: "1789-07-14"
}"""

    # Test if no copy tag has been added
    result = runner.invoke(cli, ['get', 'tag', '7'])
    assert result.output.strip() == "No results were fetched."
    

    # Test 'add tag' with various incorrect types
    result = runner.invoke(cli, ['add', 'tag', '3', '1', 'London'])
    assert result.output.strip() == "Error: incorrect type for the specified Tagset."

    result = runner.invoke(cli, ['add', 'tag', '1', '5', '1234'])
    assert result.output.strip() == "Error: incorrect type for the specified Tagset."

    result = runner.invoke(cli, ['add', 'tag', '2', '5', '1234'])
    assert result.output.strip() == "Error: incorrect type for the specified Tagset."

    result = runner.invoke(cli, ['add', 'tag', '1', '4', '2022-01-17'])
    assert result.output.strip() == "Error: incorrect type for the specified Tagset."


   # Test get methods
    result = runner.invoke(cli, ['get', 'tag', '1'])
    assert result.output.strip() == """id: 1
tagSetId: 1
tagTypeId: 1
alphanumerical {
  value: "Paris"
}"""

    result = runner.invoke(cli, ['get', 'tag', '2'])
    assert result.output.strip() == """id: 2
tagSetId: 5
tagTypeId: 5
numerical {
  value: 1080
}"""

    result = runner.invoke(cli, ['get', 'tag', '4'])
    assert result.output.strip() == """id: 4
tagSetId: 2
tagTypeId: 2
timestamp {
  value: "1789-07-14 12:00:01"
}"""

    result = runner.invoke(cli, ['get', 'tag', '5'])
    assert result.output.strip() == """id: 5
tagSetId: 3
tagTypeId: 3
time {
  value: "12:00:01"
}"""

    result = runner.invoke(cli, ['get', 'tag', '6'])
    assert result.output.strip() == """id: 6
tagSetId: 4
tagTypeId: 4
date {
  value: "1789-07-14"
}"""

    # Test get all tags
    result = runner.invoke(cli, ['get', 'tags'])
    assert result.output.strip() == read_file('output_gettags')


if __name__ == '__main__':
    test_cli()

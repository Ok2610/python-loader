import pytest
from click.testing import CliRunner
from cli import cli

def read_file(filename):
    with open(filename, "r") as file:
        content = file.read()
    return content

def test_cli():
    runner = CliRunner()
    # Reset DB
    result = runner.invoke(cli, ['reset'], input="y\n")

    # Test 'get all medias' with empty DB
    result = runner.invoke(cli, ['get', 'medias'])
    assert result.output.strip() == ""
    
    # Test 'get 1 media' with empty DB
    result = runner.invoke(cli, ['get', 'media', '1'])
    assert result.output.strip() == "{'Error': 'Could not find media with the given ID'}"

    # Test 'add directory'
    result = runner.invoke(cli, ['add', 'media', './testfiles'])
    assert result.output.strip() == read_file('output_addmedias')

    # Test 'add directory' with format filter
    result = runner.invoke(cli, ['add', 'media', './testfiles', '-f', 'mp3'])
    assert result.output.strip() == "{'Success': 'added 2 medias to database.'}\n{'Info': '2 files were found in the directory.'}"
    
    # Test 'add directory' with format filter, no files found
    result = runner.invoke(cli, ['add', 'media', './testfiles', '-f', 'gpx'])
    assert result.output.strip() == "{'Info': '0 files were found in the directory.'}"

    # Test 'add single file'
    result = runner.invoke(cli, ['add', 'media', './testfiles/random.gif'])
    assert result.output.strip().startswith("id: 857")

    # Test 'get medias'
    result = runner.invoke(cli, ['get', 'medias'])
    output_lines = result.output.split("\n\n")
    num_lines = len(output_lines)
    assert num_lines == 858     # counting the last \n which only appears here for some reason

    # Test 'get media' with unexistant ID
    result = runner.invoke(cli, ['get', 'media', '888'])
    assert result.output.strip() == "{'Error': 'Could not find media with the given ID'}"

    # Test 'get media' with ID 1
    result = runner.invoke(cli, ['get', 'media', '857'])
    assert "random.gif" in result.output        

   # Cannot test 'get-id-from-uri' as the uri will change based on the location of the client 

    

if __name__ == '__main__':
    test_cli()

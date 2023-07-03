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
    runner.invoke(cli, ['reset'], input="y\n")
    
    # Load json schema
    runner.invoke(cli, ['import', './testfiles/import.json'])
    # Export to another json file
    runner.invoke(cli, ['export', './testfiles/export_json.json'])

    assert read_file('./testfiles/import.json') == read_file('./testfiles/export_json.json')


    # Reset DB
    runner.invoke(cli, ['reset'], input="y\n")
    # Load csv schema
    runner.invoke(cli, ['import', '-f', 'csv', './testfiles/import.csv'])
    # Export to another json file
    runner.invoke(cli, ['export', './testfiles/export_csv.json'])

    assert read_file('./testfiles/import.json') == read_file('./testfiles/export_csv.json')

if __name__ == '__main__':
    test_cli()

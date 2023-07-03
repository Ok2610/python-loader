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
    # Add some tags
    runner.invoke(cli, ['add', 'tag', '1', '1', 'Paris'])
    runner.invoke(cli, ['add', 'tag', '2', '2', '1789-07-14/12:00:01'])
    runner.invoke(cli, ['add', 'tag', '3', '3', '12:00:01'])   
    runner.invoke(cli, ['add', 'tag', '4', '4', '1789-07-14'])
    runner.invoke(cli, ['add', 'tag', '5', '5', '1080'])
    runner.invoke(cli, ['add', 'tag', '5', '5', '44100'])
    # Add the medias
    runner.invoke(cli, ['add', 'media', './testfiles'])

    # Test 'get all taggings' with empty table
    result = runner.invoke(cli, ['get', 'taggings'])
    assert result.output.strip() == ""

    # Test 'get tags-of-media' with inexistant media ID
    result = runner.invoke(cli, ['get', 'tags-of-media', '11111'])
    assert result.output.strip() == ""

    # Test 'get tags-of-media' with no tags attributed to media
    result = runner.invoke(cli, ['get', 'tags-of-media', '1'])
    assert result.output.strip() == ""

    # Test 'get medias-with-tag' with inexistant tag ID
    result = runner.invoke(cli, ['get', 'medias-with-tag', '11111'])
    assert result.output.strip() == ""

    # Test 'get medias-with-tag' with no medias linked to specified tag
    result = runner.invoke(cli, ['get', 'medias-with-tag', '1'])
    assert result.output.strip() == ""

    # Add correct taggings
    result = runner.invoke(cli, ['add', 'tagging', '1', '1'])
    assert result.output.strip() == "mediaId: 1\ntagId: 1"
    
    result = runner.invoke(cli, ['add', 'tagging', '1', '2'])
    assert result.output.strip() == "mediaId: 1\ntagId: 2"

    result = runner.invoke(cli, ['add', 'tagging', '1', '3'])
    assert result.output.strip() == "mediaId: 1\ntagId: 3"

    result = runner.invoke(cli, ['add', 'tagging', '1', '4'])
    assert result.output.strip() == "mediaId: 1\ntagId: 4"

    result = runner.invoke(cli, ['add', 'tagging', '1', '5'])
    assert result.output.strip() == "mediaId: 1\ntagId: 5"

    result = runner.invoke(cli, ['add', 'tagging', '2', '5'])
    assert result.output.strip() == "mediaId: 2\ntagId: 5"

    result = runner.invoke(cli, ['add', 'tagging', '2', '6'])
    assert result.output.strip() == "mediaId: 2\ntagId: 6"

    # Test get all taggings
    result = runner.invoke(cli, ['get', 'taggings'])
    assert result.output.strip() == read_file('output_gettaggings')

    # Test get tags of media id=1
    result = runner.invoke(cli, ['get', 'tags-of-media', '1'])
    assert result.output.strip() == "1\n2\n3\n4\n5"

    # Test get tags of media id=2
    result = runner.invoke(cli, ['get', 'tags-of-media', '2'])
    assert result.output.strip() == "5\n6"

    # Test get medias with tag 1 (Location = Paris)
    result = runner.invoke(cli, ['get', 'medias-with-tag', '1'])
    assert result.output.strip() == "1"

    # Test get medias with tag 5 (Resolution = 1080)
    result = runner.invoke(cli, ['get', 'medias-with-tag', '5'])
    assert result.output.strip() == "1\n2"

    # Test get medias with tag 6 (Resolution = 44100)
    result = runner.invoke(cli, ['get', 'medias-with-tag', '6'])
    assert result.output.strip() == "2"


if __name__ == '__main__':
    test_cli()

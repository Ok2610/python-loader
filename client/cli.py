from __future__ import print_function

import click
import os
import re
import datetime

from grpc_client import LoaderClient
import FileHandler


client = LoaderClient(grpc_host='localhost', grpc_port='50051')

SERVER_ADDRESS = "localhost:50051"

@click.group()
def cli():
    """CLI interface to interact with the M3 database. From this interface you can access and add Medias, Tagsets, Tags, Taggings, Nodes and Hierarchies."""
    pass

#!================ GET functions ======================================================================

@cli.group()
def get():
    """Retrieve stored information from the model"""
    pass


@get.command()
@click.argument("id", type=int)
def media(id): # type: ignore
    """Get a single media with the given ID"""
    if id > 0:
        response = client.get_media(id)
        click.echo(response)
    else:
        click.echo("Error: index must be > 0")


@get.command()
@click.argument("file_uri", type=str)
def media_from_uri(file_uri):
    """Get a an object ID using its URI"""
    response = client.get_id(file_uri)
    click.echo(response)


@get.command()
def medias(): # type: ignore
    """List all the medias stored"""
    response_iterator = client.listall_medias()
    for response in response_iterator:
        click.echo(response)


@get.command()
@click.option("-i", "--id", "tagset_id", type=int, help="Tagset ID")
@click.option("-n", "--name", "tagset_name", type=str, help="Tagset name")
def tagset(tagset_id, tagset_name): # type: ignore
    if tagset_id is not None:
        response = client.get_tagset_by_id(tagset_id)
        click.echo(response)
    elif tagset_name is not None:
        response = client.get_tagset_by_name(tagset_name)
        click.echo(response)
    else:
        click.echo("Usage: loader get tagset [-i ID | -n NAME]\nTry 'loader get tagset --help' for help.\n\nError: Missing argument 'ID' or 'NAME'")


@get.command()
@click.option("-t", "--type", "tagtype_id", type=int, default=-1, help="Tag type filter (default: all).")
def tagsets(tagtype_id): # type: ignore
    """List all existent tagsets"""
    response_iterator = client.listall_tagsets(tagtype_id)
    for response in response_iterator:
        click.echo(response)


@get.command()
@click.argument("id", type=int)
def tag(id): # type: ignore
    """Get a single tag with the given ID"""
    if id > 0:
        response = client.get_tag(id)
        click.echo(response)
    else:
        click.echo("Error: index must be > 0")


@get.command()
@click.option("-tp", "--type", "tagtype_id", type=int, default=-1, help="Tag type filter (default: all).")
@click.option("-ts", "--tagset", "tagset_id", type=int, default=-1, help="Tagset filter (default: all).")
def tags(tagtype_id, tagset_id): # type: ignore
    """List all existent tags"""
    response_iterator = client.listall_tags(tagtype_id=tagtype_id, tagset_id=tagset_id)
    for response in response_iterator:
        click.echo(response)


@get.command()
@click.argument("tag_id", type=int)
def medias_with_tag(tag_id): # type: ignore
    """List the medias with the specified tag"""
    if tag_id > 0:
        response_iterator = client.get_medias_with_tag(tag_id)
        for response in response_iterator:
            click.echo(response)
    else:
        click.echo("Error: index must be > 0")


@get.command()
@click.argument("media_id", type=int)
def tags_of_media(media_id): # type: ignore
    """List the tags of a given media"""
    if media_id > 0:
        response_iterator = client.get_media_tags(media_id)
        for response in response_iterator:
            click.echo(response)
    else:
        click.echo("Error: index must be > 0")


@get.command()
def taggings():
    """List all existent taggings"""
    response_iterator = client.listall_taggings()
    for response in response_iterator:
        click.echo(response)
@get.command()
@click.argument("id", type=int)
def hierarchy(id): # type: ignore
    """Get a single hierarchy with the given ID"""
    if id > 0:
        response = client.get_hierarchy(id)
        click.echo(response)
    else:
        click.echo("Error: index must be > 0")

@get.command()
def hierarchies():
    """List all existent hierarchies"""
    response_iterator = client.listall_hierarchies()
    for response in response_iterator:
        click.echo(response)


@get.command()
@click.argument("id", type=int)
def node(id): # type: ignore
    """Get a single node with the given ID"""
    if id > 0:
        response = client.get_node(id)
        click.echo(response)
    else:
        click.echo("Error: index must be > 0")


@get.command()
@click.argument("node_id", type=int)
def child_nodes(node_id):
    """List the child nodes of a selected node"""
    response_iterator = client.get_child_nodes(node_id)
    for response in response_iterator:
        click.echo(response)

@get.command()
@click.argument("hierarchy_id", type=int)
def nodes_of_hierarchy(hierarchy_id):
    """List all the nodes of a selected hierarchy"""
    response_iterator = client.get_nodes_of_hierarchy(hierarchy_id)
    for response in response_iterator:
        click.echo(response)

#!================ ADD functions ======================================================================


@cli.group()
def add():
    """Add elements to the database model"""
    pass


@add.command()
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--formats",
    "-f",
    multiple=True,
    default=["jpg", "png", "bmp", "mp3", "wav", "flac", "mp4", "avi"],
    help="File formats to include (default: jpg, png, bmp, mp3, wav, flac, mp4, avi)",
)
def media(path, formats): # type: ignore
    """Add a file or multiple files from a specified directory to the database."""
    if os.path.isdir(path):
        response_iterator = client.add_dir(path, formats)
        for response in response_iterator:
            click.echo(response)
    elif os.path.isfile(path):
        response = client.add_file(path)
        click.echo(response)
    else:
        click.echo("Error: invalid path provided.")


@add.command()
@click.argument("name", type=str)
@click.argument("type", type=int, default=1, required=False)
def tagset(name, type): # type: ignore
    """Create a new tagset with specified [name] and [type] (default = 1 - AlphaNumerical)
    Supported types are 1 - Alphanumerical, 2 - Timestamp, 3 - Time, 4 - Date, 5 - Numerical."""
    if type not in range(1,6):
        raise click.BadParameter("Type doesn't exist")
    response = client.add_tagset('%s' % name, type)
    click.echo(response)


def validate_format(ctx, param, value):
    type = ctx.params.get('type_id')

    match type:
        case 1:
            return value
        case 2:
            format_regex = r'^\d{1,4}-\d{1,2}-\d{1,2}/\d{1,2}:\d{2}:\d{2}$'
        case 3:
            format_regex = r'^\d{1,2}:\d{2}:\d{2}$'
        case 4:
            format_regex = r'^\d{1,4}-\d{1,2}-\d{1,2}$'
        case 5:
            if not value.isdigit():
                raise click.BadParameter('Value must be an integer')
            return int(value)
        case _:
            raise click.BadParameter('Invalid type')
        
    if not re.match(format_regex, value):
        raise click.BadParameter('Invalid format')

    return value

@add.command()
@click.argument("tagset_id", type=int)
@click.argument("type_id", type=int)
@click.argument("value", callback=validate_format)
def tag(tagset_id, type_id, value): # type: ignore
    """Create a new tag with specified value. Based on the type, formats are as follows:\n
1 - Alphanumerical: any\n\n2 - Timestamp: YYYY-MM-dd/hh:mm:ss\n\n3 - Time: hh:mm:ss\n\n4 - Date: YYYY-MM-dd\n\n5 - Numerical: integer"""
    response = client.add_tag(tagset_id, type_id, value)
    click.echo(response)
    


@add.command()
@click.argument("media_id", type=int)
@click.argument("tag_id", type=int)
def tagging(tag_id, media_id): #type: ignore
    """Create a tagging between Media ID and Tag ID"""
    response = client.add_tagging(tag_id, media_id)
    click.echo(response)

@add.command()
@click.argument("name", type=str)
@click.argument("tagset_id", type=int)
def hierarchy(name, tagset_id): #type: ignore
    """Create an empty hierarchy with given name and tagset_id"""
    response = client.add_hierarchy(name, tagset_id)
    click.echo(response)

@add.command()
@click.argument("tag_id", type=int)
@click.argument("hierarchy_id", type=int)
@click.argument("parentnode_id", type=int)
def node(tag_id, hierarchy_id, parentnode_id): #type: ignore
    """Create a hierarchy with given name, tagset_id and rootnode_id"""
    response = client.add_node(tag_id, hierarchy_id, parentnode_id)
    click.echo(response)

@add.command()
@click.argument("tag_id", type=int)
@click.argument("hierarchy_id", type=int)
def rootnode(tag_id, hierarchy_id): #type: ignore
    """Create rootnode for given hierarchy"""
    response = client.add_rootnode(tag_id, hierarchy_id)
    click.echo(response)

#!================ DELETE functions ======================================================================

@cli.group()
def delete():
    """Delete elements from database"""
    pass

@delete.command()
@click.argument("id", type=int)
def media(id):
    """Delete a single media with the given ID"""
    if id > 0:
        response = client.delete(id)
        click.echo(response)
    else:
        click.echo("Input error: index must be > 0")


#!================ General functions ======================================================================

@cli.command(name='import')
@click.option(
    "--format",
    "-f",
    default="json",
    help="Format of the import file. Currently supported formats: json, csv.",
)
@click.argument("path", type=click.Path(exists=True))
def import_command(format, path):
    """Add tagsets, objects and tags from a setup file in the selected format."""
    if os.path.isfile(path) & path.lower().endswith(format):
        if format == "json":
            result_message = FileHandler.importJSON(path)
            click.echo(result_message)
        elif format == "csv":
            result_message = FileHandler.importCSV(path)
            click.echo(result_message)
        else:
            click.echo("Error: format '%s' is not supported." % format)
    else:
        click.echo("Error: invalid file path or format.")


@cli.command(name='export')
@click.option(
    "--format",
    "-f",
    default="json",
    help="Format of the import file. Currently supported formats: json.",
)
@click.argument("path", type=click.Path())
def export_command(format, path):
    """Export the current collection configuration to a file in specified format (default: json)."""
    if os.path.isdir(path):
        # Generate the file name automatically
        dir_path = path
        file_id = 1
        today = datetime.datetime.today().strftime("%Y-%m-%d")
        while True:
            path = f"{dir_path}/{today}-{file_id}.{format}"
            if not os.path.isfile(path):
                break
            file_id += 1

    if format == "json":
        result_message = FileHandler.exportJSON(path)
        click.echo("File created at %s" % path)
        click.echo(result_message)
    elif format == "csv":
        result_message = FileHandler.exportCSV(path)
        click.echo("File created at %s" % path)
        click.echo(result_message)
    else:
        click.echo("Error: format '%s' is not supported." % format)



@cli.command()
def reset():
    """Reset the database"""
    if click.confirm(
        "Do you want to reset the database ? All data will be lost.", default=False
    ):
        response = client.reset()
        click.echo(response)




if __name__ == "__main__":
    cli(obj={})

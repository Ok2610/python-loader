from __future__ import print_function

import click
import os
from pprint import pformat
import re
import datetime

import grpc
import dataloader_pb2
import dataloader_pb2_grpc

from grpc_client import LoaderClient
import FileHandler


client = LoaderClient(grpc_host='localhost', grpc_port='50051')

SERVER_ADDRESS = "localhost:50051"


@click.group()
@click.option("--debug","-d", is_flag=True, help="Enable debug mode.")
@click.pass_context
def cli(ctx, debug):
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug

#!================ GET functions ======================================================================

@cli.group()
@click.pass_context
def get(ctx):
    """Retrieve stored information from the model"""
    pass


@get.command()
@click.argument("id", type=int)
@click.pass_context
def media(ctx, id): # type: ignore
    """Get a single media with the given ID"""
    if ctx.obj["DEBUG"]:
        click.echo(f"DEBUG: Getting media with ID: {id}")

    if id > 0:
        response = client.get_media(id)
        click.echo(response)
    else:
        click.echo("ERROR: index must be > 0")


@get.command()
@click.argument("file_uri", type=str)
@click.pass_context
def media_from_uri(ctx, file_uri):
    """Get a an object ID using its URI"""
    if ctx.obj["DEBUG"]:
        click.echo(f"DEBUG: Getting media with ID: {id}")

    response = client.get_id(file_uri)
    click.echo(response)


@get.command()
@click.pass_context
def medias(ctx): # type: ignore
    """List all the medias stored"""
    if ctx.obj["DEBUG"]:
        click.echo("DEBUG: Getting all medias")

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
@click.pass_context
def tagsets(ctx): # type: ignore
    """List all existent tagsets"""
    response_iterator = client.listall_tagsets()
    for response in response_iterator:
        click.echo(response)


@get.command()
@click.argument("id", type=int)
@click.pass_context
def tag(ctx, id): # type: ignore
    """Get a single tag with the given ID"""
    if ctx.obj["DEBUG"]:
        click.echo(f"DEBUG: Getting tag with ID: {id}")
    if id > 0:
        response = client.get_tag(id)
        click.echo(response)
    else:
        click.echo("ERROR: index must be > 0")


@get.command()
@click.pass_context
def tags(ctx): # type: ignore
    """List all existent tags"""
    response_iterator = client.listall_tags()
    for response in response_iterator:
        click.echo(response)


@get.command()
@click.argument("tag_id", type=int)
@click.pass_context
def medias_with_tag(ctx, tag_id): # type: ignore
    """List the medias with the specified tag"""
    if tag_id > 0:
        response_iterator = client.get_medias_with_tag(tag_id)
        for response in response_iterator:
            click.echo(response)
    else:
        click.echo("ERROR: index must be > 0")


@get.command()
@click.argument("media_id", type=int)
@click.pass_context
def tags_of_media(ctx, media_id): # type: ignore
    """List the tags of a given media"""
    if media_id > 0:
        response_iterator = client.get_media_tags(media_id)
        for response in response_iterator:
            click.echo(response)
    else:
        click.echo("ERROR: index must be > 0")


@get.command()
@click.pass_context
def taggings(ctx):
    """List all existent taggings"""
    response_iterator = client.listall_taggings()
    for response in response_iterator:
        click.echo(response)


#!================ ADD functions ======================================================================


@cli.group()
@click.pass_context
def add(ctx):
    """Add elements to the database model"""
    pass


@add.command()
@click.pass_context
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--formats",
    "-f",
    multiple=True,
    default=["jpg", "png", "bmp", "mp3", "wav", "flac", "mp4", "avi"],
    help="File formats to include (default: jpg, png, bmp, mp3, wav, flac, mp4, avi)",
)
def media(ctx, path, formats): # type: ignore
    """Add a file or multiple files from a specified directory to the database."""
    if ctx.obj["DEBUG"]:
        click.echo("DEBUG: Adding files from directory.")

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
@click.pass_context
def tagset(ctx, name, type): # type: ignore
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
@click.pass_context
def tag(ctx, tagset_id, type_id, value): # type: ignore
    """Create a new tag with specified value. Based on the type, formats are as follows:\n
1 - Alphanumerical: any\n\n2 - Timestamp: YYYY-MM-dd/hh:mm:ss\n\n3 - Time: hh:mm:ss\n\n4 - Date: YYYY-MM-dd\n\n5 - Numerical: integer"""
    response = client.add_tag(tagset_id, type_id, value)
    click.echo(response)
    


@add.command()
@click.argument("media_id", type=int)
@click.argument("tag_id", type=int)
@click.pass_context
def tagging(ctx, tag_id, media_id): #type: ignore
    """Create a tagging between Media ID and Tag ID"""
    if ctx.obj["DEBUG"]:
        click.echo(f"DEBUG: Attributing tag {tag_id} to media {media_id}")
    response = client.add_tagging(tag_id, media_id)
    click.echo(response)


#!================ DELETE functions ======================================================================

@cli.group()
@click.pass_context
def delete(ctx):
    """Delete elements from database"""
    pass

@delete.command()
@click.argument("id", type=int)
@click.pass_context
def media(ctx, id):
    """Delete a single media with the given ID"""
    if ctx.obj["DEBUG"]:
            click.echo(f"DEBUG: Deleting media with ID: {id}")
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
@click.pass_context
def import_command(ctx, format, path):
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
@click.pass_context
def export_command(ctx, format, path):
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
@click.pass_context
def reset(ctx):
    """Reset the database"""
    if click.confirm(
        "Do you want to reset the database ? All data will be lost.", default=False
    ):
        if ctx.obj["DEBUG"]:
            click.echo(f"DEBUG: Resetting database...")
        with grpc.insecure_channel(SERVER_ADDRESS) as channel:
            stub = dataloader_pb2_grpc.DataLoaderStub(channel)
            request = dataloader_pb2.EmptyRequest()
            response = stub.resetDatabase(request)
            if response.success:
                click.echo("Database was successfully reset.")
            else:
                click.echo("Error: could not reset database.")
    else:
        click.echo("Operation cancelled.")




if __name__ == "__main__":
    cli(obj={})

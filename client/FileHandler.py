import json
import csv
import datetime
import os
import time
import grpc_client

def importJSON(path):
    client = grpc_client.LoaderClient()
    try:
        with open(path, 'r') as file:
            data = json.load(file)
            tagsets = data.get('tagsets', [])
            id_map = {}
            iterator = 0
            for tagset_item in tagsets:
                iterator += 1
                tagset_name = tagset_item.get('name')
                tagset_type = tagset_item.get('type')
                if tagset_name and tagset_type:
                    response = client.add_tagset(tagset_name, tagset_type)
                    if type(response) is dict: # If the tagset already exists
                        response = client.get_tagset_by_name(tagset_name)
                    id_map[iterator] = (response.id, response.tagTypeId)                         # type: ignore
                else:
                    print(f"Invalid item in tagsets: {tagset_item}")
            medias = data.get('medias', [])
            for media_item in medias:
                path = media_item.get('path')
                if media_item:
                    media_response = client.add_file(path)
                    tags = media_item.get('tags', [])
                    for tag_item in tags:
                        tagset_id = id_map[tag_item.get('tagset_id')][0]
                        tagtype_id = id_map[tag_item.get('tagset_id')][1]
                        value = tag_item.get('value')
                        tag_response = client.add_tag(tagset_id, tagtype_id, value)
                        client.add_tagging(media_id=media_response.id, tag_id=tag_response.id) # type: ignore
                else:
                    print(f"Invalid item in medias: {media_item}")
            
                        
    except FileNotFoundError:
        print(f"File not found: {path}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")


def importCSV(path):
    client = grpc_client.LoaderClient()
    try:
        with open(path, 'r') as file:
            reader = csv.reader(file, delimiter=';')
            first_line = next(reader)
            id_map = {}
            iterator = 0

            # Process the first line, i.e the tagsets
            # The syntax is [tagset_name_1];[tagset_type_1];[tagset_name_2];[tagset_type_2]...
            current_name = None
            for item in first_line:
                if item and current_name is None:
                    iterator += 1
                    current_name = item
                elif item.isdigit() and current_name is not None:
                    response = client.add_tagset(current_name, int(item))
                    if type(response) is dict: # If the tagset already exists
                        response = client.get_tagset_by_name(current_name)
                    id_map[iterator] = (response.id, response.tagTypeId)                            # type: ignore
                    current_name = None
                else:
                    print(f"Invalid item in tagsets: {tagset_item}")                  # type: ignore

            # We the process the following lines, containing medias and their tags: 
            # Syntax: [path];[id_tag1];[value1];[id_tag2];[value2]...[id_tagN];[valueN]
            iterator = 1
            for row in reader:
                try:
                    iterator += 1
                    if not row:
                        continue        # Skip empty rows
                    path = row[0]
                    media_response = client.add_file(path)
                    if not media_response.id:                       # type: ignore
                        raise Exception(f"Invalid media, could not add to DB")
                    for i in range(1, len(row), 2):
                        tagset_id = id_map[int(row[i])][0]          # we have to use the correct tagset_id in db
                        tagtype_id = id_map[int(row[i])][1]
                        if (not tagset_id) or (not tagtype_id):
                            raise Exception(f"Invalid tag definition: {tagset_id}:{tagtype_id}")
                        value = row[i+1]
                        tag_response = client.add_tag(tagset_id, tagtype_id, value)
                        if not tag_response.id:                     # type: ignore
                            raise Exception("Could not add tag")
                        tagging_response = client.add_tagging(media_id=media_response.id, tag_id=tag_response.id) # type: ignore
                        if not tagging_response.mediaId:            # type: ignore
                            raise Exception("Could not add tag")
                except Exception as e:
                    print(f"Error at line {iterator}: {e}")

    except FileNotFoundError:
        print(f"File not found: {path}")



def exportJSON(path):
    client = grpc_client.LoaderClient()
    tagsets = []
    medias = []

    response_tagsets = client.listall_tagsets(-1)
    for tagset_response in response_tagsets:
        tagsets.append(
            {"name": tagset_response.name,      # type: ignore
             "type": tagset_response.tagTypeId   # type: ignore
            })
    
    response_medias = client.listall_medias()
    for media_response in response_medias:
        # print(media_response)
        media_path = media_response.file_uri                              # type: ignore
        tags = []
        tag_ids = client.get_media_tags(media_response.id)          # type: ignore
        for id_tag in tag_ids:
            if(type(id_tag) is not int):
                print(id_tag, media_response.id)     # type: ignore
            else:
                tag_response = client.get_tag(id_tag)                     
                tagset_id = tag_response.tagSetId                               # type: ignore
                possible_values = [tag_response.alphanumerical.value,           # type: ignore
                                   tag_response.timestamp.value,                # type: ignore
                                   tag_response.time.value,                     # type: ignore
                                   tag_response.date.value,                     # type: ignore
                                   tag_response.numerical.value]                # type: ignore
                value = next(value for value in possible_values if value != "")     # Due to grpc standrads, the numerical value won't be Null if not initialised, 
                                                                                    # but zero. Thus we take advantage of the fact that it is the last possible value and
                                                                                    # that only one value can be set for a specified tag

                tags.append({"tagset_id":tagset_id, "value":value})

        medias.append({"path": media_path, "tags": tags})

    data = {"tagsets": tagsets, "medias": medias}
    print("All data loaded, dumping json file")
    with open(path, "w") as json_file:
        json.dump(data, json_file, indent=4)


def exportCSV(path):
    client = grpc_client.LoaderClient()
    tagsets = []
    medias = []

    # Prepare the header line with tagset names and types
    header = []
    response_tagsets = client.listall_tagsets(-1)
    for tagset_response in response_tagsets:
        header.extend([f"\"{tagset_response.name}\"", f"{tagset_response.tagTypeId}"]) # type: ignore
        
    # Write the data to the CSV file
    with open(path, "w", newline="", encoding="UTF-8") as file:
        csv_writer = csv.writer(file, delimiter=";", quoting=csv.QUOTE_NONE, escapechar='', quotechar='')
        csv_writer.writerow(header)


        response_medias = client.listall_medias()
        for media_response in response_medias:
            path = media_response.file_uri                              # type: ignore
            row = [f'\"{path}\"']
            tag_ids = client.get_media_tags(media_response.id)          # type: ignore
            for id_tag in tag_ids:
                tag_response = client.get_tag(int(id_tag))                      # type: ignore
                tagset_id = tag_response.tagSetId                               # type: ignore
                possible_values = [tag_response.alphanumerical.value,           # type: ignore
                                tag_response.timestamp.value,                # type: ignore
                                tag_response.time.value,                     # type: ignore
                                tag_response.date.value,                     # type: ignore
                                tag_response.numerical.value]                # type: ignore
                value = next(value for value in possible_values if value != "")
                row.extend([f'{tagset_id}', f'\"{value}\"'])
                
            csv_writer.writerow(row)


if __name__ == "__main__":
    exportJSON("./export.json")
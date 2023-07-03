import os
import json

import grpc
import dataloader_pb2
import dataloader_pb2_grpc

from google.protobuf.json_format import MessageToJson


class LoaderClient:
    def __init__(self, grpc_host='localhost', grpc_port='50051') -> None:
        self.grpc_channel = grpc.insecure_channel('%s:%s' % (grpc_host, grpc_port))
        self.grpc_stub = dataloader_pb2_grpc.DataLoaderStub(self.grpc_channel)

    #!================ Media/media functions ======================================================================

    def get_media(self, id: int):
    # Get a single media with the given ID
        try:
            request = dataloader_pb2.GetMediaByIdRequest(id=id)
            response = self.grpc_stub.getMediaById(request)
            if response.success:
                return(response.media)
            else:
                raise Exception("Could not find media with the given ID")
        except Exception as e:
            return json.loads('{"Error": "%s"}' % e)

    
    def get_id(self, file_uri: str):
    # Get a an media ID using its URI
        try:
            request = dataloader_pb2.GetMediaIdFromURIRequest(uri=file_uri)
            response = self.grpc_stub.getMediaIdFromURI(request)
            if response.success:
                return response.id
            else:
                raise Exception("URI not found in database.")
        except Exception as e:
            return json.loads('{"Error": "%s"}' % e)


    def listall_medias(self):
    # List all the medias stored
        request = dataloader_pb2.EmptyRequest()
        response_iterator = self.grpc_stub.getMedias(request)
        for response in response_iterator:
            if response.success:
                yield response.media
            else:
                yield {'Error': 'Request failed.'}
        

    def add_dir(self, directory: str, formats):
    # Add files from a specified directory to the database.
        file_count = 0
        def add_media_requests_generator(directory: str):
            nonlocal file_count
            for file in os.listdir(directory):
                if any(file.endswith('.' + ext) for ext in formats):
                    file_path = os.path.abspath(os.path.join(directory, file))
                    if file.lower().endswith(('jpg', 'png', 'bmp')):
                        file_type = 0		# Image
                    elif file.lower().endswith(('mp3', 'wav', 'flac')):
                        file_type = 1		# Audio
                    elif file.lower().endswith(('mp4', 'avi')):
                        file_type = 2		# Video
                    else: 
                        file_type = 3		# Other
                    file_count += 1
                    request = dataloader_pb2.AddMediaRequest(media={
                        "file_uri": file_path,
                        "file_type": file_type,
                        "thumbnail_uri": file_path
                        })
                    yield request
        
       
        response_iterator = self.grpc_stub.addMedias(add_media_requests_generator(directory))
        for response in response_iterator:
            if response.success:
                yield {'Success': 'added %d medias to database.' % (response.count)}
            else:
                yield {'Error': 'failed to add 1 batch of medias to database.'}
        yield {'Info': '%d files were found in the directory.' % file_count}


    def add_file(self, path: str):
    # Add a specific file to the database.
        file_path = os.path.abspath(path)
        if path.lower().endswith(('jpg', 'png', 'bmp')):
            file_type = 0		# Image
        elif path.lower().endswith(('mp3', 'wav', 'flac')):
            file_type = 1		# Audio
        elif path.lower().endswith(('mp4', 'avi')):
            file_type = 2		# Video
        else: 
            file_type = 3		# Other
        request = dataloader_pb2.AddMediaRequest(media={
            "file_uri": file_path,
            "file_type": file_type,
            "thumbnail_uri": file_path
            })
        
        response = self.grpc_stub.addMedia(request)
        if response.success:
            return response.media
        else:
            return {'Error': 'failed to add 1 batch of medias to database.'}


    # Delete a single media with the given ID
    def delete(self, id: int):
        request = dataloader_pb2.DeleteMediaRequest(id=id)
        response = self.grpc_stub.deleteMedia(request)
        if response.success:
            return {'Success': 'media removed from database.'}
        else:
            return {'Error': 'could not remove media from database'}


    #!================ Tagset functions ======================================================================

    def listall_tagsets(self):
        request = dataloader_pb2.EmptyRequest()
        response_iterator = self.grpc_stub.getTagSets(request)
        for response in response_iterator:
            if response.success:
                yield response.tagset
            else:
                yield {'Error': 'Request failed.'}

    def add_tagset(self, name: str, tagtype_id: int):
        request = dataloader_pb2.CreateTagSetRequest(name=name, tagTypeId=tagtype_id)
        response = self.grpc_stub.createTagSet(request)
        if response.success:
            return response.tagset
        else:
            return {'Error': 'could not create tagset.'}
    
    def get_tagset_by_id(self, id: int):
        request = dataloader_pb2.GetTagSetRequestById(id=id)
        response = self.grpc_stub.getTagSetById(request)
        if response.success :
            return response.tagset
        else:
            return {'Error': 'could not find tagset with the given ID.'}
    
    def get_tagset_by_name(self, name: str):
        request = dataloader_pb2.GetTagSetRequestByName(name=name)
        response = self.grpc_stub.getTagSetByName(request)
        if response.success :
            return response.tagset
        else:
            return {'Error': 'could not find tagset with the given name.'}
        
    #!================ Tag functions ======================================================================
    
    def listall_tags(self):
        request = dataloader_pb2.EmptyRequest()
        response_iterator = self.grpc_stub.getTags(request)
        for response in response_iterator:
            if response.success:
                yield response.tag
            else:
                yield {'Error': 'Request failed.'}

    def add_tag(self, tagset_id: int, tagtype_id: int, value):
        match tagtype_id:
            case 1: 
                request = dataloader_pb2.CreateTagRequest(
                    tagSetId=tagset_id, 
                    tagTypeId=tagtype_id,
                    alphanumerical = dataloader_pb2.AlphanumericalValue(value=value)
                )
            case 2: 
                value = value.replace('/', ' ')
                request = dataloader_pb2.CreateTagRequest(
                    tagSetId=tagset_id, 
                    tagTypeId=tagtype_id,
                    timestamp = dataloader_pb2.TimeStampValue(value=value)
                )
            case 3: 
                request = dataloader_pb2.CreateTagRequest(
                    tagSetId=tagset_id, 
                    tagTypeId=tagtype_id,
                    time = dataloader_pb2.TimeValue(value=value)
                )
            case 4: 
                request = dataloader_pb2.CreateTagRequest(
                    tagSetId=tagset_id, 
                    tagTypeId=tagtype_id,
                    date = dataloader_pb2.DateValue(value=value)
                )
            case 5: 
                request = dataloader_pb2.CreateTagRequest(
                    tagSetId=tagset_id, 
                    tagTypeId=tagtype_id,
                    numerical = dataloader_pb2.NumericalValue(value=int(value))
                )
            case _:
                return {'Error': 'Not a valid tag type: range is 1-5'}
            
        response = self.grpc_stub.createOrGetTag(request)
        if response.success:
            return response.tag
        else:
            return {'Error': 'could not create tag'}
    
    def get_tag(self, id: int):
        request = dataloader_pb2.GetTagRequest(id=id)
        response = self.grpc_stub.getTag(request)
        if response.success :
            return response.tag
        else:
            return {'Error': 'could not find tag with given ID'}

    #!================ Tagging functions ======================================================================

    def listall_taggings(self):
        request = dataloader_pb2.EmptyRequest()
        response_iterator = self.grpc_stub.getTaggings(request)
        for response in response_iterator:
            if response.success:
                yield response.tagging
            else:
                yield {'Error': 'Request failed.'}

    def add_tagging(self, tag_id: int, media_id: int):
        request = dataloader_pb2.CreateTaggingRequest(
            tagId=tag_id,
            mediaId=media_id
        )
        response = self.grpc_stub.createTagging(request)
        if response.success:
            return response.tagging
        else:
            return {'Error': 'could not create tagset'}
    

    def get_medias_with_tag(self, id: int):
        request = dataloader_pb2.GetMediasWithTagRequest(tagId=id)
        response_iterator = self.grpc_stub.getMediasWithTag(request)
        for response in response_iterator:
            if response.success:
                yield response.id
            else:
                yield {'Error': 'could not retrieve medias with the given tag_id'}

    def get_media_tags(self, id: int):
        request = dataloader_pb2.GetMediaTagsRequest(mediaId=id)
        response_iterator = self.grpc_stub.getMediaTags(request)
        for response in response_iterator:
            if response.success:
                yield response.id
            else:
                yield {'Error': 'could not retrieve tags with the given media_id'}

    #!================ DB management ======================================================================

    # Reset the database
    def reset(self, ctx):
        request = dataloader_pb2.EmptyRequest()
        response = self.grpc_stub.resetDatabase(request)
        if response.success:
            return {'Success': 'database was successfully reset.'}
        else:
            return {'Error': 'could not reset database'}
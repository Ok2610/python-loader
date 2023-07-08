from concurrent import futures
import logging
import psycopg2
import psycopg2.extras
import sys
import asyncio
import shortuuid
from typing import Iterable

import grpc
import dataloader_pb2
from dataloader_pb2_grpc import DataLoaderServicer, add_DataLoaderServicer_to_server

MODE = "add"
BATCH_SIZE = 50


class DataLoader(DataLoaderServicer):
    def __init__(self) -> None:
        super().__init__()
        self.conn = psycopg2.connect(
            database="loader-testing",
            user="postgres",
            password="root",
            host="localhost",
            port="5432",
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.cursor.execute("select version()")
        data = self.cursor.fetchone()
        print("Connection established to: ", data)
        if MODE == "reset":
            self.cursor.execute(open("init.sql", "r").read())
            print("DB has been reset")

    def __del__(self):
        self.cursor.close()
        self.conn.close()


    #!================ Medias =============================================================================
    def getMediaById(self, request: dataloader_pb2.IdRequest, context) -> dataloader_pb2.MediaResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getMediaById request with id=%d" % (thread_id, request.id))
        try:
            sql = """SELECT * FROM public.cubeobjects WHERE id=%d""" % request.id
            self.cursor.execute(sql)
            result = self.cursor.fetchall()[0]
            # print("[%s] -> Element fetched from DB, sending back to client..." % thread_id)
            return dataloader_pb2.MediaResponse(
                success=True,
                media=dataloader_pb2.Media(
                        id= result["id"],
                        file_uri= result["file_uri"],
                        file_type= result["file_type"],
                        thumbnail_uri= result["thumbnail_uri"]
                    )
            )
        except Exception as e:
            print("[%s] -> No results were fetched, sending error message to client..." % thread_id)
            return dataloader_pb2.MediaResponse(success=False)


    def getMedias(self, request: dataloader_pb2.EmptyRequest, context):
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getMedias request." % thread_id)
        count = 0
        try:
            sql = """SELECT * FROM public.cubeobjects"""
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            for row in res:
                count += 1
                yield dataloader_pb2.MediaResponse(
                    success=True,
                    media=dataloader_pb2.Media(
                        id= row["id"],
                        file_uri= row["file_uri"],
                        file_type= row["file_type"],
                        thumbnail_uri= row["thumbnail_uri"]
                    )
                )
        except Exception as e:
            yield dataloader_pb2.MediaResponse(success=False)
            print("[%s] -> Error: %s" % (thread_id, str(e)))
        # print("[%s] -> Fetched %d items from database" % (thread_id, count))


    def getMediaIdFromURI(self, request: dataloader_pb2.GetMediaIdFromURIRequest, context) -> dataloader_pb2.IdResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getMediaIdFromURI request with URI=%s" % (thread_id, request.uri))
        try:
            sql = (
                """SELECT id FROM public.cubeobjects WHERE file_uri='%s'"""
                % request.uri
            )
            print(sql)
            self.cursor.execute(sql)
            result = self.cursor.fetchall()[0]
            # print("[%s] -> Element fetched from DB, sending back ID to client..." % thread_id)
            return dataloader_pb2.IdResponse(success=True, id=result[0])
        except Exception as e:
            print("[%s] -> No results were fetched, sending error message to client..." % thread_id)
            return dataloader_pb2.IdResponse(success=False)



    def addMedia(self, request: dataloader_pb2.AddMediaRequest, context):
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received addMedia request." % thread_id)
        request_counter = 0
        sql = """INSERT INTO public.cubeobjects (file_uri, file_type, thumbnail_uri)
VALUES ('%s', %d, '%s') RETURNING *;""" % (
                request.media.file_uri,
                request.media.file_type,
                request.media.thumbnail_uri,
            )
        try:
            self.cursor.execute(sql)
            response = self.cursor.fetchall()[0]
            return dataloader_pb2.MediaResponse(
                success=True,
                media=dataloader_pb2.Media(
                    id=response['id'],
                    file_uri=response['file_uri'],
                    file_type=response['file_type'],
                    thumbnail_uri=response['thumbnail_uri']
                )
            )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            return dataloader_pb2.MediaResponse(success=False)

    def addMedias(self, request_iterator, context):
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received addMedias request." % thread_id)
        request_counter = 0
        sql = ""
        for request in request_iterator:
            request_counter += 1
            if request_counter % BATCH_SIZE == 1:
                sql = """INSERT INTO public.cubeobjects (file_uri, file_type, thumbnail_uri)
VALUES
"""
            sql += "('%s', %d, '%s')," % (
                request.media.file_uri,
                request.media.file_type,
                request.media.thumbnail_uri,
            )
            if request_counter % BATCH_SIZE == 0:
                sql = sql[:-1] + ";COMMIT;"
                try:
                    self.cursor.execute(sql)
                    response = dataloader_pb2.AddMediaStreamResponse(
                        success=True, count=request_counter
                    )
                except Exception as e:
                    response = dataloader_pb2.AddMediaStreamResponse(success=False)
                    print("[%s] -> Error: packet addition failed." % thread_id)
                yield response

        if request_counter % BATCH_SIZE > 0:
            sql = sql[:-1] + ";COMMIT;"
            try:
                self.cursor.execute(sql)
                response = dataloader_pb2.AddMediaStreamResponse(
                    success=True, count=request_counter
                )
            except Exception as e:
                response = dataloader_pb2.AddMediaStreamResponse(success=False)
                print("[%s] -> Error: %s" % (thread_id, str(e)))
            yield response
        # print("[%s] -> Operation completed, added %d elements to DB" % (thread_id, request_counter))


    def deleteMedia(self, request: dataloader_pb2.IdRequest, context) -> dataloader_pb2.StatusResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received deleteMedia request with id=%d" % (thread_id, request.id))
        try:
            sql = """DELETE FROM public.cubeobjects WHERE id=%d;""" % request.id
            self.cursor.execute(sql)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                # print("[%s] -> SUCCESS: Element deleted from DB." % thread_id)
                return dataloader_pb2.StatusResponse(success=True)
            else:
                raise Exception("Element not found")
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            return dataloader_pb2.StatusResponse(success=False)


    #!================ TagSets ============================================================================
    def getTagSets(self, request: dataloader_pb2.EmptyRequest, context):
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getTagSets request." % thread_id)
        count = 0
        try:
            sql = """SELECT * FROM public.tagsets"""
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            for row in res:
                count += 1
                yield dataloader_pb2.TagSetResponse(
                    success=True,
                    tagset=dataloader_pb2.TagSet(
                        id= row['id'],
                        name= row['name'],
                        tagTypeId= row['tagtype_id']
                    )
                )
        except Exception as e:
            yield dataloader_pb2.TagSetResponse(success=False)
            print("[%s] -> Error: %s" % (thread_id, str(e)))
        # print("[%s] -> Fetched %d items from database" % (thread_id, count))

    def getTagSetById(self, request: dataloader_pb2.IdRequest, context) -> dataloader_pb2.TagSetResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getTagsetById request with id=%d" % (thread_id, request.id))
        try:
            sql = """SELECT * FROM public.tagsets WHERE id=%d""" % request.id
            self.cursor.execute(sql)
            result = self.cursor.fetchall()[0]
            # print("[%s] -> Element fetched from DB, sending back to client..." % thread_id)
            return dataloader_pb2.TagSetResponse(
                success=True,
                tagset=dataloader_pb2.TagSet(
                    id= result['id'],
                    name= result['name'],
                    tagTypeId= result['tagtype_id']
                )
            )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            return dataloader_pb2.TagSetResponse(success=False)
        

    def getTagSetByName(self, request: dataloader_pb2.GetTagSetRequestByName, context) -> dataloader_pb2.TagSetResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getTagSetByName request with name=%s" % (thread_id, request.name))
        try:
            sql = """SELECT * FROM public.tagsets WHERE name='%s'""" % request.name
            self.cursor.execute(sql)
            result = self.cursor.fetchall()[0]
            # print("[%s] -> Element fetched from DB, sending back to client..." % thread_id)
            return dataloader_pb2.TagSetResponse(
                success=True,
                tagset=dataloader_pb2.TagSet(
                    id= result['id'],
                    name= result['name'],
                    tagTypeId= result['tagtype_id']
                )
            )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            return dataloader_pb2.TagSetResponse(success=False)


    def createTagSet(self, request: dataloader_pb2.CreateTagSetRequest, context) -> dataloader_pb2.TagSetResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received createTagSet request with name=%s and tag_type=%d" % (thread_id, request.name, request.tagTypeId))
        sql = "INSERT INTO public.tagsets (name, tagtype_id) VALUES ('%s', %d) RETURNING id, name, tagtype_id;" % (request.name, request.tagTypeId)
        try:
            self.cursor.execute(sql)
            inserted_tagset = self.cursor.fetchall()[0]
            response = dataloader_pb2.TagSetResponse(
                success=True,
                tagset=dataloader_pb2.TagSet(
                    id= inserted_tagset['id'],
                    name= inserted_tagset['name'],
                    tagTypeId= inserted_tagset['tagtype_id']
                )
            )
            self.conn.commit()
            # print("[%s] -> Operation completed, added 1 element to DB" % thread_id)
            return response
        except Exception as e:
            response = dataloader_pb2.TagSetResponse(success=False)
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            return response
        

    #!================ Tags ===============================================================================
    def getTags(self, request: dataloader_pb2.EmptyRequest, context):
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getTags request." % thread_id)
        count = 0
        try:
            sql = """SELECT
    t.id,
    t.tagtype_id,
    t.tagset_id,
    ant.name as text_value,
	tst.name as timestamp_value,
	tt.name as time_value,
	dt.name as date_value,
	nt.name as num_value
FROM
    public.tags t
LEFT JOIN
    public.alphanumerical_tags ant ON t.id = ant.id
LEFT JOIN
    public.timestamp_tags tst ON t.id = tst.id
LEFT JOIN
    public.time_tags tt ON t.id = tt.id
LEFT JOIN
    public.date_tags dt ON t.id = dt.id
LEFT JOIN
    public.numerical_tags nt ON t.id = nt.id
            """
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            for row in res:
                count += 1
                match row['tagtype_id']:
                    case 1:
                        tag = dataloader_pb2.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            alphanumerical = dataloader_pb2.AlphanumericalValue(value=row['text_value'])
                        )
                    case 2:
                        tag = dataloader_pb2.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            timestamp =  dataloader_pb2.TimeStampValue(value=str(row['timestamp_value']))
                        )
                    case 3:
                        tag = dataloader_pb2.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            time = dataloader_pb2.TimeValue(value=str(row['time_value']))
                        )
                    case 4:
                        tag = dataloader_pb2.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            date = dataloader_pb2.DateValue(value=str(row['date_value']))
                        )
                    case 5:
                        tag = dataloader_pb2.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            numerical = dataloader_pb2.NumericalValue(value=row['num_value'])
                        )
                    case _:
                        tag = {}
                yield dataloader_pb2.TagResponse(
                    success=True,
                    tag=tag
                    )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            yield dataloader_pb2.TagResponse(success=False)
        # print("[%s] -> Fetched %d items from database" % (thread_id, count))


    def getTag(self, request: dataloader_pb2.IdRequest, context) -> dataloader_pb2.TagResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getTag request with id=%d" % (thread_id, request.id))
        try:
            sql = ("""SELECT
    t.id,
    t.tagtype_id,
    t.tagset_id,
    ant.name as text_value,
	tst.name as timestamp_value,
	tt.name as time_value,
	dt.name as date_value,
	nt.name as num_value
FROM
    (SELECT * FROM public.tags WHERE id = %d) t
LEFT JOIN
    public.alphanumerical_tags ant ON t.id = ant.id
LEFT JOIN
    public.timestamp_tags tst ON t.id = tst.id
LEFT JOIN
    public.time_tags tt ON t.id = tt.id
LEFT JOIN
    public.date_tags dt ON t.id = dt.id
LEFT JOIN
    public.numerical_tags nt ON t.id = nt.id""" % request.id)
            self.cursor.execute(sql)
            result = self.cursor.fetchall()[0]
            match result['tagtype_id']:
                case 1:
                    tag = dataloader_pb2.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        alphanumerical = dataloader_pb2.AlphanumericalValue(value=result['text_value'])
                    )
                case 2:
                    tag = dataloader_pb2.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        timestamp =  dataloader_pb2.TimeStampValue(value=str(result['timestamp_value']))
                    )
                case 3:
                    tag = dataloader_pb2.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        time = dataloader_pb2.TimeValue(value=str(result['time_value']))
                    )
                case 4:
                    tag = dataloader_pb2.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        date = dataloader_pb2.DateValue(value=str(result['date_value']))
                    )
                case 5:
                    tag = dataloader_pb2.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        numerical = dataloader_pb2.NumericalValue(value=result['num_value'])
                    )
                case _:
                    tag = {}
            # print("[%s] -> Fetched 1 tag from database" % thread_id)
            return dataloader_pb2.TagResponse(
                success=True,
                tag=tag
                )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            return dataloader_pb2.TagResponse(success=False)


    def createOrGetTag(self, request: dataloader_pb2.CreateTagRequest, context) -> dataloader_pb2.TagResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received createOrGetTag request with tagset_id=%d and tagtype_id=%d" % (thread_id, request.tagSetId, request.tagTypeId))
        tagset_id = request.tagSetId
        tagtype_id = request.tagTypeId
        try:
            sql = """SELECT t.id, t.tagtype_id, t.tagset_id, a.name as value FROM 
(SELECT * FROM public.tags WHERE tagset_id = %d AND tagtype_id = %d) t
LEFT JOIN """ % (tagset_id, tagtype_id)
            match request.tagTypeId:
                case 1: 
                    sql += ("public.alphanumerical_tags a ON t.id = a.id WHERE a.name = '%s'" 
                            % request.alphanumerical.value)
                case 2:                                                              
                    sql += ("public.timestamp_tags a ON t.id = a.id WHERE a.name = '%s'"
                             % request.timestamp.value)
                case 3:                                                              
                    sql += ("public.time_tags a ON t.id = a.id WHERE a.name = '%s'" 
                            % request.time.value)
                case 4:                                                              
                    sql += ("public.date_tags a ON t.id = a.id WHERE a.name = '%s'" 
                            % request.date.value)
                case 5: 
                    sql += ("public.numerical_tags a ON t.id = a.id WHERE a.name = %i" 
                            % request.numerical.value)
                case _:
                    raise Exception("Not a valid tag type: range is 1-5")
            self.cursor.execute(sql)

            # Check if cursor has result, i.e. the tag already exists
            if self.cursor.rowcount > 0:
                print("[%s] -> Tag already present in DB, returning tag info." % thread_id)
                result = self.cursor.fetchall()[0]
                match request.tagTypeId:
                    case 1:
                        return dataloader_pb2.TagResponse(
                            success=True,
                            tag = dataloader_pb2.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                alphanumerical= dataloader_pb2.AlphanumericalValue(value=result['value'])
                            ))
                    case 2:
                        return dataloader_pb2.TagResponse(
                            success=True,
                            tag = dataloader_pb2.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                timestamp= dataloader_pb2.TimeStampValue(value=str(result['value']))
                            ))
                    case 3:
                        return dataloader_pb2.TagResponse(
                            success=True,
                            tag = dataloader_pb2.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                time = dataloader_pb2.TimeValue(value=str(result['value']))
                            ))
                    case 4:
                        return dataloader_pb2.TagResponse(
                            success=True,
                            tag = dataloader_pb2.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                date = dataloader_pb2.DateValue(value=str(result['value']))
                            ))
                    case 5:
                        return dataloader_pb2.TagResponse(
                            success=True,
                            tag = dataloader_pb2.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                numerical= dataloader_pb2.NumericalValue(value=result['value'])
                            ))
            
            sql = ("SELECT * FROM public.tagsets WHERE id = %d AND tagtype_id = %d" 
                    % (tagset_id, tagtype_id))
            
            self.cursor.execute(sql)
            if self.cursor.rowcount == 0:
                raise Exception("Incorrect tagtype or tagset")
            
            print("[%s] -> Tag valid and non-existent, creating tag..." % thread_id)
            sql = ("INSERT INTO public.tags (tagtype_id, tagset_id) VALUES (%d, %d) RETURNING id" 
                    % (tagtype_id, tagset_id))
            self.cursor.execute(sql)
            tag_id = self.cursor.fetchall()[0]['id']
            sql = "INSERT INTO public."
            match tagtype_id:
                case 1:
                    sql += ("alphanumerical_tags (id, name, tagset_id) VALUES (%d, '%s', %d) RETURNING *"
                            % (tag_id, request.alphanumerical.value, tagset_id))
                    self.cursor.execute(sql)
                    result = self.cursor.fetchall()[0]
                    # print("[%s] -> Success, tag inserted to DB." % thread_id)
                    return dataloader_pb2.TagResponse(
                            success=True,
                            tag = dataloader_pb2.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                alphanumerical= dataloader_pb2.AlphanumericalValue(value=result['name'])
                            ))
                case 2:
                    sql += ("timestamp_tags (id, name, tagset_id) VALUES (%d, '%s', %d) RETURNING *"
                            % (tag_id, request.timestamp.value, tagset_id))
                    self.cursor.execute(sql)
                    result = self.cursor.fetchall()[0]
                    # print("[%s] -> Success, tag inserted to DB." % thread_id)
                    return dataloader_pb2.TagResponse(
                            success=True,
                            tag = dataloader_pb2.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                timestamp= dataloader_pb2.TimeStampValue(value=str(result['name']))
                            ))
                case 3:
                    sql += ("time_tags (id, name, tagset_id) VALUES (%d, '%s', %d) RETURNING *"
                            % (tag_id, request.time.value, tagset_id))
                    self.cursor.execute(sql)
                    result = self.cursor.fetchall()[0]
                    # print("[%s] -> Success, tag inserted to DB." % thread_id)
                    return dataloader_pb2.TagResponse(
                            success=True,
                            tag = dataloader_pb2.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                time= dataloader_pb2.TimeValue(value=str(result['name']))
                            ))
                case 4:
                    sql += ("date_tags (id, name, tagset_id) VALUES (%d, '%s', %d) RETURNING *"
                            % (tag_id, request.date.value, tagset_id))
                    self.cursor.execute(sql)
                    result = self.cursor.fetchall()[0]
                    # print("[%s] -> Success, tag inserted to DB." % thread_id)
                    return dataloader_pb2.TagResponse(
                            success=True,
                            tag = dataloader_pb2.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                date= dataloader_pb2.DateValue(value=str(result['name']))
                            ))
                case 5:
                    sql += ("numerical_tags (id, name, tagset_id) VALUES (%d, '%s', %d) RETURNING *"
                            % (tag_id, request.numerical.value, tagset_id))
                    self.cursor.execute(sql)
                    result = self.cursor.fetchall()[0]
                    # print("[%s] -> Success, tag inserted to DB." % thread_id)
                    return dataloader_pb2.TagResponse(
                            success=True,
                            tag = dataloader_pb2.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                numerical= dataloader_pb2.NumericalValue(value=result['name'])
                            ))
                case _:
                    raise Exception("This should never happen")

        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            return dataloader_pb2.TagResponse(success=False)



    #!================ Taggings (ObjectTagRelations) ======================================================
    def getTaggings(self, request: dataloader_pb2.EmptyRequest, context):
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getTaggings request." % thread_id)
        count = 0
        try:
            sql = "SELECT * FROM public.objecttagrelations"
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            for row in res:
                count += 1
                yield dataloader_pb2.TaggingResponse(
                    success=True,
                    tagging=dataloader_pb2.Tagging(
                        mediaId=row['object_id'],
                        tagId=row['tag_id']
                        
                    )
                )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            yield dataloader_pb2.TaggingResponse(success=False)
        # print("[%s] -> Fetched %d items from database" % (thread_id, count))


    def getMediasWithTag(self, request: dataloader_pb2.IdRequest, context):
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getMediasWithTag request with tag_id=%d" % (thread_id, request.id))
        count = 0
        try:
            sql = ("SELECT object_id FROM public.objecttagrelations WHERE tag_id = %d"
                   % request.id)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            for row in res:
                count += 1
                yield dataloader_pb2.IdResponse(
                    success=True,
                    id=row['object_id']
                )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            yield dataloader_pb2.IdResponse(success=False)
        # print("[%s] -> Fetched %d items from database" % (thread_id, count))
    
    def getMediaTags(self, request: dataloader_pb2.IdRequest, context):
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getMediaTags request with media_id=%d" % (thread_id, request.id))
        count = 0
        try:
            sql = ("SELECT tag_id FROM public.objecttagrelations WHERE object_id = %d"
                   % request.id)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            for row in res:
                count += 1
                yield dataloader_pb2.IdResponse(
                    success=True,
                    id=row['tag_id']
                )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            yield dataloader_pb2.IdResponse(success=False)
        # print("[%s] -> Fetched %d items from database" % (thread_id, count))

    def createTagging(self, request: dataloader_pb2.CreateTaggingRequest, context) -> dataloader_pb2.TaggingResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received createTagging request with media_id=%d and tag_id=%d" % (thread_id, request.mediaId, request.tagId))
        sql = ("INSERT INTO public.objecttagrelations (object_id, tag_id) VALUES (%d, %d) RETURNING *;" 
               % (request.mediaId, request.tagId))
        try:
            self.cursor.execute(sql)
            inserted_tagging = self.cursor.fetchall()[0]
            response = dataloader_pb2.TaggingResponse(
                success=True,
                tagging = dataloader_pb2.Tagging(
                    mediaId=inserted_tagging['object_id'],
                    tagId=inserted_tagging['tag_id']
                )
            )
            self.conn.commit()
            # print("[%s] -> Operation completed, added 1 element to DB" % thread_id)
            return response
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            response = dataloader_pb2.TaggingResponse(success=False)
            return response

    #!================ Hierarchies  =======================================================================
    """ Hierarchies
	rpc getHierarchies(EmptyRequest) returns (stream HierarchyResponse) {};
    rpc getHierarchy(IdRequest) returns (HierarchyResponse) {};
	rpc createHierarchy(CreateHierarchyRequest) returns (HierarchyResponse) {};
    
    rpc createNode (CreateNodeRequest) returns (NodeResponse) {};
    rpc getNode (IdRequest) returns (NodeResponse) {};
    rpc getNodesOfHierarchy (IdRequest) returns (stream NodeResponse) {};
    rpc GetChildNodes(IdRequest) returns (stream NodeResponse) {};"""

    def getHierarchies(self, request: dataloader_pb2.EmptyRequest, context) : 
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getHierarchies request." % thread_id)
        count = 0
        try:
            sql = "SELECT * FROM public.hierarchies"
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            for row in res:
                count += 1
                yield dataloader_pb2.HierarchyResponse(
                    success=True,
                    hierarchy=dataloader_pb2.Hierarchy(
                        id=row['id'],
                        name=row['name'],
                        tagsetId=row['tagset_id'],
                        rootNodeId=row['rootnode_id']
                    )
                )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            yield dataloader_pb2.HierarchyResponse(success=False)


    def getHierarchy(self, request: dataloader_pb2.IdRequest, context) -> dataloader_pb2.HierarchyResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getHierarchy request with id=%d" % (thread_id, request.id))
        try:
            sql = """SELECT * FROM public.hierachies WHERE id=%d""" % request.id
            self.cursor.execute(sql)
            result = self.cursor.fetchall()[0]
            # print("[%s] -> Element fetched from DB, sending back to client..." % thread_id)
            return dataloader_pb2.HierarchyResponse(
                success=True,
                hierarchy=dataloader_pb2.Hierarchy(
                id=result['id'],
                name=result['name'],
                tagsetId=result['tagset_id'],
                rootNodeId=result['rootnode_id']
                )
            )
        except Exception as e:
            print("[%s] -> No results were fetched, sending error message to client..." % thread_id)
            return dataloader_pb2.HierarchyResponse(success=False)    


    def createHierarchy(self, request: dataloader_pb2.CreateHierarchyRequest, context) -> dataloader_pb2.HierarchyResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received createHierarchy request." % thread_id)
        request_counter = 0
        sql = """INSERT INTO public.hierarchies (name, tagset_id, rootnode_id)
VALUES ('%s', %d, '%s') RETURNING *;""" % (
                request.name,
                request.tagsetId,
                request.rootNodeId
            )
        try:
            self.cursor.execute(sql)
            response = self.cursor.fetchall()[0]
            return dataloader_pb2.HierarchyResponse(
                success=True,
                hierarchy=dataloader_pb2.Hierarchy(
                    id=response['id'],
                    name=response['name'],
                    tagsetId=response['tagset_id'],
                    rootNodeId=response['rootnode_id']
                )
            )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            return dataloader_pb2.HierarchyResponse(success=False)
        
    #!================ Nodes ==============================================================================
    def getNode(self, request: dataloader_pb2.IdRequest, context) -> dataloader_pb2.NodeResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getNode request with id=%d" % (thread_id, request.id))
        try:
            sql = """SELECT * FROM public.nodes WHERE id=%d""" % request.id
            self.cursor.execute(sql)
            result = self.cursor.fetchall()[0]
            # print("[%s] -> Element fetched from DB, sending back to client..." % thread_id)
            return dataloader_pb2.NodeResponse(
                success=True,
                node=dataloader_pb2.Node(
                    id=result['id'],
                    tagId=result['tag_id'],
                    hierarchyId=result['hierarchy_id'],
                    parentNodeId=result['parentnode_id']
                )
            )
        except Exception as e:
            print("[%s] -> No results were fetched, sending error message to client..." % thread_id)
            return dataloader_pb2.NodeResponse(success=False)   
    
    def getNodesOfHierarchy(self, request: dataloader_pb2.IdRequest, context) :
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getNodesOfHierarchy request with id=%d" % (thread_id, request.id))
        try:
            sql = """SELECT * FROM public.nodes WHERE hierarchy_id=%d""" % request.id
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            # print("[%s] -> Element fetched from DB, sending back to client..." % thread_id)
            for result in results:
                yield dataloader_pb2.NodeResponse(
                    success=True,
                    node=dataloader_pb2.Node(
                        id=result['id'],
                        tagId=result['tag_id'],
                        hierarchyId=result['hierarchy_id'],
                        parentNodeId=result['parentnode_id']
                    )
                )
        except Exception as e:
            print("[%s] -> No results were fetched, sending error message to client..." % thread_id)
            yield dataloader_pb2.NodeResponse(success=False)

    def getChildNodes(self, request: dataloader_pb2.IdRequest, context) :
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received getChildNodes request with id=%d" % (thread_id, request.id))
        try:
            sql = """SELECT * FROM public.nodes WHERE parentnode_id=%d""" % request.id
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            # print("[%s] -> Element fetched from DB, sending back to client..." % thread_id)
            for result in results:
                yield dataloader_pb2.NodeResponse(
                    success=True,
                    node=dataloader_pb2.Node(
                        id=result['id'],
                        tagId=result['tag_id'],
                        hierarchyId=result['hierarchy_id'],
                        parentNodeId=result['parentnode_id']
                    )
                )
        except Exception as e:
            print("[%s] -> No results were fetched, sending error message to client..." % thread_id)
            yield dataloader_pb2.NodeResponse(success=False)

    def createNode(self, request: dataloader_pb2.CreateNodeRequest, context) -> dataloader_pb2.NodeResponse :
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received creatNode request." % thread_id)
        request_counter = 0
        sql = """INSERT INTO public.nodes (tag_id, hierarchy_id, parentnode_id)
VALUES ('%s', %d, '%s') RETURNING *;""" % (
                request.tagId,
                request.hierarchyId,
                request.parentNodeId
            )
        try:
            self.cursor.execute(sql)
            response = self.cursor.fetchall()[0]
            return dataloader_pb2.NodeResponse(
                success=True,
                node=dataloader_pb2.Node(
                    id=response['id'],
                    tagId=response['tag_id'],
                    hierarchyId=response['hierarchy_id'],
                    parentNodeId=response['parentnode_id']
                )
            )
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            return dataloader_pb2.NodeResponse(success=False)


    #!================ DB Management ======================================================================
    def resetDatabase(self, request: dataloader_pb2.EmptyRequest, context) -> dataloader_pb2.StatusResponse:
        thread_id = shortuuid.ShortUUID().random(length=7)
        print("[%s] Received ResetDatabase request." % thread_id)
        try:
            self.cursor.execute(open("init.sql", "r").read())
            print("[%s] -> SUCCESS: DB has been reset" % thread_id)
            return dataloader_pb2.StatusResponse(success=True)
        except Exception as e:
            print("[%s] -> Error: %s" % (thread_id, str(e)))
            return dataloader_pb2.StatusResponse(success=False)


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor())
    add_DataLoaderServicer_to_server(DataLoader(), server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    server.start()
    print("Server listening at %s" % listen_addr)
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        MODE = sys.argv[1]
    logging.basicConfig(level=logging.INFO)
    serve()

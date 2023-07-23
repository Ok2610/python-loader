from concurrent import futures
import logging
import psycopg2
import psycopg2.extras
import sys
import random

from words import WORDS
import grpc
import dataloader_pb2 as rpc_objects
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
            self.cursor.execute(open("ddl.sql", "r").read())
            print("DB has been reset")

    def __del__(self):
        self.cursor.close()
        self.conn.close()


    #!================ Medias =============================================================================
    def getMediaById(self, request: rpc_objects.IdRequest, context) -> rpc_objects.MediaResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMediaById request with id=%d" % (thread_id, request.id))
        try:
            sql = """SELECT * FROM public.cubeobjects WHERE id=%d""" % request.id
            self.cursor.execute(sql)
            if self.cursor.rowcount == 0: raise Exception("No results were fetched.")
            result = self.cursor.fetchall()[0]
            # print("[%s] -> Element fetched from DB, sending back to client..." % thread_id)
            return rpc_objects.MediaResponse(
                media=rpc_objects.Media(
                        id= result["id"],
                        file_uri= result["file_uri"],
                        file_type= result["file_type"],
                        thumbnail_uri= result["thumbnail_uri"]
                    )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.MediaResponse(error_message=str(e))


    def getMediaByURI(self, request: rpc_objects.GetMediaByURIRequest, context) -> rpc_objects.MediaResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMediaIdFromURI request with URI=%s" % (thread_id, request.file_uri))
        try:
            sql = "SELECT * FROM public.cubeobjects WHERE file_uri='%s'" % request.file_uri
            self.cursor.execute(sql)
            if self.cursor.rowcount == 0: raise Exception("No results were fetched.")
            result = self.cursor.fetchall()[0]
            # print("[%s] -> Element fetched from DB, sending back ID to client..." % thread_id)
            return rpc_objects.MediaResponse(
                media=rpc_objects.Media(
                        id= result["id"],
                        file_uri= result["file_uri"],
                        file_type= result["file_type"],
                        thumbnail_uri= result["thumbnail_uri"]
                    )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.MediaResponse(error_message=str(e))


    def getMedias(self, request: rpc_objects.GetMediasRequest, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMedias request." % thread_id)
        count = 0
        try:
            sql = """SELECT * FROM public.cubeobjects"""
            if request.file_type > 0 :
                sql += " WHERE file_type = %d" % request.file_type
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            if self.cursor.rowcount == 0: raise Exception("No results were fetched.")
            for row in res:
                count += 1
                yield rpc_objects.MediaResponse(
                    media=rpc_objects.Media(
                        id= row["id"],
                        file_uri= row["file_uri"],
                        file_type= row["file_type"],
                        thumbnail_uri= row["thumbnail_uri"]
                    )
                )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            yield rpc_objects.MediaResponse(error_message=str(e))


    def createMedia(self, request: rpc_objects.CreateMediaRequest, context) -> rpc_objects.MediaResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createMedia request." % thread_id)
        try:
            sql = "SELECT * FROM public.cubeobjects WHERE file_uri = '%s'" % request.media.file_uri
            self.cursor.execute(sql)
            if self.cursor.rowcount > 0 :
                print("[%s] -> File URI '%s' already exists in database." % (thread_id, request.media.file_uri))
                existing_media = self.cursor.fetchall()[0]
                if existing_media['file_type'] == request.media.file_type and existing_media['thumbnail_uri'] == request.media.thumbnail_uri:
                    print("[%s] -> No conflicts, returning existing media." % thread_id)
                    return rpc_objects.MediaResponse(
                        media=rpc_objects.Media(
                            id= existing_media['id'],
                            file_uri= existing_media['file_uri'],
                            file_type= existing_media['file_type'],
                            thumbnail_uri= existing_media['thumbnail_uri']
                        ))
                else :
                    print("[%s] -> Other fields conflict, returning error message." % thread_id)
                    return rpc_objects.MediaResponse(
                        error_message="Error: Media URI '%s' already exists with a different type or thumbnail_uri." % request.media.file_uri
                        )
            sql = """INSERT INTO public.cubeobjects (file_uri, file_type, thumbnail_uri)
    VALUES ('%s', %d, '%s') RETURNING *;""" % (
                    request.media.file_uri,
                    request.media.file_type,
                    request.media.thumbnail_uri,
                )
            self.cursor.execute(sql)
            response = self.cursor.fetchall()[0]
            return rpc_objects.MediaResponse(
                media=rpc_objects.Media(
                    id=response['id'],
                    file_uri=response['file_uri'],
                    file_type=response['file_type'],
                    thumbnail_uri=response['thumbnail_uri']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.MediaResponse(error_message=str(e))

    def createMedias(self, request_iterator, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createMedias request." % thread_id)
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
                    response = rpc_objects.CreateMediaStreamResponse(count=request_counter)
                except Exception as e:
                    response = rpc_objects.CreateMediaStreamResponse(error_message=str(e))
                    print("[%s] -> Error: packet addition failed." % thread_id)
                yield response

        if request_counter % BATCH_SIZE > 0:
            sql = sql[:-1] + ";COMMIT;"
            try:
                self.cursor.execute(sql)
                response = rpc_objects.CreateMediaStreamResponse(count=request_counter)
            except Exception as e:
                response = rpc_objects.CreateMediaStreamResponse(error_message=str(e))
                print("[%s] -> %s" % (thread_id, str(e)))
            yield response
        # print("[%s] -> Operation completed, added %d elements to DB" % (thread_id, request_counter))


    def deleteMedia(self, request: rpc_objects.IdRequest, context) -> rpc_objects.StatusResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received deleteMedia request with id=%d" % (thread_id, request.id))
        try:
            sql = """DELETE FROM public.cubeobjects WHERE id=%d;""" % request.id
            self.cursor.execute(sql)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                # print("[%s] -> SUCCESS: Element deleted from DB." % thread_id)
                return rpc_objects.StatusResponse()
            else:
                raise Exception("Element not found")
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.StatusResponse(error_message=str(e))


    #!================ TagSets ============================================================================
    def getTagSets(self, request: rpc_objects.GetTagSetsRequest, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTagSets request." % thread_id)
        count = 0
        try:
            sql = """SELECT * FROM public.tagsets"""
            if request.tagTypeId > 0 :
                sql += " WHERE tagtype_id = %d" % request.tagTypeId

            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            if len(res) == 0 : raise Exception("No results were fetched.")
            for row in res:
                count += 1
                yield rpc_objects.TagSetResponse(
                    tagset=rpc_objects.TagSet(
                        id= row['id'],
                        name= row['name'],
                        tagTypeId= row['tagtype_id']
                    )
                )
        except Exception as e:
            yield rpc_objects.TagSetResponse(error_message=str(e))
            print("[%s] -> %s" % (thread_id, str(e)))

    def getTagSetById(self, request: rpc_objects.IdRequest, context) -> rpc_objects.TagSetResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTagsetById request with id=%d" % (thread_id, request.id))
        try:
            sql = """SELECT * FROM public.tagsets WHERE id=%d""" % request.id
            self.cursor.execute(sql)
            if self.cursor.rowcount == 0 : raise Exception("No results were fetched.")
            result = self.cursor.fetchall()[0]
            return rpc_objects.TagSetResponse(
                tagset=rpc_objects.TagSet(
                    id= result['id'],
                    name= result['name'],
                    tagTypeId= result['tagtype_id']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.TagSetResponse(error_message=str(e))
        

    def getTagSetByName(self, request: rpc_objects.GetTagSetRequestByName, context) -> rpc_objects.TagSetResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTagSetByName request with name=%s" % (thread_id, request.name))
        try:
            sql = """SELECT * FROM public.tagsets WHERE name='%s'""" % request.name
            self.cursor.execute(sql)
            if self.cursor.rowcount == 0 : raise Exception("No results were fetched.")          
            result = self.cursor.fetchall()[0]
            return rpc_objects.TagSetResponse(
                tagset=rpc_objects.TagSet(
                    id= result['id'],
                    name= result['name'],
                    tagTypeId= result['tagtype_id']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.TagSetResponse(error_message=str(e))

    
    # Behaviour: if a tagset with the same name and type exists, return the existent tagset.
    # If the name exists but with a different type, raise an error
    def createTagSet(self, request: rpc_objects.CreateTagSetRequest, context) -> rpc_objects.TagSetResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createTagSet request with name=%s and tag_type=%d" % (thread_id, request.name, request.tagTypeId))
        try:
            sql = "SELECT * FROM public.tagsets WHERE name = '%s'" % request.name
            self.cursor.execute(sql)
            if self.cursor.rowcount > 0 :
                print("[%s] -> Tagset name '%s' already exists in database." % (thread_id, request.name))
                existing_tagset = self.cursor.fetchall()[0]
                if existing_tagset['tagtype_id'] == request.tagTypeId:
                    print("[%s] -> No type conflict, returning existing tagset." % thread_id)
                    return rpc_objects.TagSetResponse(
                        tagset=rpc_objects.TagSet(
                            id= existing_tagset['id'],
                            name= existing_tagset['name'],
                            tagTypeId= existing_tagset['tagtype_id']
                        ))
                else :
                    print("[%s] -> Type conflict, returning error message." % thread_id)
                    return rpc_objects.TagSetResponse(
                        error_message="Error: Tagset name '%s' already exists with a different type." % request.name
                        )

            sql = "INSERT INTO public.tagsets (name, tagtype_id) VALUES ('%s', %d) RETURNING *;" % (request.name, request.tagTypeId)
            self.cursor.execute(sql)
            inserted_tagset = self.cursor.fetchall()[0]
            response = rpc_objects.TagSetResponse(
                tagset=rpc_objects.TagSet(
                    id= inserted_tagset['id'],
                    name= inserted_tagset['name'],
                    tagTypeId= inserted_tagset['tagtype_id']
                )
            )
            self.conn.commit()
            return response
        
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.TagSetResponse(error_message=str(e))
        

    #!================ Tags ===============================================================================
    def getTags(self, request: rpc_objects.GetTagsRequest, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
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
            if request.tagSetId > 0 or request.tagTypeId > 0:
                sql += " WHERE "
                if request.tagSetId > 0 and request.tagTypeId > 0:
                    sql += "t.tagset_id = %d AND t.tagtype_id = %d" % (request.tagSetId, request.tagTypeId)
                elif request.tagSetId > 0:
                    sql += "t.tagset_id = %d" % request.tagSetId
                else:
                    sql += "t.tagtype_id = %d" % request.tagTypeId
                    
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            if len(res) == 0 : raise Exception("No results were fetched.")
            for row in res:
                count += 1
                match row['tagtype_id']:
                    case 1:
                        tag = rpc_objects.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            alphanumerical = rpc_objects.AlphanumericalValue(value=row['text_value'])
                        )
                    case 2:
                        tag = rpc_objects.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            timestamp =  rpc_objects.TimeStampValue(value=str(row['timestamp_value']))
                        )
                    case 3:
                        tag = rpc_objects.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            time = rpc_objects.TimeValue(value=str(row['time_value']))
                        )
                    case 4:
                        tag = rpc_objects.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            date = rpc_objects.DateValue(value=str(row['date_value']))
                        )
                    case 5:
                        tag = rpc_objects.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            numerical = rpc_objects.NumericalValue(value=row['num_value'])
                        )
                    case _:
                        tag = {}
                yield rpc_objects.TagResponse(
                    tag=tag
                    )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            yield rpc_objects.TagResponse(error_message=str(e))
        # print("[%s] -> Fetched %d items from database" % (thread_id, count))


    def getTag(self, request: rpc_objects.IdRequest, context) -> rpc_objects.TagResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
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
            if self.cursor.rowcount == 0 : raise Exception("No results were fetched.")
            result = self.cursor.fetchall()[0]
            match result['tagtype_id']:
                case 1:
                    tag = rpc_objects.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        alphanumerical = rpc_objects.AlphanumericalValue(value=result['text_value'])
                    )
                case 2:
                    tag = rpc_objects.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        timestamp =  rpc_objects.TimeStampValue(value=str(result['timestamp_value']))
                    )
                case 3:
                    tag = rpc_objects.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        time = rpc_objects.TimeValue(value=str(result['time_value']))
                    )
                case 4:
                    tag = rpc_objects.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        date = rpc_objects.DateValue(value=str(result['date_value']))
                    )
                case 5:
                    tag = rpc_objects.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        numerical = rpc_objects.NumericalValue(value=result['num_value'])
                    )
                case _:
                    tag = {}
            # print("[%s] -> Fetched 1 tag from database" % thread_id)
            return rpc_objects.TagResponse(tag=tag)
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.TagResponse(error_message=str(e))


    def createTag(self, request: rpc_objects.CreateTagRequest, context) -> rpc_objects.TagResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createTag request with tagset_id=%d and tagtype_id=%d" % (thread_id, request.tagSetId, request.tagTypeId))
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
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                alphanumerical= rpc_objects.AlphanumericalValue(value=result['value'])
                            ))
                    case 2:
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                timestamp= rpc_objects.TimeStampValue(value=str(result['value']))
                            ))
                    case 3:
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                time = rpc_objects.TimeValue(value=str(result['value']))
                            ))
                    case 4:
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                date = rpc_objects.DateValue(value=str(result['value']))
                            ))
                    case 5:
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                numerical= rpc_objects.NumericalValue(value=result['value'])
                            ))
            
            sql = ("SELECT * FROM public.tagsets WHERE id = %d AND tagtype_id = %d" 
                    % (tagset_id, tagtype_id))
            
            self.cursor.execute(sql)
            if self.cursor.rowcount == 0:
                raise Exception("Error: incorrect type for the specified Tagset.")
            
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
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                alphanumerical= rpc_objects.AlphanumericalValue(value=result['name'])
                            ))
                case 2:
                    sql += ("timestamp_tags (id, name, tagset_id) VALUES (%d, '%s', %d) RETURNING *"
                            % (tag_id, request.timestamp.value, tagset_id))
                    self.cursor.execute(sql)
                    result = self.cursor.fetchall()[0]
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                timestamp= rpc_objects.TimeStampValue(value=str(result['name']))
                            ))
                case 3:
                    sql += ("time_tags (id, name, tagset_id) VALUES (%d, '%s', %d) RETURNING *"
                            % (tag_id, request.time.value, tagset_id))
                    self.cursor.execute(sql)
                    result = self.cursor.fetchall()[0]
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                time= rpc_objects.TimeValue(value=str(result['name']))
                            ))
                case 4:
                    sql += ("date_tags (id, name, tagset_id) VALUES (%d, '%s', %d) RETURNING *"
                            % (tag_id, request.date.value, tagset_id))
                    self.cursor.execute(sql)
                    result = self.cursor.fetchall()[0]
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                date= rpc_objects.DateValue(value=str(result['name']))
                            ))
                case 5:
                    sql += ("numerical_tags (id, name, tagset_id) VALUES (%d, '%s', %d) RETURNING *"
                            % (tag_id, request.numerical.value, tagset_id))
                    self.cursor.execute(sql)
                    result = self.cursor.fetchall()[0]
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                numerical= rpc_objects.NumericalValue(value=result['name'])
                            ))
                case _:
                    raise Exception("This should never happen")

        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.TagResponse(error_message=str(e))



    #!================ Taggings (ObjectTagRelations) ======================================================
    def getTaggings(self, request: rpc_objects.EmptyRequest, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTaggings request." % thread_id)
        try:
            sql = "SELECT * FROM public.objecttagrelations"
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            if self.cursor.rowcount == 0: raise Exception("No results were fetched.")
            for row in res:
                yield rpc_objects.TaggingResponse(
                    tagging=rpc_objects.Tagging(
                        mediaId=row['object_id'],
                        tagId=row['tag_id']
                    )
                )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            yield rpc_objects.TaggingResponse(error_message=str(e))


    def getMediasWithTag(self, request: rpc_objects.IdRequest, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMediasWithTag request with tag_id=%d" % (thread_id, request.id))
        try:
            sql = ("SELECT object_id FROM public.objecttagrelations WHERE tag_id = %d"
                   % request.id)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            if self.cursor.rowcount == 0: raise Exception("No results were fetched.")
            for row in res:
                yield rpc_objects.IdResponse(id=row['object_id'])

        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            yield rpc_objects.IdResponse(error_message=str(e))
    

    def getMediaTags(self, request: rpc_objects.IdRequest, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMediaTags request with media_id=%d" % (thread_id, request.id))
        try:
            sql = ("SELECT tag_id FROM public.objecttagrelations WHERE object_id = %d"
                   % request.id)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            if self.cursor.rowcount == 0: raise Exception("No results were fetched.")
            for row in res:
                yield rpc_objects.IdResponse(id=row['tag_id'])

        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            yield rpc_objects.IdResponse(error_message=str(e))


    def createTagging(self, request: rpc_objects.CreateTaggingRequest, context) -> rpc_objects.TaggingResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createTagging request with media_id=%d and tag_id=%d" % (thread_id, request.mediaId, request.tagId))
        sql = ("INSERT INTO public.objecttagrelations (object_id, tag_id) VALUES (%d, %d) RETURNING *;" 
               % (request.mediaId, request.tagId))
        try:
            self.cursor.execute(sql)
            inserted_tagging = self.cursor.fetchall()[0]
            response = rpc_objects.TaggingResponse(
                tagging = rpc_objects.Tagging(
                    mediaId=inserted_tagging['object_id'],
                    tagId=inserted_tagging['tag_id']
                )
            )
            self.conn.commit()
            return response
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.TaggingResponse(error_message=str(e))

    #!================ Hierarchies  =======================================================================

    def getHierarchies(self, request: rpc_objects.GetHierarchiesRequest, context) : 
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getHierarchies request." % thread_id)
        try:
            sql = "SELECT * FROM public.hierarchies"
            if request.tagsetId > 0:
                sql += " WHERE tagset_id = %d" % request.tagsetId
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            if len(res) == 0 : raise Exception("No results were fetched.") 
            for row in res:
                yield rpc_objects.HierarchyResponse(
                    hierarchy=rpc_objects.Hierarchy(
                        id=row['id'],
                        name=row['name'],
                        tagsetId=row['tagset_id'],
                        rootNodeId=row['rootnode_id']
                    )
                )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            yield rpc_objects.HierarchyResponse(error_message=str(e))


    def getHierarchy(self, request: rpc_objects.IdRequest, context) -> rpc_objects.HierarchyResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getHierarchy request with id=%d" % (thread_id, request.id))
        try:
            sql = """SELECT * FROM public.hierarchies WHERE id=%d""" % request.id
            self.cursor.execute(sql)
            if self.cursor.rowcount == 0 : raise Exception("No results were fetched.")
            result = self.cursor.fetchall()[0]
            return rpc_objects.HierarchyResponse(
                hierarchy=rpc_objects.Hierarchy(
                    id=result['id'],
                    name=result['name'],
                    tagsetId=result['tagset_id'],
                    rootNodeId=result['rootnode_id']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.HierarchyResponse(error_message=str(e))    


    def createHierarchy(self, request: rpc_objects.CreateHierarchyRequest, context) -> rpc_objects.HierarchyResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createHierarchy request." % thread_id)
        sql = """INSERT INTO public.hierarchies (name, tagset_id)
VALUES ('%s', %d) RETURNING *;""" % (
                request.name,
                request.tagsetId
            )
        try:
            self.cursor.execute(sql)
            response = self.cursor.fetchall()[0]
            return rpc_objects.HierarchyResponse(
                hierarchy=rpc_objects.Hierarchy(
                    id=response['id'],
                    name=response['name'],
                    tagsetId=response['tagset_id'],
                    rootNodeId=response['rootnode_id']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.HierarchyResponse(error_message=str(e))
        
        
    #!================ Nodes ==============================================================================
    def getNode(self, request: rpc_objects.IdRequest, context) -> rpc_objects.NodeResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getNode request with id=%d" % (thread_id, request.id))
        try:
            sql = """SELECT * FROM public.nodes WHERE id=%d""" % request.id
            self.cursor.execute(sql)
            if self.cursor.rowcount == 0 : raise Exception("No results were fetched.")
            result = self.cursor.fetchall()[0]
            return rpc_objects.NodeResponse(
                node=rpc_objects.Node(
                    id=result['id'],
                    tagId=result['tag_id'],
                    hierarchyId=result['hierarchy_id'],
                    parentNodeId=result['parentnode_id']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.NodeResponse(error_message=str(e))   
    
    def getNodes(self, request: rpc_objects.GetNodesRequest, context) :
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getNodes request" % (thread_id))
        try:
            sql = "SELECT * FROM public.nodes"
            #  TODO: implement this in a smart way 
            # Reflexion: a node is a tag reference in a hierarchy, 
            # so if more than one filter is applied we'll get only one result
            if request.hierarchyId > 0 or request.tagId > 0 or request.parentNodeId > 0:
                sql += " WHERE"
                if request.hierarchyId > 0:
                    sql += " hierarchy_id = %d AND" % request.hierarchyId
                if request.tagId > 0:
                    sql += " tag_id = %d AND" % request.tagId
                if request.parentNodeId > 0:
                    sql += " parentnode_id = %d AND" % request.parentNodeId
                sql = sql[:len(sql)-3]
            print(sql)         
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            if self.cursor.rowcount == 0 : raise Exception("No results were fetched.")
            for result in results:
                yield rpc_objects.NodeResponse(
                    node=rpc_objects.Node(
                        id=result['id'],
                        tagId=result['tag_id'],
                        hierarchyId=result['hierarchy_id'],
                        parentNodeId=result['parentnode_id']
                    )
                )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            yield rpc_objects.NodeResponse(error_message=str(e))


    def createNode(self, request: rpc_objects.CreateNodeRequest, context) -> rpc_objects.NodeResponse :
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received creatNode request." % thread_id)
        if request.parentNodeId:
            sql = """INSERT INTO public.nodes (tag_id, hierarchy_id, parentnode_id)
    VALUES ('%s', %d, '%s') RETURNING *;""" % (
                    request.tagId,
                    request.hierarchyId,
                    request.parentNodeId
            )       
            try:
                self.cursor.execute(sql)
                response = self.cursor.fetchall()[0]
                return rpc_objects.NodeResponse(
                    node=rpc_objects.Node(
                        id=response['id'],
                        tagId=response['tag_id'],
                        hierarchyId=response['hierarchy_id'],
                        parentNodeId=response['parentnode_id']
                    )
                )
            except Exception as e:
                print("[%s] -> %s" % (thread_id, str(e)))
                return rpc_objects.NodeResponse(error_message=str(e))
        
        else: # add a root node to hierarchy, only if it doesn't have any
            sql = "SELECT * FROM public.hierarchies WHERE id = %d" % request.hierarchyId
            try:
                self.cursor.execute(sql)
                if self.cursor.rowcount == 0:
                    raise Exception("Hierarchy doesn't exist.")
                elif self.cursor.fetchall()[0]['rootnode_id'] is not None:
                    raise Exception("Rootnode already exists.")
                else :
                    sql = """INSERT INTO public.nodes (tag_id, hierarchy_id) 
VALUES ('%s', %d) RETURNING *;""" % (
                        request.tagId,
                        request.hierarchyId
                        )  
                    self.cursor.execute(sql)
                    new_node = self.cursor.fetchall()[0]
                    sql = ("UPDATE hierarchies SET rootnode_id = %d WHERE id = %d" 
                           % (new_node['id'], request.hierarchyId))
                    self.cursor.execute(sql)
                    return rpc_objects.NodeResponse(
                    node=rpc_objects.Node(
                        id=new_node['id'],
                        tagId=new_node['tag_id'],
                        hierarchyId=new_node['hierarchy_id'],
                        parentNodeId=new_node['parentnode_id']
                    )
                )   

            except Exception as e:
                print("[%s] -> %s" % (thread_id, str(e)))
                return rpc_objects.NodeResponse(error_message=str(e))

    #!================ DB Management ======================================================================
    def resetDatabase(self, request: rpc_objects.EmptyRequest, context) -> rpc_objects.StatusResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received ResetDatabase request." % thread_id)
        try:
            self.cursor.execute(open("ddl.sql", "r").read())
            print("[%s] -> SUCCESS: DB has been reset" % thread_id)
            return rpc_objects.StatusResponse()
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            return rpc_objects.StatusResponse(error_message=str(e))


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

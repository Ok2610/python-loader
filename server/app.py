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
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("select version()")
        data = cursor.fetchone()
        print("Connection established to: ", data)
        if MODE == "reset":
            cursor.execute(open("ddl.sql", "r").read())
            print("DB has been reset")
        # ! Script to add tag types to tagsets
#         sql = """alter table public.tagsets add column tagtype_id integer;
# ALTER TABLE ONLY public.tagsets ADD CONSTRAINT "FK_tagsests_tag_types_tagtype_id" FOREIGN KEY (tagtype_id) REFERENCES public.tag_types(id) ON DELETE CASCADE;"""
#         cursor.execute(sql)
        # cursor.execute("select * from public.tagsets")
        # tagsets = cursor.fetchall()
        # for tagset in tagsets:
        #     tagset_id = int(tagset['id'])
        #     cursor.execute("select tagtype_id from public.tags where tagset_id = %d" % tagset_id)
        #     tagtype = cursor.fetchall()[0]
        #     cursor.execute("UPDATE public.tagsets SET tagtype_id = %d WHERE id = %d" % (tagtype['tagtype_id'], tagset_id))
        cursor.close()

    def __del__(self):
        self.conn.close()


    #!================ Medias =============================================================================
    def getMediaById(self, request: rpc_objects.IdRequest, context) -> rpc_objects.MediaResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMediaById request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "SELECT * FROM public.cubeobjects WHERE id=%d" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            cursor.close()
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
            cursor.close()
            return rpc_objects.MediaResponse(error_message=str(e))


    def getMediaByURI(self, request: rpc_objects.GetMediaByURIRequest, context) -> rpc_objects.MediaResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMediaIdFromURI request with URI=%s" % (thread_id, request.file_uri))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "SELECT * FROM public.cubeobjects WHERE file_uri=%s"
            data = (request.file_uri,)  # The comma is to make it a tuple with one element
            cursor.execute(sql, data)
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            cursor.close()
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
            cursor.close()
            return rpc_objects.MediaResponse(error_message=str(e))


    def getMedias(self, request: rpc_objects.GetMediasRequest, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMedias request" % thread_id)
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = """SELECT * FROM public.cubeobjects;"""
            if request.file_type > 0 :
                sql += " WHERE file_type = %d" % request.file_type
            cursor.execute(sql)
            res = cursor.fetchall()
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            for row in res:
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
        finally:
            cursor.close()


    def createMedia(self, request: rpc_objects.CreateMediaRequest, context) -> rpc_objects.MediaResponse:
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received createMedia request" % thread_id)
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "SELECT * FROM public.cubeobjects WHERE file_uri = %s" 
            data = (request.media.file_uri,)                                # The comma is to make it a tuple with one element
            cursor.execute(sql, data)
            if cursor.rowcount > 0 :
                # print("[%s] -> File URI '%s' already exists in database" % (thread_id, request.media.file_uri))
                existing_media = cursor.fetchall()[0]
                if existing_media['file_type'] == request.media.file_type and existing_media['thumbnail_uri'] == request.media.thumbnail_uri:
                    # print("[%s] -> No conflicts, returning existing media" % thread_id)
                    cursor.close()
                    return rpc_objects.MediaResponse(
                        media=rpc_objects.Media(
                            id= existing_media['id'],
                            file_uri= existing_media['file_uri'],
                            file_type= existing_media['file_type'],
                            thumbnail_uri= existing_media['thumbnail_uri']
                        ))
                else :
                    # print("[%s] -> Other fields conflict, returning error message" % thread_id)
                    cursor.close()
                    return rpc_objects.MediaResponse(
                        error_message="Error: Media URI '%s' already exists with a different type or thumbnail_uri" % request.media.file_uri
                        )
                
            sql = "INSERT INTO public.cubeobjects (file_uri, file_type, thumbnail_uri) VALUES (%s, %s, %s) RETURNING *;" 
            data = (request.media.file_uri, request.media.file_type, request.media.thumbnail_uri)
            cursor.execute(sql, data)
            response = cursor.fetchall()[0]
            self.conn.commit()
            cursor.close()
            return rpc_objects.MediaResponse(
                media=rpc_objects.Media(
                    id=response['id'],
                    file_uri=response['file_uri'],
                    file_type=response['file_type'],
                    thumbnail_uri=response['thumbnail_uri']
                )
            )
        except Exception as e:
            # print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.MediaResponse(error_message=str(e))

    def createMedias(self, request_iterator, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createMedias request" % thread_id)
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        request_counter = 0
        sql = ""
        data = ()
        for request in request_iterator:
            request_counter += 1
            if request_counter % BATCH_SIZE == 1:
                sql = "INSERT INTO public.cubeobjects (file_uri, file_type, thumbnail_uri) VALUES "
                data = ()
            sql += "(%s, %s, %s)," 
            data += (
                request.media.file_uri,
                request.media.file_type,
                request.media.thumbnail_uri,
            )
            if request_counter % BATCH_SIZE == 0:
                sql = sql[:-1] + ";"
                try:
                    cursor.execute(sql, data)
                    self.conn.commit()
                    response = rpc_objects.CreateMediaStreamResponse(count=request_counter)
                except Exception as e:
                    response = rpc_objects.CreateMediaStreamResponse(error_message=str(e))
                    print("[%s] -> Error: packet addition failed" % thread_id)
                yield response

        if request_counter % BATCH_SIZE > 0:
            sql = sql[:-1] + ";"
            try:
                cursor.execute(sql, data)
                self.conn.commit()
                response = rpc_objects.CreateMediaStreamResponse(count=request_counter)
            except Exception as e:
                response = rpc_objects.CreateMediaStreamResponse(error_message=str(e))
                print("[%s] -> %s" % (thread_id, str(e)))
            yield response
        # print("[%s] -> Operation completed, added %d elements to DB" % (thread_id, request_counter))
        cursor.close()


    def deleteMedia(self, request: rpc_objects.IdRequest, context) -> rpc_objects.StatusResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received deleteMedia request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        response = rpc_objects.StatusResponse()
        try:
            sql = "DELETE FROM public.cubeobjects WHERE id=%d;" % request.id
            cursor.execute(sql)
            if cursor.rowcount > 0:
                self.conn.commit()
            else:
                raise Exception("Element not found")
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            response = rpc_objects.StatusResponse(error_message=str(e))
        finally:
            cursor.close()
            return response


    #!================ TagSets ============================================================================
    def getTagSets(self, request: rpc_objects.GetTagSetsRequest, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTagSets request" % thread_id)
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        count = 0
        try:
            sql = """SELECT * FROM public.tagsets"""
            if request.tagTypeId > 0 :
                sql += " WHERE tagtype_id = %d" % request.tagTypeId

            cursor.execute(sql)
            res = cursor.fetchall()
            if len(res) == 0 : raise Exception("No results were fetched")
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
        finally:
            cursor.close()


    def getTagSetById(self, request: rpc_objects.IdRequest, context) -> rpc_objects.TagSetResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTagsetById request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "SELECT * FROM public.tagsets WHERE id=%d;" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0 : raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            cursor.close()
            return rpc_objects.TagSetResponse(
                tagset=rpc_objects.TagSet(
                    id= result['id'],
                    name= result['name'],
                    tagTypeId= result['tagtype_id']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.TagSetResponse(error_message=str(e))
        

    def getTagSetByName(self, request: rpc_objects.GetTagSetRequestByName, context) -> rpc_objects.TagSetResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTagSetByName request with name=%s" % (thread_id, request.name))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "SELECT * FROM public.tagsets WHERE name=%s"
            data = (request.name,)                              # The comma is to make it a tuple with one element
            cursor.execute(sql, data)
            if cursor.rowcount == 0 : raise Exception("No results were fetched")          
            result = cursor.fetchall()[0]
            cursor.close()
            return rpc_objects.TagSetResponse(
                tagset=rpc_objects.TagSet(
                    id= result['id'],
                    name= result['name'],
                    tagTypeId= result['tagtype_id']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.TagSetResponse(error_message=str(e))

    
    # Behaviour: if a tagset with the same name and type exists, return the existent tagset.
    # If the name exists but with a different type, raise an error
    def createTagSet(self, request: rpc_objects.CreateTagSetRequest, context) -> rpc_objects.TagSetResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createTagSet request with name=%s and tag_type=%d" % (thread_id, request.name, request.tagTypeId))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "SELECT * FROM public.tagsets WHERE name = %s;"
            data = (request.name,)
            cursor.execute(sql, data)
            if cursor.rowcount > 0 :
                print("[%s] -> Tagset name '%s' already exists in database" % (thread_id, request.name))
                existing_tagset = cursor.fetchall()[0]
                if existing_tagset['tagtype_id'] == request.tagTypeId:
                    print("[%s] -> No type conflict, returning existing tagset" % thread_id)
                    return rpc_objects.TagSetResponse(
                        tagset=rpc_objects.TagSet(
                            id= existing_tagset['id'],
                            name= existing_tagset['name'],
                            tagTypeId= existing_tagset['tagtype_id']
                        ))
                else :
                    print("[%s] -> Type conflict, returning error message" % thread_id)
                    return rpc_objects.TagSetResponse(
                        error_message="Error: Tagset name '%s' already exists with a different type" % request.name
                        )

            sql = "INSERT INTO public.tagsets (name, tagtype_id) VALUES (%s, %s) RETURNING *;"
            data = (request.name, request.tagTypeId)
            cursor.execute(sql, data)
            inserted_tagset = cursor.fetchall()[0] 
            self.conn.commit()
            cursor.close()
            return rpc_objects.TagSetResponse(
                tagset=rpc_objects.TagSet(
                    id= inserted_tagset['id'],
                    name= inserted_tagset['name'],
                    tagTypeId= inserted_tagset['tagtype_id']
                )
            )
        
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.TagSetResponse(error_message=str(e))
        

    #!================ Tags ===============================================================================
    def getTags(self, request: rpc_objects.GetTagsRequest, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTags request" % thread_id)
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
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
                    
            cursor.execute(sql)
            res = cursor.fetchall()
            if len(res) == 0 : raise Exception("No results were fetched")
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
        finally:
            cursor.close()


    def getTag(self, request: rpc_objects.IdRequest, context) -> rpc_objects.TagResponse:
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received getTag request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
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
    public.numerical_tags nt ON t.id = nt.id""" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0 : raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
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
            cursor.close()
            return rpc_objects.TagResponse(tag=tag)
        except Exception as e:
            # print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.TagResponse(error_message=str(e))


    def createTag(self, request: rpc_objects.CreateTagRequest, context) -> rpc_objects.TagResponse:
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received createTag request with tagset_id=%d and tagtype_id=%d" % (thread_id, request.tagSetId, request.tagTypeId))
        tagset_id = request.tagSetId
        tagtype_id = request.tagTypeId
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = """SELECT t.id, t.tagtype_id, t.tagset_id, a.name as value FROM 
(SELECT * FROM public.tags WHERE tagset_id = %d AND tagtype_id = %d) t
LEFT JOIN """ % (tagset_id, tagtype_id)
            data = ()
            match request.tagTypeId:
                case 1: 
                    sql += "public.alphanumerical_tags a ON t.id = a.id WHERE a.name = %s" 
                    data = (request.alphanumerical.value,)
                case 2:                                                              
                    sql += "public.timestamp_tags a ON t.id = a.id WHERE a.name = %s"
                    data = (request.timestamp.value,)
                case 3:                                                              
                    sql += "public.time_tags a ON t.id = a.id WHERE a.name = %s" 
                    data = (request.time.value,)
                case 4:                                                              
                    sql += "public.date_tags a ON t.id = a.id WHERE a.name = %s" 
                    data = (request.date.value,)
                case 5: 
                    sql += "public.numerical_tags a ON t.id = a.id WHERE a.name = %s" 
                    data = (request.numerical.value,)
                case _:
                    raise Exception("invalid tag type: range is 1-5")
            cursor.execute(sql, data)

            # Check if cursor has result, i.e. the tag already exists
            if cursor.rowcount > 0:
                # print("[%s] -> Tag already present in DB, returning tag info" % thread_id)
                result = cursor.fetchall()[0]
                match request.tagTypeId:
                    case 1:
                        cursor.close()
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                alphanumerical= rpc_objects.AlphanumericalValue(value=result['value'])
                            ))
                    case 2:
                        cursor.close()
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                timestamp= rpc_objects.TimeStampValue(value=str(result['value']))
                            ))
                    case 3:
                        cursor.close()
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                time = rpc_objects.TimeValue(value=str(result['value']))
                            ))
                    case 4:
                        cursor.close()
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                date = rpc_objects.DateValue(value=str(result['value']))
                            ))
                    case 5:
                        cursor.close()
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                numerical= rpc_objects.NumericalValue(value=result['value'])
                            ))
            
            sql = "SELECT * FROM public.tagsets WHERE tagtype_id = %d AND id = %d;" % (tagtype_id, tagset_id)
            cursor.execute(sql)
            if cursor.rowcount == 0:
                raise Exception("Error: incorrect type for the specified Tagset")
            
            # print("[%s] -> Tag valid and non-existent, creating tag.." % thread_id)
            sql = "INSERT INTO public.tags (tagtype_id, tagset_id) VALUES (%d, %d) RETURNING id" % (tagtype_id, tagset_id)
            cursor.execute(sql)
            tag_id = cursor.fetchall()[0]['id']
            sql = "INSERT INTO public."
            match tagtype_id:
                case 1:
                    sql += "alphanumerical_tags (id, name, tagset_id) VALUES (%s, %s, %s) RETURNING *"
                    data = (tag_id, request.alphanumerical.value, tagset_id)
                    cursor.execute(sql, data)
                    result = cursor.fetchall()[0]
                    cursor.close()
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                alphanumerical= rpc_objects.AlphanumericalValue(value=result['name'])
                            ))
                case 2:
                    sql += "timestamp_tags (id, name, tagset_id) VALUES (%s, %s, %s) RETURNING *"
                    data = (tag_id, request.timestamp.value, tagset_id)
                    cursor.execute(sql, data)
                    result = cursor.fetchall()[0]
                    cursor.close()
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                timestamp= rpc_objects.TimeStampValue(value=str(result['name']))
                            ))
                case 3:
                    sql += "time_tags (id, name, tagset_id) VALUES (%s, %s, %s) RETURNING *"
                    data = (tag_id, request.time.value, tagset_id)
                    cursor.execute(sql, data)
                    result = cursor.fetchall()[0]
                    cursor.close()
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                time= rpc_objects.TimeValue(value=str(result['name']))
                            ))
                case 4:
                    sql += "date_tags (id, name, tagset_id) VALUES (%s, %s, %s) RETURNING *"
                    data = (tag_id, request.date.value, tagset_id)
                    cursor.execute(sql, data)
                    result = cursor.fetchall()[0]
                    cursor.close()
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                date= rpc_objects.DateValue(value=str(result['name']))
                            ))
                case 5:
                    sql += "numerical_tags (id, name, tagset_id) VALUES (%s, %s, %s) RETURNING *"
                    data = (tag_id, request.numerical.value, tagset_id)
                    cursor.execute(sql, data)
                    result = cursor.fetchall()[0]
                    cursor.close()
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
            # print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.TagResponse(error_message=str(e))



    #!================ Taggings (ObjectTagRelations) ======================================================
    def getTaggings(self, request: rpc_objects.EmptyRequest, context):
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTaggings request" % thread_id)
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "SELECT * FROM public.objecttagrelations"
            cursor.execute(sql)
            res = cursor.fetchall()
            if cursor.rowcount == 0: raise Exception("No results were fetched")
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
        finally:
            cursor.close()


    def getMediasWithTag(self, request: rpc_objects.IdRequest, context) -> rpc_objects.RepeatedIdResponse :
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMediasWithTag request with tag_id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = ("SELECT object_id FROM public.objecttagrelations WHERE tag_id = %d"
                   % request.id)
            cursor.execute(sql)
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            else: result = [item for item, in cursor]
            cursor.close()
            return rpc_objects.RepeatedIdResponse(ids=result)

        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.RepeatedIdResponse(error_message=str(e))
    

    def getMediaTags(self, request: rpc_objects.IdRequest, context) -> rpc_objects.RepeatedIdResponse :
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received getMediaTags request with media_id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = ("SELECT tag_id FROM public.objecttagrelations WHERE object_id = %d"
                   % request.id)
            cursor.execute(sql)
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            result = [item for item, in cursor]
            cursor.close()
            return rpc_objects.RepeatedIdResponse(ids=result)

        except Exception as e:
            # print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.RepeatedIdResponse(error_message=str(e))


    def createTagging(self, request: rpc_objects.CreateTaggingRequest, context) -> rpc_objects.TaggingResponse:
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received createTagging request with media_id=%d and tag_id=%d" % (thread_id, request.mediaId, request.tagId))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = ("SELECT * FROM public.objecttagrelations WHERE object_id = %d AND tag_id = %d"
                   % (request.mediaId, request.tagId))
            cursor.execute(sql)
            if cursor.rowcount == 0: 
                sql = ("INSERT INTO public.objecttagrelations (object_id, tag_id) VALUES (%d, %d) RETURNING *;" 
                % (request.mediaId, request.tagId))
                cursor.execute(sql)
            else:
                pass
                # print("[%s] -> Tagging already present in database, returning value to client" % thread_id)

            tagging = cursor.fetchall()[0]
            cursor.close()
            return rpc_objects.TaggingResponse(
                tagging = rpc_objects.Tagging(
                    mediaId=tagging['object_id'],
                    tagId=tagging['tag_id']
                )
            )
            
        except Exception as e:
            # print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.TaggingResponse(error_message=str(e))

    #!================ Hierarchies  =======================================================================

    def getHierarchies(self, request: rpc_objects.GetHierarchiesRequest, context) : 
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getHierarchies request" % thread_id)
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "SELECT * FROM public.hierarchies"
            if request.tagSetId > 0:
                sql += " WHERE tagset_id = %d" % request.tagSetId
            cursor.execute(sql)
            res = cursor.fetchall()
            if len(res) == 0 : raise Exception("No results were fetched") 
            for row in res:
                yield rpc_objects.HierarchyResponse(
                    hierarchy=rpc_objects.Hierarchy(
                        id=row['id'],
                        name=row['name'],
                        tagSetId=row['tagset_id'],
                        rootNodeId=row['rootnode_id']
                    )
                )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            yield rpc_objects.HierarchyResponse(error_message=str(e))
        finally:
            cursor.close()


    def getHierarchy(self, request: rpc_objects.IdRequest, context) -> rpc_objects.HierarchyResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getHierarchy request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = """SELECT * FROM public.hierarchies WHERE id=%d""" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0 : raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            cursor.close()
            return rpc_objects.HierarchyResponse(
                hierarchy=rpc_objects.Hierarchy(
                    id=result['id'],
                    name=result['name'],
                    tagSetId=result['tagset_id'],
                    rootNodeId=result['rootnode_id']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.HierarchyResponse(error_message=str(e))    


    def createHierarchy(self, request: rpc_objects.CreateHierarchyRequest, context) -> rpc_objects.HierarchyResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createHierarchy request with name = %s" % (thread_id, request.name))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "SELECT * FROM public.hierarchies WHERE name = %s AND tagset_id = %s"
            data = (request.name, request.tagSetId)
            cursor.execute(sql, data)
            if cursor.rowcount == 0:
                sql = """INSERT INTO public.hierarchies (name, tagset_id) VALUES (%s, %s) RETURNING *;"""
                cursor.execute(sql, data)
            else:
                print("[%s] -> Hierarchy already present in database, returning value to client" % thread_id)

            response = cursor.fetchall()[0]
            cursor.close()
            return rpc_objects.HierarchyResponse(
                hierarchy=rpc_objects.Hierarchy(
                    id=response['id'],
                    name=response['name'],
                    tagSetId=response['tagset_id'],
                    rootNodeId=response['rootnode_id']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.HierarchyResponse(error_message=str(e))
        
        
    #!================ Nodes ==============================================================================
    def getNode(self, request: rpc_objects.IdRequest, context) -> rpc_objects.NodeResponse:
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received getNode request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = """SELECT * FROM public.nodes WHERE id=%d""" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0 : raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            cursor.close()
            return rpc_objects.NodeResponse(
                node=rpc_objects.Node(
                    id=result['id'],
                    tagId=result['tag_id'],
                    hierarchyId=result['hierarchy_id'],
                    parentNodeId=result['parentnode_id']
                )
            )
        except Exception as e:
            # print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.NodeResponse(error_message=str(e))   
    
    def getNodes(self, request: rpc_objects.GetNodesRequest, context) :
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received getNodes request" % (thread_id))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "SELECT * FROM public.nodes"
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
            cursor.execute(sql)
            results = cursor.fetchall()
            if cursor.rowcount == 0 : raise Exception("No results were fetched")
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
            # print("[%s] -> %s" % (thread_id, str(e)))
            yield rpc_objects.NodeResponse(error_message=str(e))
        finally:
            cursor.close()

    def createNode(self, request: rpc_objects.CreateNodeRequest, context) -> rpc_objects.NodeResponse :
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received creatNode request" % thread_id)
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if request.parentNodeId:
            try:
                sql = ("SELECT * FROM public.nodes WHERE tag_id = %d AND hierarchy_id = %d AND parentnode_id = %d"
                    % (
                            request.tagId,
                            request.hierarchyId,
                            request.parentNodeId
                    ))
                cursor.execute(sql)
                if cursor.rowcount == 0:
                    sql = """INSERT INTO public.nodes (tag_id, hierarchy_id, parentnode_id) VALUES (%d, %d, %d) RETURNING *;""" % (
                            request.tagId,
                            request.hierarchyId,
                            request.parentNodeId
                    )      
                    cursor.execute(sql)
                else: 
                    # print("[%s] -> Node already present in database, returning value to client" % thread_id)
                    pass
                response = cursor.fetchall()[0]
                cursor.close()
                return rpc_objects.NodeResponse(
                    node=rpc_objects.Node(
                        id=response['id'],
                        tagId=response['tag_id'],
                        hierarchyId=response['hierarchy_id'],
                        parentNodeId=response['parentnode_id']
                    )
                )
            except Exception as e:
                # print("[%s] -> %s" % (thread_id, str(e)))
                cursor.close()
                return rpc_objects.NodeResponse(error_message=str(e))
        
        else: # add a root node to hierarchy, only if it doesn't have any
            try:
                sql = ("SELECT * FROM public.nodes WHERE tag_id = %d AND hierarchy_id = %d AND parentnode_id IS NULL"
                    % (
                        request.tagId,
                        request.hierarchyId
                    ))
                cursor.execute(sql)
                if cursor.rowcount == 0:
                    sql = ("INSERT INTO public.nodes (tag_id, hierarchy_id) VALUES (%d, %d) RETURNING *;"
                           % (
                                request.tagId,
                                request.hierarchyId
                            ))
                    cursor.execute(sql)
                    node = cursor.fetchall()[0]
                    sql = ("UPDATE public.hierarchies SET rootnode_id = %d WHERE id = %d" 
                           % (node['id'], request.hierarchyId))
                    cursor.execute(sql)
                    self.conn.commit()
                else:
                    # print("[%s] -> Node already present in database, returning value to client" % thread_id)
                    node = cursor.fetchall()[0]
                cursor.close()
                return rpc_objects.NodeResponse(
                        node=rpc_objects.Node(
                            id=node['id'],
                            tagId=node['tag_id'],
                            hierarchyId=node['hierarchy_id']
                        )
                    )

            except Exception as e:
                # print("[%s] -> %s" % (thread_id, str(e)))
                cursor.close()
                return rpc_objects.NodeResponse(error_message=str(e))
            

    def deleteNode(self, request: rpc_objects.IdRequest, context) -> rpc_objects.StatusResponse:
        # The current behaviour enforced by the DB rules is that you cannot delete a node with childs,
        # or the rootnode of a hierarchy
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received deleteNode request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            sql = "DELETE FROM public.nodes WHERE id=%d;" % request.id
            cursor.execute(sql)
            if cursor.rowcount > 0:
                self.conn.commit()
                cursor.close()
                return rpc_objects.StatusResponse()
            else:
                raise Exception("Element not found")
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.StatusResponse(error_message=str(e))
        

    #!================ DB Management ======================================================================
    def resetDatabase(self, request: rpc_objects.EmptyRequest, context) -> rpc_objects.StatusResponse:
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received ResetDatabase request" % thread_id)
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cursor.execute(open("../ddl.sql", "r").read())
            print("[%s] -> SUCCESS: DB has been reset" % thread_id)
            cursor.close()
            return rpc_objects.StatusResponse()
        except Exception as e:
            print("[%s] -> %s" % (thread_id, str(e)))
            cursor.close()
            return rpc_objects.StatusResponse(error_message=str(e))


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
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

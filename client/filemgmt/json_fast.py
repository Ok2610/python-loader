from filemgmt.filehandler import FileHandler
import grpc_client
import json
import threading
import logging
import sys
from grpc import RpcError
from grpc import StatusCode
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

class FastJSONHandler(FileHandler):
# Works with optimized JSON files, allowing for faster execution times on importing and exporting
# at the cost of human readability
# NOTE: I used the library tqdm for visualising progress and measuring execution times, but it mixed the execution times of
# the different sections. All the 'tqdm.write' statements are basically prints made to work with tqdm progress bars

    def importFile(self, path):
    # Import a JSON file's contents using the 'fast' format. It is structured in 3 parts: 
    # - Tagsets and their respective tags, with value and ID
    # - Medias and their respective list of tag IDs
    # - Hierarchies with nodes containing tag IDs in a tree structure

        try:
            with open(path, 'r') as file:
                data = json.load(file)
                tagset_id_map = {}
                tag_id_map = {}
                tagset_id_map_lock = threading.Lock()
                printlock = threading.Lock()
                with ThreadPoolExecutor(max_workers=10) as threadpool:

                    # Processing of tagsets and tags
                    start = time.time()

                    # We order the tagsets according to their sizes, to begin processing the larger ones first
                    tagsets = sorted(data.get('tagsets', []), key= lambda tagset_item: -len(tagset_item.get('tags', [])))
                    
                    tagsets_pbar = tqdm(total=len(tagsets), desc="1- Adding tagsets", position=0)

                    def processTagset(tagset_item):
                    # Processing of a single tagset by a given thread. We first get the tagset's attributes, then the list of its tags
                        thread_client = grpc_client.LoaderClient()
                        tagset_id = tagset_item.get('id')
                        tagset_name = tagset_item.get('name')
                        tagset_type = tagset_item.get('type')
                        try:
                            tagset_response = thread_client.add_tagset(tagset_name, tagset_type)
                        except RpcError as e:
                            if e.code() == StatusCode.UNAVAILABLE:
                                logging.error(f"Service unavailable while adding tagset {tagset_name}: {e}")
                                print("Fatal error, check log file.", file=sys.stderr);exit(1)
                            logging.warning(f"Failed to add tagset {tagset_name} with type {tagset_type}: {e}")
                            return
                        
                        tagset_id_map_lock.acquire()
                        tagset_id_map[tagset_id] = (tagset_response.id, tagset_response.tagTypeId)
                        tagset_id_map_lock.release()
                        
                        tags = tagset_item.get('tags')
                        tagid_map_frag = {}
                        try:
                            tags_response_iterator = thread_client.add_tags(tagset_response.id, tagset_type, tags)
                        except RpcError as e:
                            if e.code() == StatusCode.UNAVAILABLE:
                                logging.error(f"Service unavailable while adding tags for tagset {tagset_name}: {e}")
                                print("Fatal error, check log file.", file=sys.stderr);exit(1)
                            logging.warning(f"Failed to add tags for tagset {tagset_name}: {e}")
                            return
                        for tags_response in tags_response_iterator:
                            if tags_response.HasField("error"):
                                logging.warning(f"Error adding tags for tagset {tagset_name}: {tags_response.error}")
                                return
                            tagid_map_frag.update(tags_response)

                        return tagid_map_frag
                                    
                    futures = [threadpool.submit(processTagset, tagset_item) for tagset_item in tagsets]
                    
                    for future in as_completed(futures):
                        frag = future.result()
                        if type(frag) is str:
                            tqdm.write(frag)
                        else :
                            tag_id_map.update(frag) # type: ignore
                        tagsets_pbar.update(1)
                    elapsed = time.time() - start
                    tqdm.write(f"Time for processing {len(tagsets)} tagsets: {elapsed}s.")


                    # Processing of medias
                    start = time.time()
                    medias = sorted(data.get('medias', []), key= lambda media_item: -len(media_item.get('tags')))
                    medias_pbar = tqdm(total=len(medias), desc="2- Adding medias", position=1)

                    def processMedia(media_item):
                    # Processing of a single media by a single thread
                        thread_client = grpc_client.LoaderClient()
                        media_path = media_item.get('path')
                        thumbnail_path = media_item.get('thumbnail')
                        try:
                            media_response = thread_client.add_file(media_path, thumbnail_path)
                        except RpcError as e:
                            if e.code() == StatusCode.UNAVAILABLE:
                                logging.error(f"Service unavailable while adding media {media_path}: {e}")
                                print("Fatal error, check log file.", file=sys.stderr);exit(1)
                            logging.warning(f"Failed to add media {media_path}: {e}")
                            return
                        tags = []
                        for item in media_item.get('tags', []):
                            if item in tag_id_map.keys():
                                tags.append(tag_id_map[item])

                        try:
                            response_iterator = thread_client.add_taggings(media_id=media_response.id, tag_ids=tags)
                        except RpcError as e:
                            if e.code() == StatusCode.UNAVAILABLE:
                                logging.error(f"Service unavailable while adding taggings for media {media_path}: {e}")
                                print("Fatal error, check log file.", file=sys.stderr);exit(1)
                            logging.warning(f"Failed to add taggings for media {media_path}: {e}")
                            return
                        # We need to iterate here because otherwise the thread will terminate without waiting for the responses
                        for response in response_iterator:
                            if response.HasField("error"):
                                logging.warning(f"Error adding taggings for media {media_path}: {response.error}")
                            continue
                        return None
                    
                    futures = [threadpool.submit(processMedia, media_item) for media_item in medias]
                    for future in as_completed(futures):
                        result = future.result() 
                        if result is not None:
                            tqdm.write(result)
                        medias_pbar.update(1)
                    elapsed = time.time() - start
                    tqdm.write(f"Time for processing {len(medias)} medias: {elapsed}s.")

                    # Processing hierarchies
                    start = time.time()
                    hierarchies = sorted(data.get('hierarchies', []), 
                                         key= lambda item : -len(json.dumps(item))) 
                    hierarchies_pbar = tqdm(total=len(hierarchies), desc="3- Adding hierarchies", position=2)
                
                    def processHierarchy(hierarchy_item):
                    # Processing of a single hierarchy by a single thread
                        try:
                            thread_client = grpc_client.LoaderClient()
                            name = hierarchy_item.get('name')
                            tagset_id = tagset_id_map[hierarchy_item.get('tagset_id')][0]
                            try:
                                hierarchy_response = thread_client.add_hierarchy(name, tagset_id)
                            except RpcError as e:
                                if e.code() == StatusCode.UNAVAILABLE:
                                    logging.error(f"Service unavailable while adding hierarchy {name} with tagset {tagset_id}: {e}")
                                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                                logging.warning(f"Failed to add hierarchy {name} with tagset {tagset_id}: {e}")
                                return
                            hierarchy_id = hierarchy_response.id

                            def addNode(node, parentnode_id):
                                tag_id = tag_id_map[node.get('tag_id')]
                                if tag_id:
                                    try:
                                        new_node = thread_client.add_node(tag_id, hierarchy_id, parentnode_id)    # type: ignore
                                    except RpcError as e:
                                        if e.code() == StatusCode.UNAVAILABLE:
                                            logging.error(f"Service unavailable while adding node with tag {tag_id} to hierarchy {hierarchy_id}: {e}")
                                            print("Fatal error, check log file.", file=sys.stderr);exit(1)
                                        logging.warning(f"Failed to add node with tag {tag_id} to hierarchy {hierarchy_id}: {e}")
                                        return
                                    child_nodes = node.get('child_nodes')
                                    for child_node_item in child_nodes:
                                        addNode(child_node_item, new_node.id)

                            
                            rootnode_item = hierarchy_item.get('rootnode')
                            rootnode_tag_id = tag_id_map[rootnode_item.get('tag_id')]
                            if rootnode_tag_id:
                                try:
                                    rootnode_id = thread_client.add_rootnode(rootnode_tag_id, hierarchy_response.id).id   # type: ignore
                                except RpcError as e:
                                    if e.code() == StatusCode.UNAVAILABLE:
                                        logging.error(f"Service unavailable while adding root node with tag {rootnode_tag_id} to hierarchy {hierarchy_id}: {e}")
                                        print("Fatal error, check log file.", file=sys.stderr);exit(1)
                                    logging.warning(f"Failed to add root node with tag {rootnode_tag_id} to hierarchy {hierarchy_id}: {e}")
                                    return
                                child_nodes = rootnode_item.get('child_nodes')
                                for child_node_item in child_nodes:                    
                                    addNode(child_node_item, rootnode_id)
                                    
                        except Exception as e:
                            printlock.acquire()
                            tqdm.write(f'Error adding hierarchy: {repr(e)}')
                            printlock.release()
                    
                    futures = [threadpool.submit(processHierarchy, hierarchy_item) for hierarchy_item in hierarchies]
                    for _ in as_completed(futures):
                        hierarchies_pbar.update(1) 
                        continue 
                    elapsed = time.time() - start
                    tqdm.write(f"Time for processing {len(hierarchies)} hierarchies: {elapsed}s.")


        except FileNotFoundError:
            print(f"File not found: {path}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
    

    def exportFile(self, path):
    # Export the current DB state to a JSON file using the 'fast' format
        start_time = time.time()
        tagsets = []
        tagsets_lock = threading.Lock()
        medias = []
        medias_lock = threading.Lock()
        hierarchies = []
        hierarchies_lock = threading.Lock()

        try:
            response_tagsets = self.client.get_tagsets(-1)
        except RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                logging.error(f"Service unavailable while retrieving tagsets: {e}")
                print("Fatal error, check log file.", file=sys.stderr);exit(1)
            logging.error(f"Failed to retrieve tagsets: {e}")
            return
        tagsets_pbar = tqdm(total=0, desc="Exporting tagsets")
        def processTagset(tagset_response):
            thread_client = grpc_client.LoaderClient()
            tags = []
            try:
                response_tags = thread_client.get_tags(-1, tagset_response.id)
            except RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    logging.error(f"Service unavailable while retrieving tags for tagset {tagset_response.id}: {e}")
                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                logging.error(f"Failed to retrieve tags for tagset {tagset_response.id}: {e}")
                return
            for tag_response in response_tags:
                if tag_response.HasField("error"):
                    logging.warning(f"Error retrieving tag {tag_response.id} for tagset {tagset_response.id}: {tag_response.error}")
                    continue
                tag_id = tag_response.id                               
                possible_values = [tag_response.alphanumerical.value,
                                tag_response.timestamp.value,
                                tag_response.time.value,
                                tag_response.date.value,
                                tag_response.numerical.value]
                value = next(value for value in possible_values if value != "")
                tags.append({"id":tag_id, "value":value})
            tagsets_lock.acquire()
            tagsets.append(
                {
                    "id"  : tagset_response.id,
                    "name": tagset_response.name,
                    "type": tagset_response.tagTypeId,
                    "tags": tags
                })
            tagsets_lock.release()
            tagsets_pbar.update(1)

        
        tagset_executor = ThreadPoolExecutor(max_workers=10)
        for tagset_response in response_tagsets:
            if tagset_response.HasField("error"):
                tagsets_pbar.total += 1
                tagsets_pbar.refresh()
                tagset_executor.submit(processTagset, tagset_response)

        tagset_executor.shutdown()
        
        medias_pbar = tqdm(total=0, desc="Exporting medias")
        def processMedia(media_response):
            thread_client = grpc_client.LoaderClient()
            tag_ids = []
            try:
                tag_ids_response = thread_client.get_media_tags(media_response.id)
            except RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    logging.error(f"Service unavailable while retrieving tags for media {media_response.id}: {e}")
                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                logging.error(f"Failed to retrieve tags for media {media_response.id}: {e}")
  
            tag_ids = list(tag_ids_response)
            medias_lock.acquire()
            medias.append(
                {
                    "path":media_response.file_uri,
                    "tags":tag_ids
                }
            )
            medias_lock.release()
            medias_pbar.update(1)

        try:
            response_medias = self.client.get_medias(-1)
        except RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                logging.error(f"Service unavailable while retrieving medias: {e}")
                print("Fatal error, check log file.", file=sys.stderr);exit(1)
            logging.error(f"Failed to retrieve medias: {e}")
            return

        medias_executor = ThreadPoolExecutor(max_workers=10)
        for media_response in response_medias:
            if media_response.HasField("error"):
                if media_response.error.code == StatusCode.NOT_FOUND:
                    continue
                else:
                    logging.warning(f"Error retrieving media {media_response.id}: {media_response.error}")
                    return
            medias_pbar.total += 1
            medias_pbar.refresh()
            medias_executor.submit(processMedia, media_response)
        
        medias_executor.shutdown()
                
        try:
            response_hierarchies = self.client.get_hierarchies(-1)
        except RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                logging.error(f"Service unavailable while retrieving hierarchies: {e}")
                print("Fatal error, check log file.", file=sys.stderr);exit(1)
            logging.error(f"Failed to retrieve hierarchies: {e}")
            return
        hierarchies_pbar = tqdm(total=0, desc="Exporting hierarchies")
        def processHierarchy(hierarchy_response):
            thread_client = grpc_client.LoaderClient()
            hierarchy_name = hierarchy_response.name
            hierarchy_tagset_id = hierarchy_response.tagSetId
            try:
                rootnode = thread_client.get_node(hierarchy_response.rootNodeId)
            except RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    logging.error(f"Service unavailable while retrieving root node for hierarchy {hierarchy_name}: {e}")
                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                logging.error(f"Failed to retrieve root node for hierarchy {hierarchy_name}: {e}")
                return
            def fillTree(node):
                try:
                    child_nodes_response = thread_client.get_nodes(parentnode_id=node.id)
                except RpcError as e:
                    if e.code() == StatusCode.UNAVAILABLE:
                        logging.error(f"Service unavailable while retrieving child nodes for node {node.id}: {e}")
                        print("Fatal error, check log file.", file=sys.stderr);exit(1)
                    logging.error(f"Failed to retrieve child nodes for node {node.id}: {e}")
                    return {"tag_id": node.tagId, "child_nodes": []}
                child_nodes = []
                for child_node in child_nodes_response:
                    if child_node.HasField("error"):
                        if child_node.error.code == StatusCode.NOT_FOUND:
                            return {"tag_id": node.tagId, "child_nodes": []}
                        else:
                            logging.warning(f"Error retrieving child node {child_node.id}: {child_node.error}")
                            return {"tag_id": node.tagId, "child_nodes": []}
                    child_nodes.append(fillTree(child_node))
                return {"tag_id": node.tagId, "child_nodes":child_nodes}
            
            filled_tree = fillTree(rootnode)
            hierarchies_lock.acquire()
            hierarchies.append({
                "name":hierarchy_name,
                "tagset_id":hierarchy_tagset_id,
                "rootnode": filled_tree
            })
            hierarchies_lock.release()
            hierarchies_pbar.update(1)

        hierarchies_executor = ThreadPoolExecutor(max_workers=10)
        for hierarchy_response in response_hierarchies:
            if hierarchy_response.HasField("error"):
                if hierarchy_response.error.code == StatusCode.NOT_FOUND:
                    continue
                else:
                    logging.warning(f"Error retrieving hierarchy {hierarchy_response.id}: {hierarchy_response.error}")
                    return
            hierarchies_pbar.total += 1
            hierarchies_pbar.refresh()
            hierarchies_executor.submit(processHierarchy, hierarchy_response)

        hierarchies_executor.shutdown()


        data = {"tagsets": tagsets, "medias": medias, "hierarchies": hierarchies}
        print("All data loaded, dumping json file")
        with open(path, "w") as json_file:
            json.dump(data, json_file, indent=4)
        
        end_time = time.time()
        print("Elapsed time: %f seconds" % (end_time-start_time))


if __name__ == "__main__":
    handler = FastJSONHandler()
    handler.importFile("./client/json_testfiles/ts+m+h.json")
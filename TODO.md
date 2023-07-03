**General**
[~] Check what the actual sql scripts are doing
[x] Implement all tables generation
[] Generate tags from CSV
[] Try to make your own csv as a user
[x] Share proto files for client & server
PLUGINS   how do you generate tags (as grpc clients) 

[] Re-implement all CLI commands with class-based interactions*


[x] Taggings: the method get_tagging(id, id) is irrelevent, we need a get_taggings(media_id) and a get_medias(tag_id)
[x] Tags : implement a create-tag method


[] Implement all new functions in server side
[] Implement CLI commands to call said functions


IMPORTANT
[] 



**GET**
[x] handle input errors

**GETALL**
[x] implement cli command

**ADD**
[x] implement cli command with _path to directory_
[x] implement double streaming behaviour for loading batches of elements --> loading progress
[x]	implement single file add
[]	implement linked tables filling

**REMOVE**    // is it really useful ?
[x] implement server/client functions with double streaming behaviour for loading batches of elements --> loading progress
[x] implement cli command

**RESET**
[x] RPC call to reset DB, i.e. run the init script ?
[x] CLI command to do so

**EXPORT TAGS FROM DB**
[x] get ID from URI
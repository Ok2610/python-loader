// TO GENERATE AN IMAGE
docker build -t [name] .

// To RUN AN IMAGE => CREATE THE CONTAINER
docker run -p -d [localport]:[containerport] [image-name]

// TO START OR RESTART AN EXISTING CONTAINER
docker restart [container-ID]

// TO START VENV IN EACH SUBFOLDER
source .venv/Scripts/activate

// TO GENERATE PROTOC FILES (inside server subfolder)
python3 -m grpc_tools.protoc -I./protos --python_out=. --pyi_out=. --grpc_python_out=. ./protos/medias.proto

Then copy pb files to client side
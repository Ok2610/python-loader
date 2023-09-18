__M3-LOADER__
***
**How to use it ?**

_Requirements:_
- Python 3.10.11 or higher

_Server side:_
- Open a terminal in the _server/_ directory.
- Create a python virtual environment using _python -m venv [venv name]_
- Activate it using _source [venv name]/Scripts/activate_
- Install requirements with _pip install -r requirements.txt_
- Update the database connection parameters in app.py, line 21
- Run the server using _python app.py_


<!-- // TO GENERATE AN IMAGE
$ docker build -t [name] .

// To RUN AN IMAGE => CREATE THE CONTAINER
$ docker run -p -d [localport]:[containerport] [image-name]

// TO START OR RESTART AN EXISTING CONTAINER
$ docker restart [container-ID] -->

// TO START PYTHON SERVER (in server folder)
$ source .venv/Scripts/activate
$ pip install requirements.txt
$ python app.py

// TO GENERATE PROTOC FILES
$ python3 -m grpc_tools.protoc -I../protos --python_out=. --pyi_out=. --grpc_python_out=. ../protos/medias.proto

Then copy pb files to client side

// TO START GO SERVER (in server folder)
$ go mod tidy
$ go run .

// TO RUN CLI WITH AUTO UPDATE (run in client folder)
$ pip install --editable .
then you can use 'loader [COMMAND]' in the console from anywhere 

// TO TEST ALL THE CLI FUNCTION
-> enable the client virtual environment (source .venv/Scripts/activate in client folder)
-> cd tests
$ pytest -vv
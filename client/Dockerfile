FROM python:3.11-slim

WORKDIR /app

COPY *.py requirements.txt ./

COPY ./filemgmt ./filemgmt

COPY ./tests ./tests

COPY ./data_loader_cli.egg-info ./data_loader_cli.egg-info

RUN pip install .

RUN pip install -r requirements.txt

CMD ["sleep", "infinity"]

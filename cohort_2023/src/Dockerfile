FROM python:3.9.1

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

WORKDIR /app
COPY /src/get_data.py get_data.py 
COPY /src/ingest_data.py ingest_data.py 

ENTRYPOINT [ "bash" ]

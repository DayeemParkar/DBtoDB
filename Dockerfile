# dockerfile - bp to build images
# image - template to run container
# container - actual running process with packaged code
FROM python:3.9.6

WORKDIR /docker-test

COPY requirements.txt .
COPY LICENSE .
COPY testing.txt .

RUN pip3 install -r requirements.txt

COPY ./Scripts ./Scripts
COPY ./Data ./Data
COPY ./Logs ./Logs

CMD [ "python", "./Scripts/dbtodb.py" ]
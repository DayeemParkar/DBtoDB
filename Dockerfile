FROM python:3.9.6

WORKDIR /docker-test

COPY requirements.txt .

ENV DBVARS="{'database': 'trade_data_db','user': 'postgres','password': 'Finserv@2023','host': '127.0.0.1','port': '5438'}" FILE_LOC=./Data/ FILE_NAME=custom_1988_2020.csv LOGGER_FILE_LOC=./Logs/ LOGGER_FILE_NAME=db_logs.log BATCH_SIZE=10000

RUN pip3 install -r requirements.txt

COPY ./Scripts ./Scripts
COPY ./Data ./Data
COPY ./Logs ./Logs

CMD [ "python", "./Scripts/dbtodb.py" ]
from flask import Flask, request
from dao import mysql_dao
from utilities import utilities
from dao import postgres_dao

app = Flask(__name__)


def get_flask_app():
    return app


@app.route('/get/connection', methods=("POST",))
def get_connection():
    db_config = request.json
    print("current engine : ", db_config.get("Engine"))
    connection = None
    err = ""

    if db_config.get("Engine") == "postgres":
        connection, err = postgres_dao.get_connection(db_config)
    else:
        connection, err = mysql_dao.get_connection(db_config)

    if connection is None:
        return utilities.get_response_json(connection, err)
    else:
        return utilities.get_response_json(connection, err)


@app.route('/create/table', methods=("POST",))
def create_table():
    config = request.json
    print("input config : ", config)
    err = mysql_dao.create_table(config)
    return utilities.get_response_json("Created table successfully", err)


@app.route('/export/data', methods=("POST",))
def export_data_as_file():
    config = request.json
    print("current engine : ", config.get("Engine"))
    connection = None
    if config.get("Engine") == "postgres":
        connection, err = postgres_dao.get_connection(config)
    else:
        connection, err = mysql_dao.get_connection(config)

    if err == "":
        response_data = None
        if config.get("Engine") == "postgres":
            response_data, err = postgres_dao.export_data_file(connection, config)
        else:
            response_data, err = mysql_dao.export_data_file(connection, config)
        return utilities.get_response_json(response_data, err)
    else:
        return utilities.get_response_json("", err)


@app.route('/import/data', methods=("POST",))
def import_data_as_file():
    config = request.json
    print("input config : ", config)
    connection = None
    if config.get("Engine") == "postgres":
        connection, err = postgres_dao.get_connection(config)
    else:
        connection, err = mysql_dao.get_connection(config)
    if err == "":
        response_data = None
        if config.get("Engine") == "postgres":
            response_data, err = postgres_dao.import_data_file(connection, config)
        else:
            response_data, err = mysql_dao.import_data_file(connection, config)

        return utilities.get_response_json(response_data, err)
    else:
        return utilities.get_response_json("", err)


@app.route('/convert/data/xlsxtocsv', methods=("POST",))
def convert_file_format():
    config = request.json
    print("input config : ", config)
    response_data, err = mysql_dao.convert_xlsx_csv(config)
    if err == "":
        return utilities.get_response_json(response_data, err)
    else:
        return utilities.get_response_json("", err)


@app.route('/convert/data/xlsx/sql', methods=("POST",))
def convert_sql_format():
    config = request.json
    print("input config : ", config)
    response_data, err = mysql_dao.convert_sql_file(config)
    if err == "":
        return utilities.get_response_json(response_data, err)
    else:
        return utilities.get_response_json("", err)
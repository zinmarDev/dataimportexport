import psycopg2
from dao import mysql_dao


def get_connection(db_config):
    host = db_config.get("Host")
    port = db_config.get("Port")
    user_name = db_config.get("UserName")
    password = db_config.get("Password")
    database = db_config.get("DatabaseName")
    schema_name = db_config.get("SchemaName")
    print("postgres host : ", host, " and port : ", port, schema_name)
    error = ""
    try:
        connection = psycopg2.connect(host=host,
                                      port=port,
                                      database=database,
                                      user=user_name,
                                      password=password
                                      )
        if connection is not None:
            print("You're connected to database.")
            return connection, error
        else:
            return connection, error
    except Exception as error:
        print("Error while connecting to POSTGRESQL")
        return None, error


def export_data_file(connection, config):
    columns = "*"
    conditions = ""
    conditions = mysql_dao.prepare_conditions(conditions, config.get("ANDConditions"), "AND")
    conditions = mysql_dao.prepare_conditions(conditions, config.get("ORConditions"), "OR")
    schema_name = config.get("SchemaName")
    if schema_name is None or schema_name == "":
        schema_name = "public"

    if config.get("ColumnList") is not None and len(config.get("ColumnList")) > 0:
        columns = ""
        for col in config.get("ColumnList"):
            if columns != "":
                columns += ", "+col
            else:
                columns += col

    query = "COPY " + schema_name + "." + config.get("TableName")+""

    if columns != "*":
        query += "("+columns+")"

    if conditions != "":
        query = "COPY (SELECT " + columns + " FROM " + schema_name + "." + config.get("TableName")+" WHERE " + conditions + ")"

    query += " To '"+config.get("FilePath") + "' Delimiter '" + config.get("Delimiter") + "' csv header"

    print("export data query : ", query)

    cursor = connection.cursor()
    err = ""
    try:
        result = cursor.execute(query)
        print("query is executed successfully ", result)

    except Exception as error:
        err = error
        print("Failed to create table in Postgresql: {}".format(error))
    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            print("postgresql connection is closed")
    return "data are exported successfully", err


def import_data_file(connection, config):
    schema_name = config.get("SchemaName")
    if schema_name is None or schema_name == "":
        schema_name = "public"

    query = "COPY "+ schema_name + "."+ config.get("TableName") + " FROM '"+config.get("FilePath") + "' Delimiter '"+ config.get("Delimiter") + "' csv header"
    print("import data query : ", query)

    cursor = connection.cursor()
    err = ""
    try:
        cursor.execute(query)
        connection.commit()
        print("query is executed successfully ")

    except Exception as error:
        err = error
        print("Failed to create table in Postgres: {}".format(error))
    finally:
        if connection is not None:
            connection.close()
            print("Postgres connection is closed")
    return "data is imported successfully", err

import logging

import azure.functions as func
import logging
import os

import azure.functions as func
import pandas as pd
import snowflake.connector
import yaml
import pathlib
from test_cli.utils.config_loader import ConfigLoader


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    with open(pathlib.Path(__file__).parent / "conf/parameter.yml", "r") as file:
        parameters = yaml.load(file, Loader=ConfigLoader)
    with open(pathlib.Path(__file__).parent / "conf/funct_query.sql", "r") as file:
        core_query = file.read()

    conn = snowflake.connector.connect(
        user=os.environ["USER_SF"],
        password=os.environ["PSW_SF"],
        account=os.environ["ACCOUNT_SF"],
        **parameters["snowflake_config"]
    ) 

    cur = conn.cursor()
    cur.execute(core_query)
    core_data = cur.fetch_pandas_all()
    
    return func.HttpResponse(body=core_data.to_json(date_format="iso", orient="split"))

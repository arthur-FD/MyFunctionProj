import logging

import azure.functions as func
import logging
import os

import azure.functions as func
import pandas as pd
import snowflake.connector
import yaml
import pathlib
from oem_view_improved.utils.config_loader import ConfigLoader


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    countries = req.params.get('countries')

    with open(pathlib.Path(__file__).parent / "conf/parameter.yml", "r") as file:
        parameters = yaml.load(file, Loader=ConfigLoader)
    with open(pathlib.Path(__file__).parent / "conf/funct_query.sql", "r") as file:
        core_query = file.read()
    req_body = req.get_json()
    print(req_body)


    if req_body['regions']!=[] and req_body['countries']!=[]:
        if len (req_body['countries'])==1:
            countries=str(tuple(req_body['countries']))[:-2]+')'
        else:
            countries=str(tuple(req_body['countries']))
        if len(req_body['regions'])==1:
            regions=str(tuple(req_body['regions']))[:-2]+')'
        else:
            regions=str(tuple(req_body['regions']))        
        core_query=f'''
            SELECT  EV_VOLUMES_TEST.OEM_GROUP, EV_VOLUMES_TEST.BRAND,EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.MODEL_ID,VEHICLE_SPEC_TEST.CATHODE, EV_VOLUMES_TEST.PERIOD_GRANULARITY,EV_VOLUMES_TEST.DATE,SUM(EV_VOLUMES_TEST.VALUE)
            FROM EV_VOLUMES_TEST
            INNER JOIN VEHICLE_SPEC_TEST ON EV_VOLUMES_TEST.MODEL_ID=VEHICLE_SPEC_TEST.MODEL_ID
            INNER JOIN GEO_COUNTRY_TEST ON EV_VOLUMES_TEST.SALES_COUNTRY_CODE=GEO_COUNTRY_TEST.COUNTRY_CODE WHERE EV_VOLUMES_TEST.SALES_COUNTRY_CODE IN {countries} and GEO_COUNTRY_TEST.REGION IN {regions}
            GROUP BY EV_VOLUMES_TEST.OEM_GROUP, EV_VOLUMES_TEST.BRAND,EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.MODEL_ID,VEHICLE_SPEC_TEST.CATHODE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,EV_VOLUMES_TEST.DATE,GEO_COUNTRY_TEST.REGION,EV_VOLUMES_TEST.SALES_COUNTRY_CODE
                '''
    elif req_body['regions']==[] and req_body['countries']!=[]:
        if len (req_body['countries'])==1:
            countries=str(tuple(req_body['countries']))[:-2]+')'
        else:
            countries=str(tuple(req_body['countries']))
        core_query=f'''
            SELECT  EV_VOLUMES_TEST.OEM_GROUP, EV_VOLUMES_TEST.BRAND,EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.MODEL_ID,VEHICLE_SPEC_TEST.CATHODE, EV_VOLUMES_TEST.PERIOD_GRANULARITY,EV_VOLUMES_TEST.DATE,SUM(EV_VOLUMES_TEST.VALUE)
            FROM EV_VOLUMES_TEST
            INNER JOIN VEHICLE_SPEC_TEST ON EV_VOLUMES_TEST.MODEL_ID=VEHICLE_SPEC_TEST.MODEL_ID
            INNER JOIN GEO_COUNTRY_TEST ON EV_VOLUMES_TEST.SALES_COUNTRY_CODE=GEO_COUNTRY_TEST.COUNTRY_CODE WHERE EV_VOLUMES_TEST.SALES_COUNTRY_CODE IN {countries}
            GROUP BY EV_VOLUMES_TEST.OEM_GROUP, EV_VOLUMES_TEST.BRAND,EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.MODEL_ID,VEHICLE_SPEC_TEST.CATHODE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,EV_VOLUMES_TEST.DATE,GEO_COUNTRY_TEST.REGION,EV_VOLUMES_TEST.SALES_COUNTRY_CODE
                '''
    elif req_body['regions']!=[] and req_body['countries']==[]:
        if len(req_body['regions'])==1:
            regions=str(tuple(req_body['regions']))[:-2]+')'
        else:
            regions=str(tuple(req_body['regions']))      
        core_query=f'''
            SELECT  EV_VOLUMES_TEST.OEM_GROUP, EV_VOLUMES_TEST.BRAND,EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.MODEL_ID,VEHICLE_SPEC_TEST.CATHODE, EV_VOLUMES_TEST.PERIOD_GRANULARITY,EV_VOLUMES_TEST.DATE,SUM(EV_VOLUMES_TEST.VALUE)
            FROM EV_VOLUMES_TEST
            INNER JOIN VEHICLE_SPEC_TEST ON EV_VOLUMES_TEST.MODEL_ID=VEHICLE_SPEC_TEST.MODEL_ID
            INNER JOIN GEO_COUNTRY_TEST ON EV_VOLUMES_TEST.SALES_COUNTRY_CODE=GEO_COUNTRY_TEST.COUNTRY_CODE WHERE GEO_COUNTRY_TEST.REGION IN {regions}
            GROUP BY EV_VOLUMES_TEST.OEM_GROUP, EV_VOLUMES_TEST.BRAND,EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.MODEL_ID,VEHICLE_SPEC_TEST.CATHODE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,EV_VOLUMES_TEST.DATE,GEO_COUNTRY_TEST.REGION,EV_VOLUMES_TEST.SALES_COUNTRY_CODE
                '''

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

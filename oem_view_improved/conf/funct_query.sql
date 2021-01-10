SELECT  GEO_COUNTRY_TEST.REGION,EV_VOLUMES_TEST.SALES_COUNTRY_CODE,EV_VOLUMES_TEST.OEM_GROUP, EV_VOLUMES_TEST.BRAND,EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.MODEL_ID,VEHICLE_SPEC_TEST.CATHODE, EV_VOLUMES_TEST.PERIOD_GRANULARITY,EV_VOLUMES_TEST.DATE,SUM(EV_VOLUMES_TEST.VALUE)
FROM EV_VOLUMES_TEST
LEFT JOIN VEHICLE_SPEC_TEST ON EV_VOLUMES_TEST.MODEL_ID=VEHICLE_SPEC_TEST.MODEL_ID
INNER JOIN GEO_COUNTRY_TEST ON EV_VOLUMES_TEST.SALES_COUNTRY_CODE=GEO_COUNTRY_TEST.COUNTRY_CODE
GROUP BY EV_VOLUMES_TEST.OEM_GROUP, EV_VOLUMES_TEST.BRAND,EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.MODEL_ID,VEHICLE_SPEC_TEST.CATHODE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,EV_VOLUMES_TEST.DATE,GEO_COUNTRY_TEST.REGION,EV_VOLUMES_TEST.SALES_COUNTRY_CODE

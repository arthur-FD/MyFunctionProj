SELECT  OEM_GROUP, BRAND,PROPULSION,MODEL_ID, PERIOD_GRANULARITY,DATE,SUM(VALUE)
FROM EV_VOLUMES_TEST
GROUP BY OEM_GROUP, BRAND,PROPULSION,MODEL_ID,PERIOD_GRANULARITY,DATE
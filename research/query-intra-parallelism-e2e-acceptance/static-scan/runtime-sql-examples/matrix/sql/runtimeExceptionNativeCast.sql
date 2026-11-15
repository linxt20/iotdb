SELECT CAST(device_id AS INT64) AS invalid_value
FROM runtime_dop_e2e.telemetry
WHERE (SIN(CAST(s1 AS DOUBLE)) * SIN(CAST(s1 AS DOUBLE))
       + COS(CAST(s1 AS DOUBLE)) * COS(CAST(s1 AS DOUBLE))) > 0.5

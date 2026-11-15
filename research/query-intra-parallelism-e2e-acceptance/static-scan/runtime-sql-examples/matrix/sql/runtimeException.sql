SELECT time, device_id, CAST(device_id AS INT64) AS native_cast_failure
FROM runtime_dop_e2e.telemetry
WHERE (CASE
  WHEN (SIN(SIN(SIN(SIN(SIN(CAST(s1 AS DOUBLE)))))) * SIN(SIN(SIN(SIN(SIN(CAST(s1 AS DOUBLE))))))
        + COS(SIN(SIN(SIN(SIN(CAST(s1 AS DOUBLE)))))) * COS(SIN(SIN(SIN(SIN(CAST(s1 AS DOUBLE))))))) > 0.5
  THEN CASE WHEN device_id = 'd063' AND time >= 1815448512 AND time < 1815448576 THEN s1 - s1 + 1 ELSE s1 - s1 END
  ELSE s1 - s1
END) = 1

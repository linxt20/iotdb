SELECT time, device_id, g, s1, s2
FROM runtime_dop_e2e.telemetry
WHERE (CASE
  WHEN (SIN(SIN(SIN(SIN(SIN(CAST(s1 AS DOUBLE)))))) * SIN(SIN(SIN(SIN(SIN(CAST(s1 AS DOUBLE))))))
        + COS(SIN(SIN(SIN(SIN(CAST(s1 AS DOUBLE)))))) * COS(SIN(SIN(SIN(SIN(CAST(s1 AS DOUBLE))))))) > 0.5
  THEN CASE WHEN time >= 0 AND time < 64 THEN s1 - s1 + 1 ELSE s1 - s1 END
  ELSE s1 - s1
END) = 1

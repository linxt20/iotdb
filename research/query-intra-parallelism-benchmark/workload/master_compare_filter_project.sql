SELECT device_id, time, s1 + 1 AS s1_plus_one, s2 * 2.0 AS s2_twice
FROM m1db.bench
WHERE s1 % 7919 = 0;

SELECT device_id, time, s1, s2
FROM m1db.bench
WHERE s1 % 7919 = 0;

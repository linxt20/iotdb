SELECT device_id, time, s1, s2
FROM benchdb.bench
WHERE s1 % 7919 = -1;

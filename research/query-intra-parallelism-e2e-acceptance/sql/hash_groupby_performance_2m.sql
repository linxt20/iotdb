SELECT g,
       COUNT(*) AS rows,
       SUM(s1) AS sum_s1,
       MIN(s1) AS min_s1,
       MAX(s1) AS max_s1
FROM p0multi.op_hash_perf_2m
GROUP BY g;

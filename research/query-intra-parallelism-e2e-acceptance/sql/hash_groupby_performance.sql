SELECT g, COUNT(*) AS rows, SUM(s1) AS sum_s1
FROM p0multi.op_hash_perf
GROUP BY g;

-- Write hive script to calculate the longest period of stay of couples with children
SELECT max(datediff(srch_co, srch_ci)) AS days 
FROM train 
WHERE srch_adults_cnt=2 
AND srch_children_cnt>0;
WITH h AS (
  SELECT
    AVG(C_17_00021) avg_21,
    AVG(C_18_00052) avg_52,
    AVG(C_17_00021*C_18_00052) avg_21_52,
    AVG(C_18_00052*C_18_00052) avg_52_52
  FROM
    t_50999999_learn
--  WHERE 
--    c_datetime < TIMESTAMP('2014-07-21 10:00:00')
)
SELECT 
  avg_21 - ((avg_21_52 - avg_52 * avg_21) / (avg_52_52 - avg_52 * avg_52))*avg_52 AS w0,
  (avg_21_52 - avg_52 * avg_21) / (avg_52_52 - avg_52 * avg_52) AS w1
FROM 
  h;

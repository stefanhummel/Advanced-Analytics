create view t_50999999_learn AS
  SELECT
    c_18_00052,
    c_17_00021
  FROM
    t_50999999
  WHERE
    c_datetime < TIMESTAMP('2014-07-21 10:00:00')
;

create view t_50999999_test AS
  SELECT
    c_18_00052,
    c_17_00021
  FROM
    t_50999999
  WHERE
    c_datetime >= TIMESTAMP('2014-07-21 10:00:00')
;

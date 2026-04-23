SELECT * FROM survey_db.course_metadata
where trainer_name is null or course_start is null;

set sql_safe_updates = 0;
DELETE FROM survey_db.course_metadata
where trainer_name is null or course_start is null;
set session sql_safe_updates = 1;

SELECT *
FROM survey_db.course_metadata
WHERE course_run IN (
  SELECT course_run
  FROM survey_db.course_metadata
  GROUP BY course_run
  HAVING COUNT(*) > 1
)
ORDER BY course_run;

SET SESSION sql_safe_updates = 0;
DELETE FROM survey_db.course_metadata WHERE course_run = 1089341 LIMIT 1;
-- DELETE FROM survey_db.course_metadata WHERE course_run = 1229915 and trainer_name is null;
DELETE FROM survey_db.course_metadata WHERE course_run = 1042509 LIMIT 1;
-- DELETE FROM survey_db.course_metadata WHERE course_run = 1245211 LIMIT 1;
-- DELETE FROM survey_db.course_metadata WHERE course_run = 1244560 LIMIT 1;
SET SESSION sql_safe_updates = 1;

select * from survey_db.course_metadata
order by course_start ASC;
-- edit V2 -----------
select * from survey_db.course_metadata WHERE course_run = 940316;

SET SESSION sql_safe_updates = 0;
update survey_db.course_metadata
-- set course_start = '2024-08-20',course_end = '2024-09-10' where course_run = 961864;
set trainer_name = 'William Chua Sow Ngang' where course_run = 940316;
SET SESSION sql_safe_updates = 1;
-- edit V3 --------

-- Cancel import jobs that are stuck
CANCEL JOBS (WITH x AS (SHOW JOBS) SELECT job_id FROM x WHERE job_type='IMPORT' and status != 'failed');
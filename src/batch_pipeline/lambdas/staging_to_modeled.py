from database_commons import get_db_connection

LOAD_DEPARTMENTS = """
INSERT INTO departments (id,department)
    SELECT DISTINCT id,department FROM staging_departments 
ON CONFLICT (id) DO 
UPDATE 
	SET department=EXCLUDED.department;
"""

LOAD_JOBS = """
INSERT INTO jobs (id,job)
    SELECT distinct id,job FROM staging_jobs 
ON CONFLICT (id) DO 
UPDATE 
	SET job=EXCLUDED.job;
"""

LOAD_HIRED_EMPLOYEES = """
INSERT INTO hired_employees  (name, hired_datetime, department_id, job_id)
    SELECT distinct name, hired_datetime, department_id , job_id  
    FROM staging_hired_employees 
    WHERE 
        name IS NOT NULL AND
        hired_datetime IS NOT NULL AND
        department_id IS NOT NULL AND job_id is not NULL
ON CONFLICT (hired_datetime, name, department_id, job_id) DO 
UPDATE
	SET 
	name=EXCLUDED.name,
	hired_datetime = EXCLUDED.hired_datetime,
	department_id=EXCLUDED.department_id,
	job_id=EXCLUDED.job_id;
"""
CLEAN_STAGING = """
DELETE FROM staging_departments;
DELETE FROM staging_jobs;
DELETE FROM staging_hired_employees;
"""


def run(event, _):
    connection = get_db_connection(event)
    try:
        with connection:
            with connection.cursor() as cur:
                cur.execute(LOAD_DEPARTMENTS)
                cur.execute(LOAD_JOBS)
                cur.execute(LOAD_HIRED_EMPLOYEES)
                cur.execute(CLEAN_STAGING)
    except Exception as e:
        connection.rollback()
    else:
        connection.commit()
    finally:
        connection.close()

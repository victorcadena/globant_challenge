from psycopg2.extras import RealDictCursor
from database_commons import get_db_connection

def run(event, _):
    print("Got into the event for online transactions!")
    print(event)

def get_employees_hired_by_department(event):
    sql = """
        select
            department, 
            job,
            sum(case 
                    when date_part('quarter', hired_datetime) = 1 then 1
                    else 0
                end) as "Q1",
            sum(case 
                    when date_part('quarter', hired_datetime) = 2 then 1
                    else 0
                end) as "Q2",
            sum(case 
                    when date_part('quarter', hired_datetime) = 3 then 1
                    else 0
                end) as "Q3",
            sum(case 
                    when date_part('quarter', hired_datetime) = 4 then 1
                    else 0
                end) as "Q4"
        from
        (
            select 
            departments.department as department,
            jobs.job  as job,
            hired_datetime 
            from 
            public.hired_employees employees join public.departments departments 
            on employees.department_id  = departments.id 
            and hired_datetime < '2022-01-01' and hired_datetime >= '2021-01-01'
            join public.jobs jobs 
            on employees.job_id = jobs.id
        ) filtered_employees
        group by department, job
        order by department, job
    """
    return _get_json_results_from_db(event, sql)
    
def get_above_average_departments(event):
    sql = """
    with mean_employees as (
	select avg(department_hire_count) as average_hire
        from (
            select 
            count(*) as department_hire_count,
            departments.department
            from
            public.hired_employees employees join public.departments departments 
            on employees.department_id  = departments.id  and
            hired_datetime < '2022-01-01' and hired_datetime >= '2021-01-01'
            group by department
        ) hires_by_department
    )
    select 
    departments.id,
    departments.department,
    count(*) as hired
    from 
    public.hired_employees employees join public.departments departments 
    on employees.department_id  = departments.id
    group by departments.id, departments.department
    having count(*) > (select average_hire from mean_employees)
    order by hired desc
    """
    return _get_json_results_from_db(event, sql)

def _get_json_results_from_db(event, sql):
    connection = get_db_connection(event)
    result = []
    try:
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                result = cur.fetchall()
        return result
    except Exception as e:
        raise e
    finally:
        connection.close()

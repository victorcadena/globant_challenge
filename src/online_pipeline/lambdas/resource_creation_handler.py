import json
from database_commons import get_db_connection
from api_commons import form_response, handle_exception
import psycopg2.extras as extras

def request_handling_facade(event, _):
    try:
        supported_resources_to_handler = {
            "/employees": create_employees,
        }
        resource = event["resource"]
        request_body = event["body"]
        request_body = request_body if isinstance(request_body, dict) else json.loads(request_body)
        result = supported_resources_to_handler[resource](request_body)
        return form_response(result)
    except AssertionError as e:
        return handle_exception(e, 400)
    except Exception as e:
        return handle_exception(e, 500)

def create_employees(employees):
    for employee in employees:
        assert "name" in employee \
        and "department" in employee \
        and "job" in employee, "The employee must have name, department and job, check the payload"
    
    assert len(employees) <= 1000, "The maximum batch size is 1000, please fix the payload"
    batch_insert_sql = """
        with mapped_departments_employee as (
        SELECT temp_employee.*, d.id AS department_id
        FROM  (
            VALUES
                (%(name)s, %(department)s, %(job)s)
                
            ) temp_employee (name, department, job)
        LEFT JOIN departments d on d.department  = temp_employee.department
    ),
    mapped_jobs_employee as (
        SELECT mapped_departments_employee.*, j.id AS job_id
        FROM  mapped_departments_employee
        LEFT JOIN jobs j on j.job  = mapped_departments_employee.job
    )
    insert into hired_employees (name, department_id, job_id)
    select name, department_id, job_id from mapped_jobs_employee
    """
    try:
        connection = get_db_connection()
        with connection:
            with connection.cursor() as cur:
                extras.execute_batch(cur, batch_insert_sql, employees)
        connection.commit()
    except Exception as e:
        print(e)
        raise e
    else:
        return {"result": "Batch inserted correctly"}
    finally:
        connection.close()
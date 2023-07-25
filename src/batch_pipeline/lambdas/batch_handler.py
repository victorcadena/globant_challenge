import boto3
from datetime import datetime
from api_commons import form_response, handle_exception

TARGET_BATCH_STEP_FUNCTION = "arn:aws:states:us-west-2:727474809098:stateMachine:load_hr_database"

def run(event, _):
    try:
        client = boto3.client("stepfunctions")
        execution_name = get_execution_name()
        client.start_execution(
            stateMachineArn=TARGET_BATCH_STEP_FUNCTION,
            name=execution_name
        )
        response = {"message": "Batch Process started succesfully"}
        return form_response(response)
    except Exception as e:
        return handle_exception(e, 500)

def get_execution_name(): 
    now_string = f"{int(datetime.timestamp(datetime.now()) * 1000)}"
    return f"ExecutionTimestamp={now_string}-BatchHRPipeline"

import boto3
from datetime import datetime

TARGET_BATCH_STEP_FUNCTION = "arn:aws:iam::727474809098:role/GlobantChallengeStack-statemachineRole8DD785C2-MDIYI07DV641"

def run(event, _):
    client = boto3.client("stepfunctions")
    execution_name = get_execution_name()
    client.start_execution(
        stateMachineArn=TARGET_BATCH_STEP_FUNCTION,
        name=execution_name
    )


def get_execution_name(): 
    now_string = f"{int(datetime.now().timestamp * 1000)}"
    return f"ExecutionTimestamp={now_string}-BatchHRPipeline"

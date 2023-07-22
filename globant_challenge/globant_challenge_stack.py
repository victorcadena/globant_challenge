from aws_cdk import (
    aws_s3 as s3,
    Stack,
    aws_lambda as lambda_,
    Duration
)
from constructs import Construct

class GlobantChallengeStack(Stack):
    
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_assets = "./lambdas"
        hr_bucket = s3.Bucket(
            scope,
            "hr_data_globant",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_sSL=True,
            versioned=True,
            bucket_name="hr-data-globant"
        )

        landing_bucket = s3.Bucket(
            scope,
            "landing_globant",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_sSL=True,
            versioned=True,
            bucket_name="hr-data-globant"
        )

        self.function = lambda_.Function(
            self,
            "scheduled-lambda",
            function_name="move_hr_raw_to_landing",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(lambda_assets),
            handler="move_to_landing.run",
            timeout=Duration.seconds(300),
        )



from aws_cdk import (
    aws_s3 as s3,
    Stack
)
from constructs import Construct

class GlobantChallengeStack(Stack):
    
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

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
            "landing_bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_sSL=True,
            versioned=True,
            bucket_name="hr-data-globant"
        )



from turtle import Turtle
from aws_cdk import (
    aws_s3 as s3,
    Stack,
    aws_lambda as lambda_,
    aws_ec2 as ec2,
    aws_rds as rds,
    Duration,
    RemovalPolicy
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


        default_vpc = ec2.Vpc.from_lookup(self, "default_vpc",
            is_default=True
        )

        hr_db_instance = rds.DatabaseInstance(
            vpc=default_vpc,
            vpc_subnets=ec2.SubnetType.PUBLIC,
            engine=rds.PostgresEngineVersion.VER_13_2,
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO),
            credentials = rds.Credentials.from_generated_secret("hr_database_pg"), # This secret will be created after deploy
            multi_az=False, #True in real life for redundancy
            allocated_storage=20,
            max_allocated_storage=30,
            allow_major_version_upgrade=False,
            auto_minor_version_upgrade=True,
            removal_policy=RemovalPolicy.RETAIN,
            database_name="hr",
            publicly_accessible=True #False in real life
        )
        # in prod do something like hr_db_instance.connections.allow_from(...)



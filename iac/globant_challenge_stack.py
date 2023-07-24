from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_apigateway as apigateway,
    aws_secretsmanager as secrets,
    aws_iam as iam
)
from constructs import Construct
import os

class GlobantChallengeStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.create_buckets()
        self.create_fast_metadata_store()
        self.create_target_database()
        self.create_raw_to_landing_processing()
        self.create_dataset_load_to_staging_processing()
        self.create_step_function()
        self.create_api()


    def create_buckets(self):
        self.globant_hr_bucket = s3.Bucket(self, "globant_hr",
            bucket_name="globant-hr",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN
        )

        self.landing_bukcet: s3.Bucket = s3.Bucket(self, "landing_globant",
            bucket_name="landing-globant",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        

        self.pipelines_metadata = s3.Bucket(self, "pipelines_metadata_globant",
            bucket_name="pipelines-metadata-globant",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN
        )

        # All the account can read, do not do in PROD, principle of least access
        self.globant_hr_bucket.grant_read_write(iam.AccountRootPrincipal())
        self.landing_bukcet.grant_read_write(iam.AccountRootPrincipal())
        self.pipelines_metadata.grant_read_write(iam.AccountRootPrincipal())
        

    def create_fast_metadata_store(self):
        table: dynamodb.Table = dynamodb.Table(
            self,
            "processed_tracker",
            table_name="processed_tracker",
            partition_key=dynamodb.Attribute(name="pipeline_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="last_processed", type=dynamodb.AttributeType.STRING),
            read_capacity=1,
            write_capacity=1,
        )
        table.grant_read_write_data(iam.AccountRootPrincipal())

    def create_target_database(self):
        vpc = ec2.Vpc.from_lookup(self, "default_vpc", is_default=True)
        
        self.hr_db_instance: rds.DatabaseInstance = rds.DatabaseInstance(self, "hr_database",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_14),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.SMALL),
            credentials=rds.Credentials.from_generated_secret("hr"),  # Optional - will default to 'admin' username and generated password
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            database_name="hr",
            removal_policy=RemovalPolicy.DESTROY
        )

        self.lambdas_role = iam.Role(
            self, 
            "read_db_secret", 
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )
        # self.hr_db_instance.grant_connect(self.lambdas_role) -> do in PROD: Not doing this se we can check the db from outside
        self.hr_db_instance.secret.grant_read(self.lambdas_role)

    def create_raw_to_landing_processing(self):
        self.raw_to_landing_lambda = lambda_.Function(self, "raw_to_landing",
            code=lambda_.Code.from_asset(os.path.join(".", "src", "batch_pipeline", "lambdas")),
            handler="raw_to_landing.run",
            runtime=lambda_.Runtime.PYTHON_3_9,
            environment={
                "TARGET_DB_CREDENTIALS_SECRET": secrets.Secret.from_secret_name_v2(self, "hr_db_scret", "hr").secret_arn
            }
        )

        hr_new_data_event = event_sources.S3EventSource(
            self.globant_hr_bucket,
            events=[s3.EventType.OBJECT_CREATED, s3.EventType.OBJECT_REMOVED],
            filters=[s3.NotificationKeyFilter(prefix="hr/")]
        )

        self.raw_to_landing_lambda.add_event_source(hr_new_data_event)
        
    def create_dataset_load_to_staging_processing(self):
        self.landing_to_stg: lambda_.Function = lambda_.Function(self, "landing_to_staging",
            code=lambda_.Code.from_asset(os.path.join(".", "src", "batch_pipeline", "lambdas")),
            handler="landing_to_staging_db.run",
            runtime=lambda_.Runtime.PYTHON_3_9,
            role=self.lambdas_role
        )

    def create_step_function(self):
        database_secret = self.hr_db_instance.secret.secret_arn
        source = self.landing_bukcet.s3_url_for_object("hr")
        departments_payload = sfn.TaskInput.from_object({
            "target_db_secret": database_secret,
            "dataset": "departments",
            "domain": "hr",
            "source": source
        })

        jobs_payload = sfn.TaskInput.from_object({
            "target_db_secret": database_secret,
            "dataset": "jobs",
            "domain": "hr",
            "source": source
        })

        employees_payload = sfn.TaskInput.from_object({
            "target_db_secret": database_secret,
            "dataset": "employees",
            "domain": "hr",
            "source": source
        })

        departments_to_stg_task = tasks.LambdaInvoke(
            self,
            "Load departments to staging",
            lambda_function=self.landing_to_stg,
            payload=departments_payload
        )

        jobs_to_stg_task= tasks.LambdaInvoke(
            self,
            "Load jobs to staging",
            lambda_function=self.landing_to_stg,
            payload=jobs_payload
        )

        employees_to_stg_task = tasks.LambdaInvoke(
            self,
            "Load employees to staging",
            lambda_function=self.landing_to_stg,
            payload=employees_payload
        )

        main_workflow = (
            departments_to_stg_task
            .next(jobs_to_stg_task)
            .next(employees_to_stg_task)
        )

        self.state_machine = sfn.StateMachine(
            self,
            "state-machine",
            state_machine_name="load_hr_database",
            definition=main_workflow,
            timeout=Duration.minutes(10),
            state_machine_type=sfn.StateMachineType.STANDARD, # for ETL

        )

    def create_api(self):
        start_batch_execution = lambda_.Function(
            self, "batch_execution",
            code=lambda_.Code.from_asset(os.path.join(".", "src", "batch_pipeline", "lambdas")),
            handler="batch_handler.run",
            runtime=lambda_.Runtime.PYTHON_3_9,
            environment={
                "PIPELINE_STEP_FUNCTION_ARN": self.state_machine.state_machine_arn
            }
        )

        batch_api = apigateway.LambdaRestApi(self, "batch_api",
            handler=start_batch_execution,
            proxy=False
        )

        batch_pipeline_execution = batch_api.root.add_resource("batch_process")
        batch_pipeline_execution.add_method("POST")

        # Online API Definition
        start_online_execution = lambda_.Function(self, "online_execution",
            code=lambda_.Code.from_asset(os.path.join(".", "src", "online_pipeline", "lambdas")),
            handler="online_handler.run",
            runtime=lambda_.Runtime.PYTHON_3_9,
            environment={
                "TARGET_DB_SECRET": secrets.Secret.from_secret_name_v2(self, "online_hr_db_secret", "hr").secret_arn
            }
        )

        online_api = apigateway.LambdaRestApi(self, "online_api",
            handler=start_online_execution,
            proxy=False
        )

        online_pipeline_execution = online_api.root.add_resource("employees")
        online_pipeline_execution.add_method("POST")

        online_pipeline_execution = online_api.root.add_resource("jobs")
        online_pipeline_execution.add_method("POST")

        online_pipeline_execution = online_api.root.add_resource("departments")
        online_pipeline_execution.add_method("POST")
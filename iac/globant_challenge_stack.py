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
        
        self.create_roles()
        self.create_buckets()
        self.create_target_database()
        self.create_source_to_raw_processing()
        self.create_raw_to_staging_processing()
        self.create_staging_to_modeled_processing()
        self.create_step_function()
        self.create_api()

    def create_roles(self):
        self.processing_lambdas_role = iam.Role(
            self, 
            "processing_lambdas_role", 
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        self.source_to_raw_lambda_role = iam.Role(
            self, 
            "source_to_raw_role", 
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )
        self.rds_secret_arn = "arn:aws:secretsmanager:us-west-2:727474809098:secret:rds_import-aEDh4L"
        secret = secrets.Secret.from_secret_attributes(self, "rds_import_s3_secret",
            secret_complete_arn=self.rds_secret_arn
        )

        secret.grant_read(self.processing_lambdas_role)



    def create_buckets(self):
        self.globant_hr_bucket = s3.Bucket(self, "globant_hr",
            bucket_name="globant-hr",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN
        )

        self.raw_bucket: s3.Bucket = s3.Bucket(self, "raw-globant",
            bucket_name="raw-globant-hr",
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
        self.globant_hr_bucket.grant_read(self.source_to_raw_lambda_role)
        self.raw_bucket.grant_read_write(self.source_to_raw_lambda_role)
        self.raw_bucket.grant_read_write(self.processing_lambdas_role)
        
        self.pipelines_metadata.grant_read_write(iam.AccountRootPrincipal())
        

    def create_target_database(self):
        vpc = ec2.Vpc.from_lookup(self, "default_vpc", is_default=True)
        security_group: ec2.SecurityGroup = ec2.SecurityGroup(self, "db_security_group",
            vpc=vpc,
            allow_all_outbound=True
        )

        security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(), 
            ec2.Port.tcp(5432), 
            'DB Connect from Anywhere'
        )
        
        self.hr_db_instance: rds.DatabaseInstance = rds.DatabaseInstance(self, "hr_database",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_14),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.SMALL),
            credentials=rds.Credentials.from_generated_secret("hr"),  # Optional - will default to 'admin' username and generated password
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            database_name="hr",
            removal_policy=RemovalPolicy.DESTROY,
            security_groups=[security_group],
            publicly_accessible=True
        )
        
        # self.hr_db_instance.grant_connect(self.lambdas_role) -> do in PROD: Not doing this se we can check the db from outside
        self.hr_db_instance.secret.grant_read(self.processing_lambdas_role)

    def create_source_to_raw_processing(self):
        self.source_to_raw = lambda_.Function(self, "source_to_raw_function",
            code=lambda_.Code.from_asset(os.path.join(".", "src", "batch_pipeline", "lambdas")),
            handler="source_to_raw.run",
            runtime=lambda_.Runtime.PYTHON_3_9,
            environment={
                "TARGET_DB_CREDENTIALS_SECRET": secrets.Secret.from_secret_name_v2(self, "hr_db_scret", "hr").secret_arn
            },
            role=self.source_to_raw_lambda_role,
            timeout=Duration.minutes(15)
        )

        hr_new_data_event = event_sources.S3EventSource(
            self.globant_hr_bucket,
            events=[s3.EventType.OBJECT_CREATED, s3.EventType.OBJECT_REMOVED],
            filters=[s3.NotificationKeyFilter(prefix="hr/")]
        )

        self.source_to_raw.add_event_source(hr_new_data_event)
        
    def create_raw_to_staging_processing(self):
        self.raw_to_stg: lambda_.Function = lambda_.Function(self, "raw_to_staging_lambda",
            code=lambda_.Code.from_asset(os.path.join(".", "src", "batch_pipeline", "lambdas")),
            handler="raw_to_staging_db.run",
            runtime=lambda_.Runtime.PYTHON_3_9,
            role=self.processing_lambdas_role,
            timeout=Duration.minutes(15)
        )

    def create_staging_to_modeled_processing(self):
        self.staging_to_modeled: lambda_.Function = lambda_.Function(self, "staging_to_modeled_lambda",
            code=lambda_.Code.from_asset(os.path.join(".", "src", "batch_pipeline", "lambdas")),
            handler="staging_to_modeled.run",
            runtime=lambda_.Runtime.PYTHON_3_9,
            role=self.processing_lambdas_role,
            timeout=Duration.minutes(15)
        )

    def create_step_function(self):
        database_secret = self.hr_db_instance.secret.secret_arn
        source = self.raw_bucket.bucket_name
        departments_payload = sfn.TaskInput.from_object({
            "target_db_secret": database_secret,
            "rds_secret_arn": self.rds_secret_arn,
            "dataset": "departments",
            "domain": "hr",
            "source": source
        })

        jobs_payload = sfn.TaskInput.from_object({
            "target_db_secret": database_secret,
            "rds_secret_arn": self.rds_secret_arn,
            "dataset": "jobs",
            "domain": "hr",
            "source": source
        })

        employees_payload = sfn.TaskInput.from_object({
            "target_db_secret": database_secret,
            "rds_secret_arn": self.rds_secret_arn,
            "dataset": "hired_employees",
            "domain": "hr",
            "source": source
        })

        departments_to_stg_task = tasks.LambdaInvoke(
            self,
            "Load departments to staging",
            lambda_function=self.raw_to_stg,
            payload=departments_payload
        )

        jobs_to_stg_task= tasks.LambdaInvoke(
            self,
            "Load jobs to staging",
            lambda_function=self.raw_to_stg,
            payload=jobs_payload
        )

        employees_to_stg_task = tasks.LambdaInvoke(
            self,
            "Load employees to staging",
            lambda_function=self.raw_to_stg,
            payload=employees_payload
        )

        validate_data_pass = sfn.Pass(self, "Validate Data")

        staging_to_modeled = tasks.LambdaInvoke(
            self,
            "Load staging to modeled",
            lambda_function=self.staging_to_modeled,
            payload=sfn.TaskInput.from_object({
                "target_db_secret": database_secret
            })
        )

        main_workflow = (
            departments_to_stg_task
            .next(jobs_to_stg_task)
            .next(employees_to_stg_task)
            .next(validate_data_pass)
            .next(staging_to_modeled)
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
            },
            timeout=Duration.minutes(1)
        )

        batch_api = apigateway.LambdaRestApi(self, "batch_api",
            handler=start_batch_execution,
            proxy=False
        )

        batch_pipeline_execution = batch_api.root.add_resource("batch_process")
        batch_pipeline_execution.add_method("POST")

        # Online Batch Creation Definition
        start_batch_upload_execution = lambda_.Function(self, "resource_creation",
            code=lambda_.Code.from_asset(os.path.join(".", "src", "online_pipeline", "lambdas")),
            handler="resource_creation_handler.request_handling_facade",
            runtime=lambda_.Runtime.PYTHON_3_9,
            environment={
                "TARGET_DB_SECRET": self.hr_db_instance.secret.secret_arn
            },
            timeout=Duration.minutes(15),
            role=self.processing_lambdas_role
        )
        self.state_machine.grant_execution(self.processing_lambdas_role)

        online_api = apigateway.LambdaRestApi(self, "online_api",
            handler=start_batch_upload_execution,
            proxy=False
        )

        online_pipeline_execution = online_api.root.add_resource("employees")
        online_pipeline_execution.add_method("POST")

        # Online reports definition
        start_report_execution = lambda_.Function(self, "reports_function",
            code=lambda_.Code.from_asset(os.path.join(".", "src", "online_pipeline", "lambdas")),
            handler="reports_handler.run",
            runtime=lambda_.Runtime.PYTHON_3_9,
            environment={
                "TARGET_DB_SECRET": self.hr_db_instance.secret.secret_arn
            },
            timeout=Duration.minutes(15),
            role=self.processing_lambdas_role
        )

        reports_api = apigateway.LambdaRestApi(self, "reports_api",
            handler=start_report_execution,
            proxy=False
        )

        employees_by_department_resource = reports_api.root.add_resource("employees_by_department")
        employees_by_department_resource.add_method("GET")

        abover_average_departments_resource = reports_api.root.add_resource("abover_average_departments")
        abover_average_departments_resource.add_method("GET")

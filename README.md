
# Globant Challenge

### Architecture
The pipeline is done in an idempotent and scalable way to get great performance. 

![globant_challenge_architecture](https://github.com/victorcadena/globant_challenge/assets/4828992/b89d2396-601a-48e4-a443-cf230ed74336)


### How to run the IAC code and deploy

Remember, you should authenticate before this 

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

### Production considerations

Before doing anything, please consider this 

1. Database scale
2. Authentication and authorization
3. Model and data validation online
4. Data Validation in the pipeline
5. Scale the lambdas 1 per file via parallel iteration in step function
6. Decouple the stack for single responsibility
7. CI/CD and tests
8. If reports are needed on scale materialized views or sending to a data mart
9. Use roles instead of authentication with keys on the import from RDS
10. Event notification from source bucket
11. Alarms on quality and be able to retry failed records
12. If needed multi region DNS routing, database replication over regions
13. Have an ORM for a better integration with the DB, the relationship, keys, model validation, etc…
14. Change to Serial the DB keys, for the migration purpose was not done
15. Logging instead of printing
16. Parameters and configuration in a common place, not inside stack code
17. CloudWatch alarms on failed pipeline
18. Include pgBouncer for a connection pool and reuse of connections to the DB

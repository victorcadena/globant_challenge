#!/usr/bin/env python3
import os

import aws_cdk as cdk

from globant_challenge.globant_challenge_stack import GlobantChallengeStack


app = cdk.App()

import aws_cdk as cdk
from globant_challenge.cicd_stack import CICDSTack

app = cdk.App()
CICDSTack(app, "CICDStack",
    env=cdk.Environment(account="727474809098", region="eu-west-2")
)

app.synth()

app.synth()

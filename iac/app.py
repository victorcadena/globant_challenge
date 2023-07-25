#!/usr/bin/env python3
import os

import aws_cdk as cdk

from iac.globant_challenge_stack import GlobantChallengeStack


app = cdk.App()
GlobantChallengeStack(app, "GlobantChallengeStack",
    env=cdk.Environment(account='727474809098', region='us-west-2'),
)

app.synth()

#! /usr/bin/env bash
set -xe

# This is script is designed to be deployed to a lambda function which has been
# configured with all necessary python dependencies via layers or other means.
# see https://docs.aws.amazon.com/lambda/latest/dg/python-package.html for more.

zip -r9 function.zip lambda_function.py idseq

aws lambda update-function-code \
  --profile czbiohub \
  --function-name covid19-to-idseq \
  --zip-file fileb://function.zip
#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { NeptuneEtlStack } from '../lib/neptune-etl-stack';

const app = new cdk.App();

new NeptuneEtlStack(app, 'NeptuneEtlStack', {
  env: {
    account: '926093770964',
    region: 'ap-northeast-1',
  },
  description: 'Neptune ETL Lambda functions - DeepFlow, AWS, CFN pipelines',
});

app.synth();

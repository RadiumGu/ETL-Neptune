import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import { Construct } from 'constructs';
import * as path from 'path';

export class NeptuneEtlStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // =========================================================
    // 引用已有资源（不重建）
    // =========================================================

    // VPC（引用已有）
    const vpc = ec2.Vpc.fromLookup(this, 'ExistingVpc', {
      vpcId: 'vpc-010ab37a3f9f74725',
    });

    // Lambda 安全组（已有 neptune-lambda-sg）
    const lambdaSg = ec2.SecurityGroup.fromSecurityGroupId(
      this,
      'NeptuneLambdaSg',
      'sg-078f24929b25f09cd',
      { allowAllOutbound: true },
    );

    // 已有 NeptuneETLPolicy（neptune-db:connect 等权限）
    const neptuneEtlPolicy = iam.ManagedPolicy.fromManagedPolicyArn(
      this,
      'NeptuneETLPolicy',
      'arn:aws:iam::926093770964:policy/NeptuneETLPolicy',
    );

    // =========================================================
    // Lambda Execution Role（新建，附加已有 Policy + 额外只读权限）
    // =========================================================
    const lambdaRole = new iam.Role(this, 'NeptuneEtlLambdaRole', {
      roleName: 'neptune-etl-lambda-role',
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
        neptuneEtlPolicy,
      ],
    });

    // EC2/EKS/ELBv2/Lambda/StepFn/DynamoDB/CFN 只读权限
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      sid: 'AWSReadOnlyForETL',
      effect: iam.Effect.ALLOW,
      actions: [
        // EC2
        'ec2:DescribeInstances',
        'ec2:DescribeSubnets',
        'ec2:DescribeVpcs',
        'ec2:DescribeSecurityGroups',
        // EKS
        'eks:DescribeCluster',
        'eks:ListClusters',
        'eks:ListNodegroups',
        'eks:DescribeNodegroup',
        // ELBv2 (ALB)
        'elasticloadbalancing:DescribeLoadBalancers',
        'elasticloadbalancing:DescribeTargetGroups',
        'elasticloadbalancing:DescribeTargetHealth',
        'elasticloadbalancing:DescribeListeners',
        'elasticloadbalancing:DescribeRules',
        'elasticloadbalancing:DescribeTags',
        // Lambda
        'lambda:ListFunctions',
        'lambda:GetFunctionConfiguration',
        'lambda:ListTags',
        // Step Functions
        'states:ListStateMachines',
        'states:DescribeStateMachine',
        'states:ListTagsForResource',
        // DynamoDB
        'dynamodb:ListTables',
        'dynamodb:DescribeTable',
        'dynamodb:ListTagsOfResource',
        // CloudFormation
        'cloudformation:GetTemplate',
        'cloudformation:ListStackResources',
        'cloudformation:DescribeStacks',
        'cloudformation:DescribeStackResources',
        // AutoScaling（EKS NodeGroup 解析）
        'autoscaling:DescribeAutoScalingGroups',
        // STS（EKS token）
        'sts:GetCallerIdentity',
        // CloudWatch（EC2/Lambda 性能指标）
        'cloudwatch:GetMetricStatistics',
        'cloudwatch:GetMetricData',
        'cloudwatch:ListMetrics',
      ],
      resources: ['*'],
    }));

    // =========================================================
    // 公共配置
    // =========================================================
    const neptuneEndpoint = 'petsite-neptune.cluster-czbjnsviioad.ap-northeast-1.neptune.amazonaws.com';
    const neptunePort = '8182';
    const awsRegion = 'ap-northeast-1';

    // VPC 私有子网选择（通过已有 neptune-lambda-sg 所在子网，使用 subnetType=PRIVATE_WITH_EGRESS）
    // VPC.fromLookup 会在 synth 时从 context 读取，部署时从实际 VPC 读取私有子网
    const vpcSubnets: ec2.SubnetSelection = {
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    };

    // =========================================================
    // Lambda 1: neptune-etl-from-deepflow（每5分钟）
    // =========================================================
    const deepflowEtlFn = new lambda.Function(this, 'NeptuneEtlFromDeepflow', {
      functionName: 'neptune-etl-from-deepflow',
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'neptune_etl_deepflow.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/etl_deepflow')),
      timeout: cdk.Duration.minutes(4),
      memorySize: 256,
      role: lambdaRole,
      vpc,
      vpcSubnets,
      securityGroups: [lambdaSg],
      environment: {
        NEPTUNE_ENDPOINT: neptuneEndpoint,
        NEPTUNE_PORT: neptunePort,
        REGION: awsRegion,
        CLICKHOUSE_HOST: '11.0.2.30',
        CLICKHOUSE_PORT: '8123',
        CH_HOST: '11.0.2.30',
        CH_PORT: '8123',
        INTERVAL_MIN: '6',
        EKS_CLUSTER_ARN: `arn:aws:eks:${awsRegion}:926093770964:cluster/PetSite`,
      },
      description: 'ETL: ClickHouse L7 flow_log → Neptune Calls/HasMetrics edges + perf metrics (every 5min)',
    });

    new events.Rule(this, 'DeepflowEtlSchedule', {
      ruleName: 'neptune-etl-every-5min',
      description: 'Trigger neptune-etl-from-deepflow every 5 minutes',
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      targets: [new targets.LambdaFunction(deepflowEtlFn)],
    });

    // =========================================================
    // Lambda 2: neptune-etl-from-aws（每15分钟）
    // =========================================================
    const awsEtlFn = new lambda.Function(this, 'NeptuneEtlFromAws', {
      functionName: 'neptune-etl-from-aws',
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'neptune_etl_aws.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/etl_aws')),
      timeout: cdk.Duration.minutes(5),
      memorySize: 256,
      role: lambdaRole,
      vpc,
      vpcSubnets,
      securityGroups: [lambdaSg],
      environment: {
        NEPTUNE_ENDPOINT: neptuneEndpoint,
        NEPTUNE_PORT: neptunePort,
        REGION: awsRegion,
        EKS_CLUSTER_NAME: 'PetSite',
      },
      description: 'ETL: AWS API static topology → Neptune nodes/edges (every 15min)',
    });

    new events.Rule(this, 'AwsEtlSchedule', {
      ruleName: 'neptune-etl-every-15min',
      description: 'Trigger neptune-etl-from-aws every 15 minutes',
      schedule: events.Schedule.rate(cdk.Duration.minutes(15)),
      targets: [new targets.LambdaFunction(awsEtlFn)],
    });

    // =========================================================
    // Lambda 3: neptune-etl-from-cfn（CFN 部署后 + 每日 2:00 CST）
    // =========================================================
    const cfnEtlFn = new lambda.Function(this, 'NeptuneEtlFromCfn', {
      functionName: 'neptune-etl-from-cfn',
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'neptune_etl_cfn.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/etl_cfn')),
      timeout: cdk.Duration.minutes(2),
      memorySize: 256,
      role: lambdaRole,
      vpc,
      vpcSubnets,
      securityGroups: [lambdaSg],
      environment: {
        NEPTUNE_ENDPOINT: neptuneEndpoint,
        NEPTUNE_PORT: neptunePort,
        REGION: awsRegion,
        CFN_STACK_NAMES: 'ServicesEks2,Applications',
      },
      description: 'ETL: CFN template declared deps → Neptune DependsOn edges (on deploy + daily)',
    });

    // 触发方式1: CFN 部署完成后自动触发（ServicesEks2 或 Applications 更新/创建完成）
    new events.Rule(this, 'CfnEtlOnStackUpdate', {
      ruleName: 'neptune-etl-cfn-on-deploy',
      description: 'Trigger neptune-etl-from-cfn on CFN stack update/create complete',
      eventPattern: {
        source: ['aws.cloudformation'],
        detailType: ['CloudFormation Stack Status Change'],
        detail: {
          'status-details': {
            status: ['UPDATE_COMPLETE', 'CREATE_COMPLETE'],
          },
          'stack-id': [
            {
              prefix: 'arn:aws:cloudformation:ap-northeast-1:926093770964:stack/ServicesEks2/',
            },
            {
              prefix: 'arn:aws:cloudformation:ap-northeast-1:926093770964:stack/Applications/',
            },
          ],
        },
      },
      targets: [new targets.LambdaFunction(cfnEtlFn)],
    });

    // 触发方式2: 每日 2:00 AM CST = UTC 18:00 前一天
    new events.Rule(this, 'CfnEtlDailySync', {
      ruleName: 'neptune-etl-cfn-daily',
      description: 'Trigger neptune-etl-from-cfn daily at 2:00 AM CST (UTC 18:00)',
      schedule: events.Schedule.cron({ hour: '18', minute: '0' }),
      targets: [new targets.LambdaFunction(cfnEtlFn)],
    });

    // =========================================================
    // CloudFormation Outputs
    // =========================================================
    new cdk.CfnOutput(this, 'DeepflowEtlFunctionArn', {
      value: deepflowEtlFn.functionArn,
      description: 'neptune-etl-from-deepflow Lambda ARN',
    });
    new cdk.CfnOutput(this, 'AwsEtlFunctionArn', {
      value: awsEtlFn.functionArn,
      description: 'neptune-etl-from-aws Lambda ARN',
    });
    new cdk.CfnOutput(this, 'CfnEtlFunctionArn', {
      value: cfnEtlFn.functionArn,
      description: 'neptune-etl-from-cfn Lambda ARN',
    });
    new cdk.CfnOutput(this, 'LambdaRoleArn', {
      value: lambdaRole.roleArn,
      description: 'Shared Lambda Execution Role ARN',
    });

    // Tags
    cdk.Tags.of(this).add('Project', 'graph-dp');
    cdk.Tags.of(this).add('Phase', 'exploration');
    cdk.Tags.of(this).add('CreatedBy', 'openclaw-agent');
  }
}

"""
neptune_etl_aws.py - AWS 静态拓扑 → Neptune 知识图谱

Lambda 2: neptune-etl-from-aws
触发频率: 每15分钟
功能:
  1. 采集 EC2/EKS/ALB/Lambda/StepFn/DynamoDB/RDS/SQS/SNS/S3/ECR 静态拓扑
  2. 写节点（含 managedBy 属性）
  3. 写静态边（BelongsTo/LocatedIn/RoutesTo/Invokes/AccessesData/Contains）
  4. 查询 CloudWatch 指标：EC2 CPU/网络、Lambda 时长/错误率
  5. 写独立的 Region / AvailabilityZone 节点，并连边

设计原则（防止变更后节点缺失）：
  - 全部用 paginator 全量扫描，不 hardcode 资源列表
  - AZ/Region 节点由每个资源写入时通过 upsert_az_region() 顺手维护，
    不依赖单独步骤，确保任意资源路径都能带出 AZ 节点
  - 新增资源类型只需加一个 collect_*() 函数 + 对应 upsert 块
"""

import os
import json
import time
import logging
import datetime
import boto3
import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ===== 配置 =====
NEPTUNE_ENDPOINT = os.environ.get('NEPTUNE_ENDPOINT',
    'petsite-neptune.cluster-czbjnsviioad.ap-northeast-1.neptune.amazonaws.com')
NEPTUNE_PORT = int(os.environ.get('NEPTUNE_PORT', '8182'))
REGION = os.environ.get('REGION', 'ap-northeast-1')
EKS_CLUSTER_NAME = os.environ.get('EKS_CLUSTER_NAME', 'PetSite')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'prod')

# ===== AWS 服务故障边界（Fault Boundary）=====
#
# 故障边界表示：当该 AWS 服务发生故障时，影响的最小隔离域。
# SRE 查询「哪些服务在同一故障域」时，遍历相同 fault_boundary + scope 的节点即可。
#
#  az:     故障影响单个 AZ（EC2、RDS Instance、Subnet 都绑定物理机架/数据中心）
#  region: 服务是 region 级冗余，单 AZ 故障不影响服务本身
#          但如果整个 region 宕机，所有 region 级服务一起受影响
#  global: 控制面是全局的（如 IAM、Route53），但实际影响范围复杂，保守标注
#
FAULT_BOUNDARY_MAP = {
    # AZ 级
    'EC2Instance':      ('az',     None),      # 物理机绑定单个 AZ
    'RDSInstance':      ('az',     None),      # 数据库实例绑定单个 AZ
    'Subnet':           ('az',     None),      # 子网属于单个 AZ
    'AvailabilityZone': ('az',     None),
    # Region 级
    'LambdaFunction':   ('region', REGION),    # Lambda 跨 AZ 自动分布
    'EKSCluster':       ('region', REGION),    # control plane 是 regional
    'LoadBalancer':     ('region', REGION),    # ALB 跨多 AZ
    'DynamoDBTable':    ('region', REGION),    # regional + multi-AZ replication
    'SQSQueue':         ('region', REGION),    # regional redundant
    'SNSTopic':         ('region', REGION),
    'S3Bucket':         ('region', REGION),    # 桶属于 region（全局 API 另说）
    'ECRRepository':    ('region', REGION),
    'StepFunction':     ('region', REGION),
    'RDSCluster':       ('region', REGION),    # 集群跨 AZ
    'NeptuneCluster':   ('region', REGION),    # 图数据库集群，跨 AZ
    'NeptuneInstance':  ('az',     None),      # 图数据库实例绑定单个 AZ
    'Region':           ('region', REGION),
}

# ===== PetSite 恢复优先级配置 =====
# 基于 PetSite 代码分析（宠物领养商店）
LAMBDA_RECOVERY_PRIORITY = {
    'petstatusupdater':   'Tier1',   # SQS 驱动的宠物状态更新
    'cleanup':            'Tier2',   # 清理 Lambda
    'guardduty':          'Tier2',   # 安全审计 Lambda
    'cloudwatch-custom':  'Tier2',   # CW 自定义 Widget Lambda
    'dynamodb-query':     'Tier2',   # DynamoDB 查询辅助 Lambda
}

EC2_RECOVERY_PRIORITY = {
    # deepflow-server 是关键观测基础设施
    'deepflow':           'Tier1',
}

# ===== 业务能力层配置（BusinessCapability 节点）=====
# 宠物商店三条核心业务链路
BUSINESS_CAPABILITIES = [
    {
        'name':              'PetAdoptionFlow',
        'description':       '宠物领养核心流程：浏览→搜索→支付→状态更新',
        'recovery_priority': 'Tier0',
        'serves_services':   ['petsite', 'petsearch', 'payforadoption'],
        'serves_lambda':     [],
        'depends_on_types':  ['DynamoDBTable', 'SQSQueue', 'SNSTopic', 'StepFunction', 'RDSCluster'],
    },
    {
        'name':              'AdoptionHistoryView',
        'description':       '领养历史记录查询与展示',
        'recovery_priority': 'Tier1',
        'serves_services':   ['petlistadoptions', 'petadoptionshistory'],
        'serves_lambda':     [],
        'depends_on_types':  ['RDSCluster'],
    },
    {
        'name':              'PetInventoryManagement',
        'description':       '宠物库存状态异步管理（领养后更新可用性）',
        'recovery_priority': 'Tier1',
        'serves_services':   [],                    # petstatusupdater 是 Lambda，不是 K8s 微服务
        'serves_lambda':     ['petstatusupdater'],  # 模糊匹配 Lambda 函数名
        'depends_on_types':  ['DynamoDBTable', 'SQSQueue'],
    },
    {
        'name':              'PetFoodService',
        'description':       '宠物食品信息服务',
        'recovery_priority': 'Tier2',
        'serves_services':   ['petfood'],
        'serves_lambda':     [],
        'depends_on_types':  [],
    },
]

# ===== 全局 Session（避免每次 neptune_query 重建 credential）=====
_frozen_creds = None

def _get_creds():
    global _frozen_creds
    if _frozen_creds is None:
        _frozen_creds = boto3.Session(region_name=REGION).get_credentials().get_frozen_credentials()
    return _frozen_creds

# ===== Neptune 工具函数 =====

_http_session = None

def _get_http_session():
    global _http_session
    if _http_session is None:
        import requests as req_lib
        _http_session = req_lib.Session()
    return _http_session

def neptune_query(gremlin: str) -> dict:
    creds = _get_creds()
    url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/gremlin"
    data = json.dumps({"gremlin": gremlin})
    headers = {
        "Content-Type": "application/json",
        "host": f"{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}",
    }
    aws_req = AWSRequest(method="POST", url=url, data=data, headers=headers)
    SigV4Auth(creds, "neptune-db", REGION).add_auth(aws_req)
    r = _get_http_session().post(url, headers=dict(aws_req.headers), data=data, verify=False, timeout=20)
    if r.status_code != 200:
        raise Exception(f"Neptune error {r.status_code}: {r.text[:300]}")
    return r.json()

def safe_str(s) -> str:
    return str(s).replace("'", "\\'").replace('"', '\\"')[:256]

def extract_value(val):
    if isinstance(val, dict) and '@value' in val:
        v = val['@value']
        if isinstance(v, list) and len(v) > 0:
            return extract_value(v[0])
        return v
    return val

_vid_cache = {}  # (label, name) → vertex_id

def get_vertex_id(label: str, name: str):
    key = (label, name)
    if key in _vid_cache:
        return _vid_cache[key]
    n = safe_str(name)
    result = neptune_query(f"g.V().has('{label}', 'name', '{n}').id()")
    ids = result.get('result', {}).get('data', {}).get('@value', [])
    if not ids:
        return None
    vid = ids[0]
    vid = extract_value(vid) if isinstance(vid, dict) else vid
    _vid_cache[key] = vid
    return vid

def upsert_vertex(label: str, name: str, extra_props: dict, managed_by: str = 'manual'):
    """upsert 节点，返回 vertex ID 并写入缓存"""
    n = safe_str(name)
    mb = safe_str(managed_by)
    # 全局注入 environment
    all_props = {'environment': ENVIRONMENT}
    # 按 label 自动注入 fault_boundary 和 region（故障边界信息）
    fb_entry = FAULT_BOUNDARY_MAP.get(label)
    if fb_entry:
        fb_type, fb_region = fb_entry
        all_props['fault_boundary'] = fb_type
        if fb_region:
            all_props['region'] = fb_region
    all_props.update(extra_props)  # extra_props 优先（允许覆盖，如 RDS 实例显式传 az）
    props_create = f"'name': '{n}', 'managedBy': '{mb}', 'source': 'aws-etl'"
    props_match = f"'managedBy': '{mb}', 'last_updated': {int(time.time())}, 'environment': '{ENVIRONMENT}'"
    for k, v in all_props.items():
        ks = safe_str(k)
        vs = safe_str(v)
        props_create += f", '{ks}': '{vs}'"
        props_match += f", '{ks}': '{vs}'"
    gremlin = (
        f"g.mergeV([(T.label): '{label}', 'name': '{n}'])"
        f".option(Merge.onCreate, [(T.label): '{label}', {props_create}])"
        f".option(Merge.onMatch, [{props_match}])"
        f".id()"
    )
    result = neptune_query(gremlin)
    ids = result.get('result', {}).get('data', {}).get('@value', [])
    if ids:
        vid = ids[0]
        vid = extract_value(vid) if isinstance(vid, dict) else vid
        _vid_cache[(label, name)] = vid
        return vid
    return None

def upsert_edge(src_id, dst_id, label: str, props: dict = None):
    """upsert 边（by vertex ID）"""
    if src_id is None or dst_id is None:
        return None
    ts = int(time.time())
    lb = safe_str(label)
    prop_str = f".property('source', 'aws-etl').property('last_updated', {ts})"
    if props:
        for k, v in props.items():
            ks = safe_str(k)
            vs = safe_str(v)
            prop_str += f".property('{ks}', '{vs}')"
    gremlin = (
        f"g.V('{src_id}').as('s').V('{dst_id}')"
        f".coalesce("
        f"  __.inE('{lb}').where(__.outV().hasId('{src_id}')),"
        f"  __.addE('{lb}').from('s')"
        f")"
        + prop_str
    )
    return neptune_query(gremlin)

# ===== AZ / Region 节点维护（核心设计：随资源写入顺手维护）=====

def upsert_az_region(az: str, region: str = None) -> tuple:
    """
    upsert AvailabilityZone 和 Region 节点，并连 Region→AZ Contains 边。
    返回 (az_vid, region_vid)，az 为空时返回 (None, None)。

    设计原则：每个带 AZ 属性的资源（Subnet/EC2/RDS实例）写入时
    都调用此函数，保证 AZ/Region 节点始终存在，无需单独维护步骤。
    """
    if not az:
        return None, None
    r = region or REGION
    # upsert Region
    upsert_vertex('Region', r, {'region_name': r, 'provider': 'aws'}, 'aws')
    region_vid = get_vertex_id('Region', r)
    # upsert AZ
    upsert_vertex('AvailabilityZone', az, {'az_name': az, 'region': r}, 'aws')
    az_vid = get_vertex_id('AvailabilityZone', az)
    # Region → AZ: Contains
    if region_vid and az_vid:
        upsert_edge(region_vid, az_vid, 'Contains', {'source': 'aws-etl'})
    return az_vid, region_vid

def link_to_az(resource_vid, az: str):
    """将资源节点连到 AZ 节点（LocatedIn 边）"""
    if not resource_vid or not az:
        return
    az_vid = get_vertex_id('AvailabilityZone', az)
    if az_vid:
        upsert_edge(resource_vid, az_vid, 'LocatedIn', {'source': 'aws-etl'})

# ===== managedBy 判断逻辑 =====

def resolve_managed_by(tags: list) -> str:
    tag_dict = {t.get('Key', ''): t.get('Value', '') for t in (tags or [])}
    if tag_dict.get('aws:cloudformation:stack-name'):
        return 'cloudformation'
    if tag_dict.get('aws:eks:cluster-name') or tag_dict.get('eks:cluster-name'):
        return 'eks-managed'
    return 'manual'

def resolve_managed_by_dict(tags: dict) -> str:
    if not tags:
        return 'manual'
    if tags.get('aws:cloudformation:stack-name'):
        return 'cloudformation'
    if tags.get('aws:eks:cluster-name') or tags.get('eks:cluster-name'):
        return 'eks-managed'
    return 'manual'

# ===== 采集各资源 =====

def collect_ec2_instances(ec2_client) -> list:
    nodes = []
    paginator = ec2_client.get_paginator('describe_instances')
    for page in paginator.paginate(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
    ):
        for reservation in page['Reservations']:
            for inst in reservation['Instances']:
                instance_id = inst['InstanceId']
                tags = inst.get('Tags', [])
                name = next((t['Value'] for t in tags if t['Key'] == 'Name'), instance_id)
                # EKS 节点标记：有 eks: 前缀的 tag
                is_eks_node = any(t['Key'].startswith('eks:') for t in tags)
                nodes.append({
                    'id': instance_id,
                    'name': name,
                    'instance_type': inst.get('InstanceType', ''),
                    'az': inst.get('Placement', {}).get('AvailabilityZone', ''),
                    'private_ip': inst.get('PrivateIpAddress', ''),
                    'private_dns': inst.get('PrivateDnsName', ''),  # ContainerInsights NodeName（仅EKS）
                    'subnet_id': inst.get('SubnetId', ''),
                    'managed_by': resolve_managed_by(tags),
                    'is_eks_node': is_eks_node,
                })
    logger.info(f"EC2 instances: {len(nodes)}")
    return nodes

def collect_subnets(ec2_client) -> list:
    subnets = []
    paginator = ec2_client.get_paginator('describe_subnets')
    for page in paginator.paginate():
        for sn in page['Subnets']:
            tags = sn.get('Tags', [])
            name = next((t['Value'] for t in tags if t['Key'] == 'Name'), sn['SubnetId'])
            subnets.append({
                'subnet_id': sn['SubnetId'],
                'name': name,
                'cidr': sn.get('CidrBlock', ''),
                'az': sn.get('AvailabilityZone', ''),
                'vpc_id': sn.get('VpcId', ''),
            })
    logger.info(f"Subnets: {len(subnets)}")
    return subnets

def collect_eks_cluster(eks_client) -> dict:
    try:
        resp = eks_client.describe_cluster(name=EKS_CLUSTER_NAME)
        cluster = resp['cluster']
        return {
            'name': cluster['name'],
            'version': cluster.get('version', ''),
            'status': cluster.get('status', ''),
            'endpoint': cluster.get('endpoint', ''),
            'arn': cluster.get('arn', ''),
        }
    except Exception as e:
        logger.error(f"EKS cluster describe failed: {e}")
        return {}

def collect_eks_nodegroup_instances(eks_client, ec2_client) -> list:
    members = []
    try:
        ngs = eks_client.list_nodegroups(clusterName=EKS_CLUSTER_NAME)
        for ng_name in ngs.get('nodegroups', []):
            ng = eks_client.describe_nodegroup(
                clusterName=EKS_CLUSTER_NAME, nodegroupName=ng_name
            )
            asg_names = [
                res['name']
                for res in ng['nodegroup'].get('resources', {}).get('autoScalingGroups', [])
            ]
            if asg_names:
                asg_client = boto3.client('autoscaling', region_name=REGION)
                for asg_name in asg_names:
                    resp = asg_client.describe_auto_scaling_groups(
                        AutoScalingGroupNames=[asg_name]
                    )
                    for asg in resp.get('AutoScalingGroups', []):
                        for inst in asg.get('Instances', []):
                            members.append(inst['InstanceId'])
    except Exception as e:
        logger.error(f"EKS nodegroup instances failed: {e}")
    logger.info(f"EKS member instances: {members}")
    return members

def collect_load_balancers(elb_client) -> list:
    lbs = []
    paginator = elb_client.get_paginator('describe_load_balancers')
    for page in paginator.paginate():
        for lb in page['LoadBalancers']:
            try:
                tags_resp = elb_client.describe_tags(ResourceArns=[lb['LoadBalancerArn']])
                tag_list = tags_resp['TagDescriptions'][0]['Tags'] if tags_resp['TagDescriptions'] else []
                managed_by = resolve_managed_by_dict({t['Key']: t['Value'] for t in tag_list})
            except Exception:
                managed_by = 'manual'
            # ALB 跨多个 AZ，取 AZ 列表
            azs = [az_info['ZoneName'] for az_info in lb.get('AvailabilityZones', [])]
            lbs.append({
                'arn': lb['LoadBalancerArn'],
                'name': lb['LoadBalancerName'],
                'dns': lb.get('DNSName', ''),
                'scheme': lb.get('Scheme', ''),
                'type': lb.get('Type', ''),
                'azs': azs,
                'managed_by': managed_by,
            })
    logger.info(f"Load Balancers: {len(lbs)}")
    return lbs

def collect_alb_target_groups(elb_client) -> list:
    tgs = []
    paginator = elb_client.get_paginator('describe_target_groups')
    for page in paginator.paginate():
        for tg in page['TargetGroups']:
            try:
                health = elb_client.describe_target_health(TargetGroupArn=tg['TargetGroupArn'])
                healthy_targets = [
                    desc['Target']['Id']
                    for desc in health.get('TargetHealthDescriptions', [])
                    if desc.get('TargetHealth', {}).get('State') == 'healthy'
                ]
            except Exception:
                healthy_targets = []
            tgs.append({
                'arn': tg['TargetGroupArn'],
                'name': tg['TargetGroupName'],
                'port': tg.get('Port', 0),
                'protocol': tg.get('Protocol', ''),
                'lb_arns': tg.get('LoadBalancerArns', []),
                'healthy_targets': healthy_targets,
            })
    logger.info(f"Target Groups: {len(tgs)}")
    return tgs

def collect_lambda_functions(lambda_client) -> list:
    fns = []
    paginator = lambda_client.get_paginator('list_functions')
    for page in paginator.paginate():
        for fn in page['Functions']:
            fn_name = fn['FunctionName']
            fn_arn = fn['FunctionArn']
            try:
                tags_resp = lambda_client.list_tags(Resource=fn_arn)
                managed_by = resolve_managed_by_dict(tags_resp.get('Tags', {}))
            except Exception:
                managed_by = 'manual'
            # env_vars 和 MemorySize 直接从 list_functions 响应里读（无需额外 API 调用）
            env_vars = fn.get('Environment', {}).get('Variables', {})
            memory_size = fn.get('MemorySize', -1)
            fns.append({
                'name': fn_name,
                'arn': fn_arn,
                'runtime': fn.get('Runtime', ''),
                'managed_by': managed_by,
                'env_vars': env_vars,
                'memory_size': memory_size,
            })
    logger.info(f"Lambda functions: {len(fns)}")
    return fns

def collect_step_functions(sfn_client) -> list:
    sms = []
    paginator = sfn_client.get_paginator('list_state_machines')
    for page in paginator.paginate():
        for sm in page['stateMachines']:
            sm_arn = sm['stateMachineArn']
            try:
                tags_resp = sfn_client.list_tags_for_resource(resourceArn=sm_arn)
                tag_dict = {t['key']: t['value'] for t in tags_resp.get('tags', [])}
                managed_by = resolve_managed_by_dict(tag_dict)
            except Exception:
                managed_by = 'manual'
            try:
                defn = sfn_client.describe_state_machine(stateMachineArn=sm_arn)
                defn_str = defn.get('definition', '')
            except Exception:
                defn_str = ''
            sms.append({
                'name': sm['name'],
                'arn': sm_arn,
                'managed_by': managed_by,
                'definition': defn_str,
            })
    logger.info(f"Step Functions: {len(sms)}")
    return sms

def collect_dynamodb_tables(ddb_client) -> list:
    tables = []
    paginator = ddb_client.get_paginator('list_tables')
    for page in paginator.paginate():
        for table_name in page['TableNames']:
            try:
                desc = ddb_client.describe_table(TableName=table_name)['Table']
                table_arn = desc.get('TableArn', '')
                try:
                    tags_resp = ddb_client.list_tags_of_resource(ResourceArn=table_arn)
                    tag_dict = {t['Key']: t['Value'] for t in tags_resp.get('Tags', [])}
                    managed_by = resolve_managed_by_dict(tag_dict)
                except Exception:
                    managed_by = 'manual'
                tables.append({
                    'name': table_name,
                    'arn': table_arn,
                    'status': desc.get('TableStatus', ''),
                    'managed_by': managed_by,
                })
            except Exception as e:
                logger.warning(f"DynamoDB table {table_name} describe failed: {e}")
    logger.info(f"DynamoDB tables: {len(tables)}")
    return tables

def collect_rds_clusters(rds_client) -> list:
    """采集 RDS/Aurora 集群"""
    clusters = []
    paginator = rds_client.get_paginator('describe_db_clusters')
    for page in paginator.paginate():
        for cluster in page['DBClusters']:
            tag_dict = {t['Key']: t['Value'] for t in cluster.get('TagList', [])}
            managed_by = resolve_managed_by_dict(tag_dict)
            clusters.append({
                'id': cluster['DBClusterIdentifier'],
                'arn': cluster.get('DBClusterArn', ''),
                'engine': cluster.get('Engine', ''),
                'engine_version': cluster.get('EngineVersion', ''),
                'status': cluster.get('Status', ''),
                'endpoint': cluster.get('Endpoint', ''),
                'reader_endpoint': cluster.get('ReaderEndpoint', ''),
                'azs': cluster.get('AvailabilityZones', []),
                'managed_by': managed_by,
                'members': [m['DBInstanceIdentifier'] for m in cluster.get('DBClusterMembers', [])],
            })
    logger.info(f"RDS clusters: {len(clusters)}")
    return clusters

def collect_rds_instances(rds_client) -> list:
    """采集 RDS/Aurora 实例（含 AZ 信息）"""
    instances = []
    paginator = rds_client.get_paginator('describe_db_instances')
    for page in paginator.paginate():
        for inst in page['DBInstances']:
            tag_dict = {t['Key']: t['Value'] for t in inst.get('TagList', [])}
            managed_by = resolve_managed_by_dict(tag_dict)
            instances.append({
                'id': inst['DBInstanceIdentifier'],
                'arn': inst.get('DBInstanceArn', ''),
                'engine': inst.get('Engine', ''),
                'instance_class': inst.get('DBInstanceClass', ''),
                'az': inst.get('AvailabilityZone', ''),
                'status': inst.get('DBInstanceStatus', ''),
                'cluster_id': inst.get('DBClusterIdentifier', ''),
                'endpoint': inst.get('Endpoint', {}).get('Address', ''),
                'port': str(inst.get('Endpoint', {}).get('Port', '')),
                'is_writer': not inst.get('ReadReplicaSourceDBInstanceIdentifier'),
                'managed_by': managed_by,
            })
    logger.info(f"RDS instances: {len(instances)}")
    return instances

def collect_sqs_queues(sqs_client) -> list:
    """采集 SQS 队列，并通过 RedrivePolicy 精准识别 DLQ→主队列关系"""
    queues = []
    try:
        paginator = sqs_client.get_paginator('list_queues')
        for page in paginator.paginate():
            for url in page.get('QueueUrls', []):
                queue_name = url.split('/')[-1]
                try:
                    attrs = sqs_client.get_queue_attributes(
                        QueueUrl=url,
                        AttributeNames=['QueueArn', 'ApproximateNumberOfMessages',
                                        'VisibilityTimeout', 'MessageRetentionPeriod',
                                        'RedrivePolicy', 'RedriveAllowPolicy']
                    ).get('Attributes', {})
                    try:
                        tags_resp = sqs_client.list_queue_tags(QueueUrl=url)
                        managed_by = resolve_managed_by_dict(tags_resp.get('Tags', {}))
                    except Exception:
                        managed_by = 'manual'
                    is_dlq = 'dlq' in queue_name.lower() or 'dead' in queue_name.lower()
                    # 解析 RedrivePolicy（DLQ 上的属性，指向主队列 ARN）
                    redrive_target_arn = None
                    redrive_raw = attrs.get('RedrivePolicy', '')
                    if redrive_raw:
                        try:
                            import json as _json
                            rp = _json.loads(redrive_raw)
                            redrive_target_arn = rp.get('deadLetterTargetArn')
                        except Exception:
                            pass
                    queues.append({
                        'name': queue_name,
                        'url': url,
                        'arn': attrs.get('QueueArn', ''),
                        'is_dlq': str(is_dlq),
                        'managed_by': managed_by,
                        'redrive_target_arn': redrive_target_arn,  # DLQ→主队列的 ARN
                    })
                except Exception as e:
                    logger.warning(f"SQS queue {queue_name} attrs failed: {e}")
    except Exception as e:
        logger.error(f"SQS list failed: {e}")
    logger.info(f"SQS queues: {len(queues)}")
    return queues

def collect_sns_topics(sns_client) -> list:
    """采集 SNS Topic（region 级服务，无 AZ）"""
    topics = []
    try:
        paginator = sns_client.get_paginator('list_topics')
        for page in paginator.paginate():
            for topic in page.get('Topics', []):
                arn = topic['TopicArn']
                name = arn.split(':')[-1]
                try:
                    tags_resp = sns_client.list_tags_for_resource(ResourceArn=arn)
                    tag_dict = {t['Key']: t['Value'] for t in tags_resp.get('Tags', [])}
                    managed_by = resolve_managed_by_dict(tag_dict)
                except Exception:
                    managed_by = 'manual'
                # 获取订阅数量
                try:
                    attrs = sns_client.get_topic_attributes(TopicArn=arn).get('Attributes', {})
                    subs_confirmed = attrs.get('SubscriptionsConfirmed', '0')
                except Exception:
                    subs_confirmed = '0'
                topics.append({
                    'name': name,
                    'arn': arn,
                    'subscriptions_confirmed': subs_confirmed,
                    'managed_by': managed_by,
                })
    except Exception as e:
        logger.error(f"SNS list failed: {e}")
    logger.info(f"SNS topics: {len(topics)}")
    return topics

def collect_s3_buckets_in_region(s3_client) -> list:
    """采集 S3 Bucket（仅本 region 的，S3 是全局服务但桶有所属 region）"""
    buckets = []
    try:
        resp = s3_client.list_buckets()
        for bucket in resp.get('Buckets', []):
            bname = bucket['Name']
            try:
                loc = s3_client.get_bucket_location(Bucket=bname)
                bucket_region = loc.get('LocationConstraint') or 'us-east-1'
                if bucket_region != REGION:
                    continue  # 只收本 region 的桶
                try:
                    tags_resp = s3_client.get_bucket_tagging(Bucket=bname)
                    tag_dict = {t['Key']: t['Value'] for t in tags_resp.get('TagSet', [])}
                    managed_by = resolve_managed_by_dict(tag_dict)
                except Exception:
                    managed_by = 'manual'
                buckets.append({
                    'name': bname,
                    'region': bucket_region,
                    'managed_by': managed_by,
                })
            except Exception as e:
                logger.debug(f"S3 bucket {bname} skip: {e}")
    except Exception as e:
        logger.error(f"S3 list_buckets failed: {e}")
    logger.info(f"S3 buckets (region={REGION}): {len(buckets)}")
    return buckets

def collect_ecr_repositories(ecr_client) -> list:
    """采集 ECR Repository（region 级服务，无 AZ）"""
    repos = []
    try:
        paginator = ecr_client.get_paginator('describe_repositories')
        for page in paginator.paginate():
            for repo in page.get('repositories', []):
                try:
                    tags_resp = ecr_client.list_tags_for_resource(resourceArn=repo['repositoryArn'])
                    tag_dict = {t['Key']: t['Value'] for t in tags_resp.get('tags', [])}
                    managed_by = resolve_managed_by_dict(tag_dict)
                except Exception:
                    managed_by = 'manual'
                repos.append({
                    'name': repo['repositoryName'],
                    'arn': repo['repositoryArn'],
                    'uri': repo['repositoryUri'],
                    'managed_by': managed_by,
                })
    except Exception as e:
        logger.error(f"ECR list failed: {e}")
    logger.info(f"ECR repositories: {len(repos)}")
    return repos

# ===== CloudWatch 指标查询 =====

def get_cloudwatch_metric(cw_client, namespace, metric_name, dimensions, stat, period_sec, lookback_min):
    try:
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(minutes=lookback_min)
        resp = cw_client.get_metric_statistics(
            Namespace=namespace, MetricName=metric_name,
            Dimensions=dimensions, StartTime=start_time, EndTime=end_time,
            Period=period_sec, Statistics=[stat],
        )
        datapoints = sorted(resp.get('Datapoints', []), key=lambda x: x['Timestamp'])
        return float(datapoints[-1].get(stat, -1)) if datapoints else -1.0
    except Exception as e:
        logger.warning(f"CW {namespace}/{metric_name}: {e}")
        return -1.0

def discover_cwagent_disk_dims(cw_client, instances: list) -> dict:
    """探测非 EKS 实例的 CWAgent disk_used_percent 维度（根分区 path='/'）。
    返回 {instance_id: [dim_list]} 只包含有数据的实例。"""
    result = {}
    for inst in instances:
        iid = inst['id']
        try:
            r = cw_client.list_metrics(
                Namespace='CWAgent',
                MetricName='disk_used_percent',
                Dimensions=[
                    {'Name': 'InstanceId', 'Value': iid},
                    {'Name': 'path', 'Value': '/'},
                ]
            )
            if r.get('Metrics'):
                result[iid] = r['Metrics'][0]['Dimensions']
        except Exception as e:
            logger.warning(f"discover_cwagent_disk_dims {iid}: {e}")
    return result


def fetch_ec2_cloudwatch_metrics_batch(cw_client, instances: list) -> dict:
    """批量查询所有 EC2 实例的 CW 指标。
    - AWS/EC2: CPUUtilization, NetworkIn, NetworkOut（所有实例）
    - ContainerInsights: node_memory_utilization, node_filesystem_utilization
      （EKS 节点，维度 InstanceId + NodeName + ClusterName）
    - CWAgent: mem_used_percent, disk_used_percent
      （非 EKS 节点，安装了 CloudWatch Agent）
    """
    if not instances:
        return {}
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(minutes=15)

    # 预探测非 EKS 实例的 CWAgent 磁盘维度（list_metrics 代价很低）
    non_eks_instances = [i for i in instances if not i.get('is_eks_node')]
    cwagent_disk_dims = discover_cwagent_disk_dims(cw_client, non_eks_instances) if non_eks_instances else {}

    queries = []
    id_map = {}  # metric_id → (instance_id, metric_key)
    for inst in instances:
        iid = inst['id']
        safe_id = iid.replace('-', '_')
        id_map[f"cpu_{safe_id}"] = (iid, 'cpu_util_avg')
        id_map[f"netin_{safe_id}"] = (iid, 'network_in_bytes')
        id_map[f"netout_{safe_id}"] = (iid, 'network_out_bytes')
        dims = [{'Name': 'InstanceId', 'Value': iid}]
        queries += [
            {'Id': f"cpu_{safe_id}", 'MetricStat': {'Metric': {'Namespace': 'AWS/EC2', 'MetricName': 'CPUUtilization', 'Dimensions': dims}, 'Period': 300, 'Stat': 'Average'}},
            {'Id': f"netin_{safe_id}", 'MetricStat': {'Metric': {'Namespace': 'AWS/EC2', 'MetricName': 'NetworkIn', 'Dimensions': dims}, 'Period': 300, 'Stat': 'Average'}},
            {'Id': f"netout_{safe_id}", 'MetricStat': {'Metric': {'Namespace': 'AWS/EC2', 'MetricName': 'NetworkOut', 'Dimensions': dims}, 'Period': 300, 'Stat': 'Average'}},
        ]
        if inst.get('is_eks_node'):
            # EKS 节点：ContainerInsights（需要 NodeName + ClusterName）
            node_name = inst.get('private_dns', '')
            if node_name:
                ci_dims = [
                    {'Name': 'InstanceId', 'Value': iid},
                    {'Name': 'NodeName', 'Value': node_name},
                    {'Name': 'ClusterName', 'Value': EKS_CLUSTER_NAME},
                ]
                id_map[f"mem_{safe_id}"] = (iid, 'memory_util')
                id_map[f"disk_{safe_id}"] = (iid, 'disk_util')
                queries += [
                    {'Id': f"mem_{safe_id}", 'MetricStat': {'Metric': {'Namespace': 'ContainerInsights', 'MetricName': 'node_memory_utilization', 'Dimensions': ci_dims}, 'Period': 300, 'Stat': 'Average'}},
                    {'Id': f"disk_{safe_id}", 'MetricStat': {'Metric': {'Namespace': 'ContainerInsights', 'MetricName': 'node_filesystem_utilization', 'Dimensions': ci_dims}, 'Period': 300, 'Stat': 'Average'}},
                ]
        else:
            # 非 EKS 节点：CWAgent（需要 InstanceType）
            inst_type = inst.get('instance_type', '')
            if inst_type:
                cwa_mem_dims = [
                    {'Name': 'InstanceId', 'Value': iid},
                    {'Name': 'InstanceType', 'Value': inst_type},
                ]
                id_map[f"cwmem_{safe_id}"] = (iid, 'memory_util')
                queries.append({'Id': f"cwmem_{safe_id}", 'MetricStat': {'Metric': {'Namespace': 'CWAgent', 'MetricName': 'mem_used_percent', 'Dimensions': cwa_mem_dims}, 'Period': 300, 'Stat': 'Average'}})
            if iid in cwagent_disk_dims:
                id_map[f"cwdisk_{safe_id}"] = (iid, 'disk_util')
                queries.append({'Id': f"cwdisk_{safe_id}", 'MetricStat': {'Metric': {'Namespace': 'CWAgent', 'MetricName': 'disk_used_percent', 'Dimensions': cwagent_disk_dims[iid]}, 'Period': 300, 'Stat': 'Average'}})
    results = {}  # instance_id → metrics dict
    try:
        for i in range(0, len(queries), 500):
            resp = cw_client.get_metric_data(MetricDataQueries=queries[i:i+500], StartTime=start_time, EndTime=end_time)
            for r in resp.get('MetricDataResults', []):
                mid = r['Id']
                if mid not in id_map:
                    continue
                iid, metric_key = id_map[mid]
                val = r['Values'][0] if r.get('Values') else -1.0
                if iid not in results:
                    results[iid] = {}
                results[iid][metric_key] = val
        # 记录指标来源
        for inst in instances:
            iid = inst['id']
            mem = results.get(iid, {}).get('memory_util')
            if mem is not None and mem > 0:
                src = 'ContainerInsights' if inst.get('is_eks_node') else 'CWAgent'
                logger.info(f"EC2 {src} {inst['name']}: memory={mem:.2f}% disk={results[iid].get('disk_util',-1):.2f}%")
    except Exception as e:
        logger.warning(f"EC2 batch CW failed: {e}")
    # 转换单位，输出标准化 dict
    out = {}
    for inst in instances:
        iid = inst['id']
        raw = results.get(iid, {})
        cpu = raw.get('cpu_util_avg', -1.0)
        net_in = raw.get('network_in_bytes', -1.0)
        net_out = raw.get('network_out_bytes', -1.0)
        mem = raw.get('memory_util', -1.0)
        disk = raw.get('disk_util', -1.0)
        out[iid] = {
            'cpu_util_avg': round(cpu, 2) if cpu >= 0 else -1.0,
            'network_in_mbps': round(net_in / 300 / 1024 / 1024 * 8, 4) if net_in >= 0 else -1.0,
            'network_out_mbps': round(net_out / 300 / 1024 / 1024 * 8, 4) if net_out >= 0 else -1.0,
            'memory_util': round(mem, 2) if mem >= 0 else -1.0,
            'disk_util': round(disk, 2) if disk >= 0 else -1.0,
        }
    return out

def fetch_lambda_cloudwatch_metrics_batch(cw_client, fns: list) -> dict:
    """批量查询所有 Lambda 函数的 CW 指标（p99_duration_ms + concurrent_executions）"""
    if not fns:
        return {}
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(minutes=30)
    queries = []
    id_map = {}
    for fn in fns:
        fname = fn['name']
        safe_name = ''.join(c if c.isalnum() else '_' for c in fname)[:60]
        dims = [{'Name': 'FunctionName', 'Value': fname}]
        # p99 Duration（get_metric_data 里百分位用 Stat='p99'）
        id_map[f"dur_p99_{safe_name}"] = (fname, 'p99_duration_ms')
        id_map[f"inv_{safe_name}"] = (fname, 'invocations')
        id_map[f"err_{safe_name}"] = (fname, 'errors')
        id_map[f"thr_{safe_name}"] = (fname, 'throttles')
        id_map[f"conc_{safe_name}"] = (fname, 'concurrent_executions')
        queries += [
            {'Id': f"dur_p99_{safe_name}", 'MetricStat': {'Metric': {'Namespace': 'AWS/Lambda', 'MetricName': 'Duration', 'Dimensions': dims}, 'Period': 900, 'Stat': 'p99'}},
            {'Id': f"inv_{safe_name}", 'MetricStat': {'Metric': {'Namespace': 'AWS/Lambda', 'MetricName': 'Invocations', 'Dimensions': dims}, 'Period': 900, 'Stat': 'Sum'}},
            {'Id': f"err_{safe_name}", 'MetricStat': {'Metric': {'Namespace': 'AWS/Lambda', 'MetricName': 'Errors', 'Dimensions': dims}, 'Period': 900, 'Stat': 'Sum'}},
            {'Id': f"thr_{safe_name}", 'MetricStat': {'Metric': {'Namespace': 'AWS/Lambda', 'MetricName': 'Throttles', 'Dimensions': dims}, 'Period': 900, 'Stat': 'Sum'}},
            {'Id': f"conc_{safe_name}", 'MetricStat': {'Metric': {'Namespace': 'AWS/Lambda', 'MetricName': 'ConcurrentExecutions', 'Dimensions': dims}, 'Period': 900, 'Stat': 'Average'}},
        ]
    raw_results = {}
    try:
        for i in range(0, len(queries), 500):
            resp = cw_client.get_metric_data(MetricDataQueries=queries[i:i+500], StartTime=start_time, EndTime=end_time)
            for r in resp.get('MetricDataResults', []):
                mid = r['Id']
                if mid not in id_map:
                    continue
                fname, metric_key = id_map[mid]
                val = r['Values'][0] if r.get('Values') else -1.0
                if fname not in raw_results:
                    raw_results[fname] = {}
                raw_results[fname][metric_key] = val
    except Exception as e:
        logger.warning(f"Lambda batch CW failed: {e}")

    # 批量获取 MemorySize（list_functions 里已有）
    fn_memory_map = {}
    for fn in fns:
        fn_memory_map[fn['name']] = fn.get('memory_size', -1)

    out = {}
    for fn in fns:
        fname = fn['name']
        raw = raw_results.get(fname, {})
        p99_dur = raw.get('p99_duration_ms', -1.0)
        inv = raw.get('invocations', -1.0)
        err = raw.get('errors', -1.0)
        thr = raw.get('throttles', -1.0)
        conc = raw.get('concurrent_executions', -1.0)
        out[fname] = {
            'p99_duration_ms': round(p99_dur, 2) if p99_dur >= 0 else -1.0,
            'error_rate': round(err / inv, 4) if inv > 0 and err >= 0 else -1.0,
            'throttle_rate': round(thr / inv, 4) if inv > 0 and thr >= 0 else -1.0,
            'invocations_per_min': round(inv / 30, 2) if inv >= 0 else -1.0,
            'concurrent_executions': round(conc, 2) if conc >= 0 else -1.0,
            'memory_size_mb': fn_memory_map.get(fname, -1),
        }
    return out

def update_ec2_metrics(name, metrics):
    try:
        n = safe_str(name)
        ts = int(time.time())
        # 使用 property(single,...) 确保覆盖而非追加（Neptune 默认 list cardinality）
        props = f".property(single,'cw_updated_at',{ts})"
        for key in ['cpu_util_avg', 'network_in_mbps', 'network_out_mbps', 'memory_util', 'disk_util']:
            props += f".property(single,'{key}',{float(metrics.get(key, -1.0)):.4f})"
        neptune_query(f"g.V().has('EC2Instance', 'name', '{n}'){props}")
        return True
    except Exception as e:
        logger.error(f"update_ec2_metrics {name}: {e}")
        return False

def update_lambda_metrics(name, metrics):
    try:
        n = safe_str(name)
        ts = int(time.time())
        props = f".property(single,'cw_updated_at',{ts})"
        for key in ['p99_duration_ms', 'error_rate', 'throttle_rate', 'invocations_per_min', 'concurrent_executions']:
            props += f".property(single,'{key}',{float(metrics.get(key, -1.0)):.4f})"
        props += f".property(single,'memory_size_mb',{int(metrics.get('memory_size_mb', -1))})"
        neptune_query(f"g.V().has('LambdaFunction', 'name', '{n}'){props}")
        return True
    except Exception as e:
        logger.error(f"update_lambda_metrics {name}: {e}")
        return False

# ===== StepFunction definition 解析 =====

def extract_sfn_lambda_refs(definition_str, lambda_fns):
    if not definition_str:
        return []
    refs = []
    try:
        defn = json.loads(definition_str)
        lambda_arns = set()
        def scan(obj):
            if isinstance(obj, dict):
                resource = obj.get('Resource', '')
                if resource and ('lambda' in resource.lower() or ':function:' in resource):
                    lambda_arns.add(resource)
                for v in obj.values():
                    scan(v)
            elif isinstance(obj, list):
                for item in obj:
                    scan(item)
        scan(defn)
        lambda_name_map = {fn['arn']: fn['name'] for fn in lambda_fns}
        lambda_name_map.update({fn['name']: fn['name'] for fn in lambda_fns})
        for arn in lambda_arns:
            clean_arn = arn.replace(':sync', '').replace(':async', '').rstrip('*').rstrip(':')
            if clean_arn in lambda_name_map:
                refs.append(lambda_name_map[clean_arn])
            else:
                fn_name = clean_arn.split(':')[-1]
                if fn_name in lambda_name_map:
                    refs.append(fn_name)
    except Exception as e:
        logger.warning(f"SFN definition parse failed: {e}")
    return list(set(refs))

# ===== 业务能力层（BusinessCapability 节点）=====

def upsert_business_capabilities() -> dict:
    """
    创建业务能力层节点（BusinessCapability），并连接到对应的技术服务。

    边类型：
      BusinessCapability → Microservice:  Serves（由哪些微服务提供支撑）
      BusinessCapability → LambdaFunction: Serves
      BusinessCapability → 基础设施节点:   DependsOn（依赖哪类基础设施，按 label 查找）

    不依赖 vertex ID 硬编码，全部通过图遍历动态查找已存在的节点。
    """
    stats = {'created': 0, 'edges': 0}
    ts = int(time.time())

    for cap in BUSINESS_CAPABILITIES:
        cap_name = safe_str(cap['name'])
        priority = safe_str(cap['recovery_priority'])
        desc = safe_str(cap['description'])

        # upsert BusinessCapability 节点
        cap_vid = upsert_vertex('BusinessCapability', cap_name, {
            'description': desc,
            'recovery_priority': priority,
            'layer': 'business',
        }, 'manual')
        if not cap_vid:
            logger.warning(f"BusinessCapability upsert failed: {cap_name}")
            continue
        stats['created'] += 1

        # DependsOn → 基础设施节点（Python 端过滤 CDK 内部资源）
        # CDK 内部资源特征：名字含 provider/waiter/framework 或是 ARN 格式
        INTERNAL_KEYWORDS = ('provider', 'waiter', 'framework', 'customresource',
                              'arn:aws:', 'iscompl', 'onevent', 'ontimeout')
        for dep_label in cap.get('depends_on_types', []):
            dl = safe_str(dep_label)
            # 先在 Python 中拉出所有该 label 的节点，过滤后再创建边
            try:
                r_nodes = neptune_query(
                    f"g.V().hasLabel('{dl}').project('id','name').by(id()).by(values('name')).toList()"
                )
                node_list = r_nodes.get('result', {}).get('data', {}).get('@value', [])
                for node_item in node_list:
                    node_vals = {}
                    nv = node_item.get('@value', []) if isinstance(node_item, dict) else []
                    for i in range(0, len(nv), 2):
                        k = nv[i]; v = nv[i+1]
                        if isinstance(v, dict) and '@value' in v:
                            vl = v['@value']; v = vl[0] if vl else ''
                            if isinstance(v, dict) and '@value' in v: v = v['@value']
                        node_vals[k] = str(v)
                    node_id = node_vals.get('id', '')
                    node_name = node_vals.get('name', '').lower()
                    # 跳过 CDK 内部节点
                    if any(kw in node_name for kw in INTERNAL_KEYWORDS):
                        logger.debug(f"DependsOn skip CDK-internal: {node_name}")
                        continue
                    if not node_id:
                        continue
                    try:
                        neptune_query(
                            f"g.V('{cap_vid}').as('cap').V('{node_id}')"
                            f".coalesce("
                            f"  __.inE('DependsOn').where(__.outV().hasId('{cap_vid}')),"
                            f"  __.addE('DependsOn').from('cap')"
                            f").property('source','business-layer').property('last_updated',{ts})"
                        )
                        stats['edges'] += 1
                    except Exception as e:
                        logger.debug(f"DependsOn edge {cap_name}→{node_name}: {e}")
            except Exception as e:
                logger.warning(f"DependsOn query {cap_name}→{dep_label}: {e}")

        # Serves → Microservice（主动 upsert 节点，不依赖 DeepFlow 是否采集过）
        for svc_name in cap.get('serves_services', []):
            sn = safe_str(svc_name)
            # 确保 Microservice 节点存在（从 etl_aws 写入，DeepFlow 可能还没采集到）
            svc_vid = upsert_vertex('Microservice', sn, {
                'namespace': 'default',
                'source': 'business-layer',
                'fault_boundary': 'az',
                'region': REGION,
                'recovery_priority': cap.get('recovery_priority', 'Tier2'),
            }, 'manual')
            if not svc_vid:
                logger.debug(f"Microservice upsert failed: {sn}")
                continue
            try:
                neptune_query(
                    f"g.V('{cap_vid}').as('cap')"
                    f".V('{svc_vid}')"
                    f".coalesce("
                    f"  __.inE('Serves').where(__.outV().hasId('{cap_vid}')),"
                    f"  __.addE('Serves').from('cap')"
                    f").property('source','business-layer').property('last_updated',{ts})"
                )
                stats['edges'] += 1
            except Exception as e:
                logger.debug(f"Serves edge {cap_name}→Microservice({svc_name}) skip: {e}")

        # Serves → LambdaFunction（按名字模糊匹配，因为 Lambda 名含随机后缀）
        for fn_pattern in cap.get('serves_lambda', []):
            fp = safe_str(fn_pattern)
            try:
                neptune_query(
                    f"g.V('{cap_vid}').as('cap')"
                    f".V().hasLabel('LambdaFunction').filter(__.values('name').containing('{fp}'))"
                    f".coalesce("
                    f"  __.inE('Serves').where(__.outV().hasId('{cap_vid}')),"
                    f"  __.addE('Serves').from('cap')"
                    f").property('source','business-layer').property('last_updated',{ts})"
                )
                stats['edges'] += 1
            except Exception as e:
                logger.debug(f"Serves edge {cap_name}→Lambda({fn_pattern}) skip: {e}")

    logger.info(f"BusinessCapability: created={stats['created']}, edges={stats['edges']}")
    return stats


# ===== 主 ETL 逻辑 =====

def run_etl():
    logger.info("=== neptune-etl-from-aws 开始 ===")

    session = boto3.Session(region_name=REGION)
    ec2_client = session.client('ec2', region_name=REGION)
    eks_client = session.client('eks', region_name=REGION)
    elb_client = session.client('elbv2', region_name=REGION)
    lambda_client = session.client('lambda', region_name=REGION)
    sfn_client = session.client('stepfunctions', region_name=REGION)
    ddb_client = session.client('dynamodb', region_name=REGION)
    cw_client = session.client('cloudwatch', region_name=REGION)
    rds_client = session.client('rds', region_name=REGION)
    sqs_client = session.client('sqs', region_name=REGION)
    sns_client = session.client('sns', region_name=REGION)
    s3_client = session.client('s3', region_name=REGION)
    ecr_client = session.client('ecr', region_name=REGION)

    stats = {'vertices': 0, 'edges': 0, 'cw_ec2': 0, 'cw_lambda': 0}

    # =========================================================
    # Step 0: 确保 Region 节点存在（结果写入缓存，后续所有步骤直接用）
    # =========================================================
    upsert_vertex('Region', REGION, {'region_name': REGION, 'provider': 'aws'}, 'aws')
    region_vid = _vid_cache.get(('Region', REGION))
    stats['vertices'] += 1

    # =========================================================
    # Step 1: Subnet（带 AZ）→ 顺手 upsert AZ/Region 节点
    # =========================================================
    subnets = collect_subnets(ec2_client)
    subnet_map = {}  # subnet_id → name
    subnet_vid_map = {}  # subnet_id → vid
    for sn in subnets:
        sn_vid = upsert_vertex('Subnet', sn['name'], {
            'subnet_id': sn['subnet_id'],
            'cidr': sn['cidr'],
            'az': sn['az'],
            'vpc_id': sn['vpc_id'],
        }, 'cloudformation')
        subnet_map[sn['subnet_id']] = sn['name']
        subnet_vid_map[sn['subnet_id']] = sn_vid
        stats['vertices'] += 1
        # upsert AZ/Region + Region→AZ
        az_vid, _ = upsert_az_region(sn['az'])
        # Subnet → AZ: LocatedIn
        if sn_vid and az_vid:
            upsert_edge(sn_vid, az_vid, 'LocatedIn', {'source': 'aws-etl'})
            stats['edges'] += 1

    # =========================================================
    # Step 2: EC2 实例
    # =========================================================
    ec2_instances = collect_ec2_instances(ec2_client)
    inst_vid_map = {}  # instance_id → vid
    for inst in ec2_instances:
        # 按 Name tag 匹配恢复优先级
        priority = 'Tier2'
        for key, p in EC2_RECOVERY_PRIORITY.items():
            if key.lower() in inst['name'].lower():
                priority = p
                break
        inst_vid = upsert_vertex('EC2Instance', inst['name'], {
            'instance_id': inst['id'],
            'instance_type': inst['instance_type'],
            'az': inst['az'],
            'private_ip': inst['private_ip'],
            'recovery_priority': priority,
        }, inst['managed_by'])
        inst_vid_map[inst['id']] = inst_vid
        stats['vertices'] += 1
        # 顺手维护 AZ 节点（upsert AZ/Region，但不直接连 EC2→AZ 边）
        # EC2 只连到 Subnet（Subnet 已经 LocatedIn AZ，可以通过两步遍历推出 AZ，避免冗余）
        upsert_az_region(inst['az'])
        # EC2 → Subnet: LocatedIn
        if inst['subnet_id'] in subnet_vid_map:
            sn_vid = subnet_vid_map[inst['subnet_id']]
            if inst_vid and sn_vid:
                upsert_edge(inst_vid, sn_vid, 'LocatedIn', {'source': 'aws-etl'})
                stats['edges'] += 1

    # EC2 CloudWatch 指标（批量）
    ec2_cw = fetch_ec2_cloudwatch_metrics_batch(cw_client, ec2_instances)
    for inst in ec2_instances:
        try:
            if update_ec2_metrics(inst['name'], ec2_cw.get(inst['id'], {})):
                stats['cw_ec2'] += 1
        except Exception as e:
            logger.warning(f"EC2 CW metrics {inst['name']}: {e}")

    # =========================================================
    # Step 3: EKS 集群
    # =========================================================
    eks_cluster = collect_eks_cluster(eks_client)
    if eks_cluster:
        cluster_vid = upsert_vertex('EKSCluster', eks_cluster['name'], {
            'version': eks_cluster['version'],
            'status': eks_cluster['status'],
            'arn': eks_cluster['arn'],
        }, 'cloudformation')
        stats['vertices'] += 1
        # EC2 Instance → BelongsTo EKSCluster（EC2 属于 EKS，方向：EC2→EKSCluster）
        eks_instance_ids = collect_eks_nodegroup_instances(eks_client, ec2_client)
        for inst_id in eks_instance_ids:
            inst_vid = inst_vid_map.get(inst_id)
            if cluster_vid and inst_vid:
                upsert_edge(inst_vid, cluster_vid, 'BelongsTo', {'source': 'aws-etl'})
                stats['edges'] += 1

    # =========================================================
    # Step 4: ALB（跨 AZ，连到每个 AZ）
    # =========================================================
    load_balancers = collect_load_balancers(elb_client)
    target_groups = collect_alb_target_groups(elb_client)
    lb_arn_map = {lb['arn']: lb['name'] for lb in load_balancers}

    for lb in load_balancers:
        lb_vid = upsert_vertex('LoadBalancer', lb['name'], {
            'arn': lb['arn'],
            'dns': lb['dns'],
            'scheme': lb['scheme'],
            'lb_type': lb['type'],
        }, lb['managed_by'])
        stats['vertices'] += 1
        # ALB → AZ: LocatedIn（ALB 可以跨多个 AZ）
        for az in lb.get('azs', []):
            az_vid, _ = upsert_az_region(az)
            if lb_vid and az_vid:
                upsert_edge(lb_vid, az_vid, 'LocatedIn', {'source': 'aws-etl'})
                stats['edges'] += 1

    lb_vid_map = {lb['name']: _vid_cache.get(('LoadBalancer', lb['name'])) for lb in load_balancers}
    for tg in target_groups:
        for lb_arn in tg.get('lb_arns', []):
            lb_name = lb_arn_map.get(lb_arn)
            if not lb_name:
                continue
            lb_vid = lb_vid_map.get(lb_name)
            if not lb_vid:
                continue
            tg_vid = upsert_vertex('Microservice', tg['name'], {
                'port': str(tg['port']),
                'protocol': tg['protocol'],
                'role': 'target-group',
            }, 'cloudformation')
            stats['vertices'] += 1
            if tg_vid:
                upsert_edge(lb_vid, tg_vid, 'RoutesTo', {'source': 'aws-etl'})
                stats['edges'] += 1

    # =========================================================
    # Step 5: Lambda
    # =========================================================
    lambda_fns = collect_lambda_functions(lambda_client)
    fn_vid_map = {}
    for fn in lambda_fns:
        # 按名称匹配恢复优先级
        priority = 'Tier2'
        for key, p in LAMBDA_RECOVERY_PRIORITY.items():
            if key in fn['name'].lower():
                priority = p
                break
        fn_vid = upsert_vertex('LambdaFunction', fn['name'], {
            'arn': fn['arn'],
            'runtime': fn['runtime'],
            'recovery_priority': priority,
        }, fn['managed_by'])
        fn_vid_map[fn['name']] = fn_vid
        stats['vertices'] += 1

    # Lambda CloudWatch 指标（批量）
    lambda_cw = fetch_lambda_cloudwatch_metrics_batch(cw_client, lambda_fns)
    for fn in lambda_fns:
        try:
            if update_lambda_metrics(fn['name'], lambda_cw.get(fn['name'], {})):
                stats['cw_lambda'] += 1
        except Exception as e:
            logger.warning(f"Lambda CW metrics {fn['name']}: {e}")

    # =========================================================
    # Step 6: DynamoDB
    # =========================================================
    ddb_tables = collect_dynamodb_tables(ddb_client)
    ddb_name_map = {}
    ddb_vid_map = {}
    for tbl in ddb_tables:
        tbl_vid = upsert_vertex('DynamoDBTable', tbl['name'], {
            'arn': tbl['arn'],
            'status': tbl['status'],
        }, tbl['managed_by'])
        stats['vertices'] += 1
        ddb_name_map[tbl['name']] = tbl['arn']
        ddb_vid_map[tbl['name']] = tbl_vid

    # Lambda → DynamoDB: AccessesData（通过 env var）
    for fn in lambda_fns:
        fn_vid = fn_vid_map.get(fn['name'])
        if not fn_vid:
            continue
        for var_name, var_val in fn.get('env_vars', {}).items():
            if isinstance(var_val, str) and var_val in ddb_vid_map:
                tbl_vid = ddb_vid_map[var_val]
                if tbl_vid:
                    upsert_edge(fn_vid, tbl_vid, 'AccessesData', {
                        'source': 'aws-etl', 'evidence': f'env:{var_name}'
                    })
                    stats['edges'] += 1

    # Lambda → Lambda: Invokes（通过 env var 引用另一个 Lambda 的 ARN 或名称）
    # Fix: 之前 env var 匹配没有区分 Lambda ARN，错误创建了 AccessesData 边
    fn_name_set = {fn['name'] for fn in lambda_fns}
    fn_arn_to_name = {fn['arn']: fn['name'] for fn in lambda_fns}
    for fn in lambda_fns:
        fn_vid = fn_vid_map.get(fn['name'])
        if not fn_vid:
            continue
        for var_name, var_val in fn.get('env_vars', {}).items():
            if not isinstance(var_val, str):
                continue
            target_name = None
            if ':function:' in var_val:
                # 是 Lambda ARN，提取函数名或查 ARN map
                clean_arn = var_val.rstrip(':*').rstrip(':')
                target_name = fn_arn_to_name.get(clean_arn) or clean_arn.split(':')[-1]
            elif var_val in fn_name_set and var_val != fn['name']:
                target_name = var_val
            if target_name and target_name in fn_vid_map and target_name != fn['name']:
                upsert_edge(fn_vid, fn_vid_map[target_name], 'Invokes', {
                    'source': 'aws-etl', 'evidence': f'env:{var_name}'
                })
                stats['edges'] += 1

    # =========================================================
    # Step 7: Step Functions
    # =========================================================
    sfn_machines = collect_step_functions(sfn_client)
    sfn_vid_map = {}
    for sm in sfn_machines:
        sm_vid = upsert_vertex('StepFunction', sm['name'], {
            'arn': sm['arn'],
        }, sm['managed_by'])
        sfn_vid_map[sm['name']] = sm_vid
        stats['vertices'] += 1

    for sm in sfn_machines:
        sm_vid = sfn_vid_map.get(sm['name'])
        if not sm_vid:
            continue
        for fn_name in extract_sfn_lambda_refs(sm['definition'], lambda_fns):
            fn_vid = fn_vid_map.get(fn_name)
            if fn_vid:
                upsert_edge(sm_vid, fn_vid, 'Invokes', {'source': 'aws-etl'})
                stats['edges'] += 1

    # =========================================================
    # Step 8: RDS / Aurora + Neptune
    # =========================================================
    rds_clusters = collect_rds_clusters(rds_client)
    rds_instances = collect_rds_instances(rds_client)
    rds_cluster_vid_map = {}

    for cluster in rds_clusters:
        is_neptune = cluster['engine'] == 'neptune'
        vertex_label = 'NeptuneCluster' if is_neptune else 'RDSCluster'
        c_vid = upsert_vertex(vertex_label, cluster['id'], {
            'arn': cluster['arn'],
            'engine': cluster['engine'],
            'engine_version': cluster['engine_version'],
            'status': cluster['status'],
            'endpoint': cluster['endpoint'],
            'reader_endpoint': cluster['reader_endpoint'],
        }, cluster['managed_by'])
        rds_cluster_vid_map[cluster['id']] = c_vid
        stats['vertices'] += 1
        if c_vid and region_vid:
            upsert_edge(c_vid, region_vid, 'LocatedIn', {'source': 'aws-etl'})
            stats['edges'] += 1

    for inst in rds_instances:
        is_neptune = inst['engine'] == 'neptune'
        vertex_label = 'NeptuneInstance' if is_neptune else 'RDSInstance'
        role = 'writer' if inst['is_writer'] else 'reader'
        i_vid = upsert_vertex(vertex_label, inst['id'], {
            'arn': inst['arn'],
            'engine': inst['engine'],
            'instance_class': inst['instance_class'],
            'az': inst['az'],
            'status': inst['status'],
            'endpoint': inst['endpoint'],
            'port': inst['port'],
            'role': role,
        }, inst['managed_by'])
        stats['vertices'] += 1
        az_vid, _ = upsert_az_region(inst['az'])
        if i_vid and az_vid:
            upsert_edge(i_vid, az_vid, 'LocatedIn', {'source': 'aws-etl'})
            stats['edges'] += 1
        if inst['cluster_id'] and inst['cluster_id'] in rds_cluster_vid_map:
            c_vid = rds_cluster_vid_map[inst['cluster_id']]
            if i_vid and c_vid:
                upsert_edge(i_vid, c_vid, 'BelongsTo', {'source': 'aws-etl'})
                stats['edges'] += 1

    # =========================================================
    # Step 9: SQS（NEW）
    # =========================================================
    sqs_queues = collect_sqs_queues(sqs_client)
    sqs_arn_to_name = {}   # arn → name（用于 SNS 订阅查找）
    sqs_vid_map = {}       # name → vid
    for q in sqs_queues:
        q_vid = upsert_vertex('SQSQueue', q['name'], {
            'arn': q['arn'],
            'url': q['url'],
            'is_dlq': q['is_dlq'],
        }, q['managed_by'])
        sqs_vid_map[q['name']] = q_vid
        sqs_arn_to_name[q['arn']] = q['name']
        stats['vertices'] += 1
        if q_vid and region_vid:
            upsert_edge(q_vid, region_vid, 'LocatedIn', {'source': 'aws-etl'})
            stats['edges'] += 1

    # SQS DLQ → 主队列: DeadLetterOf
    # RedrivePolicy 在主队列上，指向 DLQ 的 ARN
    # 边方向：DLQ →DeadLetterOf→ MainQueue（表示"此队列是谁的死信队列"）
    for q in sqs_queues:
        target_arn = q.get('redrive_target_arn')
        if not target_arn:
            continue
        # q 是主队列，target_arn 是 DLQ 的 ARN
        dlq_name = sqs_arn_to_name.get(target_arn)
        if dlq_name and sqs_vid_map.get(dlq_name) and sqs_vid_map.get(q['name']):
            upsert_edge(sqs_vid_map[dlq_name], sqs_vid_map[q['name']], 'DeadLetterOf',
                        {'source': 'aws-etl'})
            stats['edges'] += 1
            logger.info(f"DeadLetterOf: {dlq_name} → {q['name']}")

    # Lambda → SQS: AccessesData（env var）
    for fn in lambda_fns:
        fn_vid = fn_vid_map.get(fn['name'])
        if not fn_vid:
            continue
        for var_name, var_val in fn.get('env_vars', {}).items():
            if not isinstance(var_val, str):
                continue
            q_name = sqs_arn_to_name.get(var_val) if var_val.startswith('arn:') else (var_val if var_val in sqs_vid_map else None)
            if q_name and sqs_vid_map.get(q_name):
                upsert_edge(fn_vid, sqs_vid_map[q_name], 'AccessesData', {
                    'source': 'aws-etl', 'evidence': f'env:{var_name}'
                })
                stats['edges'] += 1

    # =========================================================
    # Step 10: SNS（NEW）
    # =========================================================
    sns_topics = collect_sns_topics(sns_client)
    sns_vid_map = {}   # name → vid
    sns_arn_to_name = {}
    for topic in sns_topics:
        t_vid = upsert_vertex('SNSTopic', topic['name'], {
            'arn': topic['arn'],
            'subscriptions_confirmed': topic['subscriptions_confirmed'],
        }, topic['managed_by'])
        sns_vid_map[topic['name']] = t_vid
        sns_arn_to_name[topic['arn']] = topic['name']
        stats['vertices'] += 1
        if t_vid and region_vid:
            upsert_edge(t_vid, region_vid, 'LocatedIn', {'source': 'aws-etl'})
            stats['edges'] += 1

    # SNS → SQS: PublishesTo
    for topic in sns_topics:
        t_vid = sns_vid_map.get(topic['name'])
        if not t_vid:
            continue
        try:
            paginator = sns_client.get_paginator('list_subscriptions_by_topic')
            for page in paginator.paginate(TopicArn=topic['arn']):
                for sub in page.get('Subscriptions', []):
                    if sub.get('Protocol') == 'sqs':
                        q_name = sqs_arn_to_name.get(sub.get('Endpoint', ''))
                        if q_name and sqs_vid_map.get(q_name):
                            upsert_edge(t_vid, sqs_vid_map[q_name], 'PublishesTo', {'source': 'aws-etl'})
                            stats['edges'] += 1
        except Exception as e:
            logger.warning(f"SNS subscriptions {topic['name']}: {e}")

    # =========================================================
    # Step 11: S3（NEW，仅本 region）
    # =========================================================
    s3_buckets = collect_s3_buckets_in_region(s3_client)
    s3_vid_map = {}
    for bucket in s3_buckets:
        b_vid = upsert_vertex('S3Bucket', bucket['name'], {
            'region': bucket['region'],
        }, bucket['managed_by'])
        s3_vid_map[bucket['name']] = b_vid
        stats['vertices'] += 1
        if b_vid and region_vid:
            upsert_edge(b_vid, region_vid, 'LocatedIn', {'source': 'aws-etl'})
            stats['edges'] += 1

    # Lambda → S3: AccessesData（env var）
    for fn in lambda_fns:
        fn_vid = fn_vid_map.get(fn['name'])
        if not fn_vid:
            continue
        for var_name, var_val in fn.get('env_vars', {}).items():
            if isinstance(var_val, str) and var_val in s3_vid_map:
                upsert_edge(fn_vid, s3_vid_map[var_val], 'AccessesData', {
                    'source': 'aws-etl', 'evidence': f'env:{var_name}'
                })
                stats['edges'] += 1

    # =========================================================
    # Step 12: ECR（NEW）
    # =========================================================
    ecr_repos = collect_ecr_repositories(ecr_client)
    for repo in ecr_repos:
        r_vid = upsert_vertex('ECRRepository', repo['name'], {
            'arn': repo['arn'],
            'uri': repo['uri'],
        }, repo['managed_by'])
        stats['vertices'] += 1
        if r_vid and region_vid:
            upsert_edge(r_vid, region_vid, 'LocatedIn', {'source': 'aws-etl'})
            stats['edges'] += 1

    # =========================================================
    # Step 13: 业务能力层节点（BusinessCapability）
    # =========================================================
    biz_stats = upsert_business_capabilities()
    stats['vertices'] += biz_stats.get('created', 0)
    stats['edges'] += biz_stats.get('edges', 0)

    # =========================================================
    # Step 14: ETL Lambda → NeptuneCluster WritesTo 关联
    #   这 3 个 Lambda 是图数据库的数据管道，静态配置 WritesTo 边
    #   managed_by='etl' 区分它们与 PetSite 业务 Lambda
    # =========================================================
    ETL_LAMBDA_NAMES = [
        'neptune-etl-from-aws',
        'neptune-etl-from-deepflow',
        'neptune-etl-from-cfn',
    ]
    NEPTUNE_CLUSTER_NAME = 'petsite-neptune'
    neptune_cluster_vid = neptune_query(
        f"g.V().has('NeptuneCluster','name','{NEPTUNE_CLUSTER_NAME}').id()"
    )['result']['data']['@value']
    def _extract_vid(raw):
        if not raw: return None
        v = raw[0]
        if isinstance(v, dict): return v.get('@value', str(v))
        return str(v)
    neptune_cluster_vid = _extract_vid(neptune_cluster_vid)
    for etl_fn in ETL_LAMBDA_NAMES:
        etl_vid_resp = neptune_query(
            f"g.V().has('LambdaFunction','name','{etl_fn}').id()"
        )['result']['data']['@value']
        etl_vid = _extract_vid(etl_vid_resp)
        if etl_vid:
            # 标记为 etl managed
            neptune_query(f"g.V('{etl_vid}').property(single,'managed_by','etl')")
            if neptune_cluster_vid:
                upsert_edge(etl_vid, neptune_cluster_vid, 'WritesTo', {'source': 'aws-etl'})
                stats['edges'] += 1

    logger.info(f"=== neptune-etl-from-aws 完成: {stats} ===")
    return stats


def handler(event, context):
    """Lambda 入口"""
    try:
        result = run_etl()
        return {"statusCode": 200, "body": result}
    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        raise

"""
neptune_etl_deepflow.py - ClickHouse L7 flow_log → Neptune 服务调用图

Lambda: neptune-etl-from-deepflow
触发频率: 每5分钟
优化: 批量写入 + 合并查询，~700次请求降至~20-30次
"""

import os
import json
import time
import logging
import base64
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
CH_HOST = os.environ.get('CLICKHOUSE_HOST', os.environ.get('CH_HOST', '11.0.2.30'))
CH_PORT = int(os.environ.get('CLICKHOUSE_PORT', os.environ.get('CH_PORT', '8123')))
INTERVAL_MIN = int(os.environ.get('INTERVAL_MIN', '6'))
EKS_CLUSTER_ARN = os.environ.get('EKS_CLUSTER_ARN',
    'arn:aws:eks:ap-northeast-1:926093770964:cluster/PetSite')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '20'))
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'prod')

# ===== PetSite 服务恢复优先级（基于代码分析）=====
# Tier0: 核心收益路径，宕机直接影响用户无法领养宠物
# Tier1: 重要功能，宕机导致体验降级但主流程仍可运行
# Tier2: 辅助功能
MICROSERVICE_RECOVERY_PRIORITY = {
    # Tier0 - 核心领养流程
    'petsite':           'Tier0',
    'petsearch':         'Tier0',
    'payforadoption':    'Tier0',
    # Tier1 - 重要但非阻塞
    'petlistadoptions':  'Tier1',
    'petadoptionshistory': 'Tier1',
    'petstatusupdater':  'Tier1',
    'petfood':           'Tier1',
    # Tier2 - 辅助
    'trafficgenerator':  'Tier2',
}

# K8s pod app label → Neptune 微服务名映射
# K8s 部署使用 kebab-case（pay-for-adoption），但 Neptune/DeepFlow 用 PetSite 代码约定名
K8S_SERVICE_ALIAS = {
    'pay-for-adoption': 'payforadoption',
    'list-adoptions':   'petlistadoptions',
    'search-service':   'petsearch',
    'pethistory':       'pethistory',
    'traffic-generator': 'trafficgenerator',
    # petsite pod label 已是 'petsite'，无需 alias
}


# ===== Neptune 请求（复用 session）=====

_http_session = None

def _get_http_session():
    global _http_session
    if _http_session is None:
        import requests
        _http_session = requests.Session()
    return _http_session

_frozen_creds = None
def _get_creds():
    global _frozen_creds
    if _frozen_creds is None:
        _frozen_creds = boto3.Session(region_name=REGION).get_credentials().get_frozen_credentials()
    return _frozen_creds

def get_aws_session():
    return boto3.Session(region_name=REGION)

def neptune_query(gremlin: str) -> dict:
    creds = _get_creds()
    url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/gremlin"
    data = json.dumps({"gremlin": gremlin})
    headers = {
        "Content-Type": "application/json",
        "host": f"{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}",
    }
    req = AWSRequest(method="POST", url=url, data=data, headers=headers)
    SigV4Auth(creds, "neptune-db", REGION).add_auth(req)
    r = _get_http_session().post(url, headers=dict(req.headers), data=data, verify=False, timeout=30)
    if r.status_code != 200:
        raise Exception(f"Neptune error {r.status_code}: {r.text[:300]}")
    return r.json()

def extract_value(val):
    if isinstance(val, dict) and '@value' in val:
        v = val['@value']
        if isinstance(v, list) and len(v) > 0:
            return extract_value(v[0])
        return v
    return val

# ===== ClickHouse 查询 =====

def ch_query(sql: str) -> list:
    import requests
    try:
        r = requests.post(f"http://{CH_HOST}:{CH_PORT}/", data=sql, timeout=30)
        if r.status_code != 200:
            raise Exception(f"ClickHouse error {r.status_code}: {r.text[:200]}")
        return [line.split('\t') for line in r.text.strip().split('\n') if line.strip()]
    except Exception as e:
        logger.error(f"ClickHouse query failed: {e}")
        return []

def ch_query_json(sql: str) -> dict:
    import requests
    try:
        r = requests.post(f"http://{CH_HOST}:{CH_PORT}/", data=sql + ' FORMAT JSON', timeout=30)
        if r.status_code != 200:
            raise Exception(f"ClickHouse error {r.status_code}: {r.text[:200]}")
        return r.json()
    except Exception as e:
        logger.error(f"ClickHouse JSON query failed: {e}")
        return {}

# ===== L7 性能指标 =====

def fetch_l7_metrics() -> dict:
    # 注意：DeepFlow l7_flow_log 没有 server_ip 字段，服务端 IP 是 ip4_1
    # response_status 是枚举(0=正常,1=异常,2=不存在,3=服务端异常,4=客户端异常)，不是 HTTP 状态码
    sql = """
SELECT IPv4NumToString(ip4_1) AS server_ip,
    quantile(0.5)(response_duration)/1000 AS p50_latency_ms,
    quantile(0.99)(response_duration)/1000 AS p99_latency_ms,
    count()/300 AS rps,
    countIf(response_status >= 1) / count() AS error_rate
FROM flow_log.l7_flow_log
WHERE toUnixTimestamp(time) > toUnixTimestamp(now()) - 300
    AND response_duration > 0 AND ip4_1 != 0
GROUP BY server_ip
"""
    result = {}
    try:
        data = ch_query_json(sql)
        for row in data.get('data', []):
            ip = row.get('server_ip', '')
            if ip:
                result[ip] = {
                    'p50_latency_ms': float(row.get('p50_latency_ms', -1) or -1),
                    'p99_latency_ms': float(row.get('p99_latency_ms', -1) or -1),
                    'rps': float(row.get('rps', -1) or -1),
                    'error_rate': float(row.get('error_rate', -1) or -1),
                }
        logger.info(f"L7 metrics fetched for {len(result)} IPs")
    except Exception as e:
        logger.error(f"fetch_l7_metrics failed: {e}")
    return result


def fetch_active_connections() -> dict:
    """从 network_map.1m 查 TCP 连接数（用 syn_count 代理活跃连接）"""
    sql = """
SELECT IPv4NumToString(ip4_1) AS server_ip,
    sum(syn_count) AS active_connections
FROM `flow_metrics`.`network_map.1m`
WHERE toUnixTimestamp(time) > toUnixTimestamp(now()) - 300
    AND ip4_1 != 0 AND protocol = 6
GROUP BY server_ip
"""
    result = {}
    try:
        data = ch_query_json(sql)
        for row in data.get('data', []):
            ip = row.get('server_ip', '')
            if ip:
                result[ip] = int(float(row.get('active_connections', 0) or 0))
        logger.info(f"active_connections fetched for {len(result)} IPs")
    except Exception as e:
        logger.error(f"fetch_active_connections failed: {e}")
    return result

# ===== EKS Token & IP 映射 =====

def get_eks_token(cluster_name: str) -> str:
    try:
        import botocore
        from botocore.signers import RequestSigner
        session = get_aws_session()
        signer = RequestSigner(
            botocore.model.ServiceId('sts'), REGION, 'sts', 'v4',
            session.get_credentials(), session.events,
        )
        params = {
            'method': 'GET',
            'url': f'https://sts.{REGION}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15',
            'body': {}, 'headers': {'x-k8s-aws-id': cluster_name}, 'context': {},
        }
        signed = signer.generate_presigned_url(params, region_name=REGION, expires_in=60, operation_name='')
        return 'k8s-aws-v1.' + base64.urlsafe_b64encode(signed.encode()).decode().rstrip('=')
    except Exception as e:
        logger.warning(f"Failed to get EKS token: {e}")
        return ''

def _get_eks_k8s_session() -> tuple:
    """建立 EKS/K8s 连接，返回 (k8s_endpoint, token, ca_file)，失败返回 (None, None, None)"""
    try:
        session = get_aws_session()
        eks_client = session.client('eks', region_name=REGION)
        cluster_name = EKS_CLUSTER_ARN.split('/')[-1]
        cluster_info = eks_client.describe_cluster(name=cluster_name)
        k8s_endpoint = cluster_info['cluster']['endpoint']
        ca_data = cluster_info['cluster']['certificateAuthority']['data']
        token = get_eks_token(cluster_name)
        if not token:
            return None, None, None
        import tempfile
        ca_bytes = base64.b64decode(ca_data)
        with tempfile.NamedTemporaryFile(suffix='.crt', delete=False) as f:
            f.write(ca_bytes)
            ca_file = f.name
        return k8s_endpoint, token, ca_file
    except Exception as e:
        logger.warning(f"EKS session setup failed: {e}")
        return None, None, None

def build_ip_service_map() -> dict:
    ip_map = {}
    import requests
    k8s_endpoint, token, ca_file = _get_eks_k8s_session()
    if not k8s_endpoint:
        return ip_map
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    try:
        # Step 1: node_name → AZ 映射
        node_az_map = {}
        try:
            nodes_resp = requests.get(
                f"{k8s_endpoint}/api/v1/nodes",
                headers=headers, verify=ca_file, timeout=15
            )
            if nodes_resp.status_code == 200:
                for node in nodes_resp.json().get('items', []):
                    node_name = node.get('metadata', {}).get('name', '')
                    labels = node.get('metadata', {}).get('labels', {})
                    az = (
                        labels.get('topology.kubernetes.io/zone') or
                        labels.get('failure-domain.beta.kubernetes.io/zone', '')
                    )
                    if node_name and az:
                        node_az_map[node_name] = az
            logger.info(f"K8s node→AZ map: {len(node_az_map)} nodes, AZs: {set(node_az_map.values())}")
        except Exception as e:
            logger.warning(f"K8s nodes AZ map failed: {e}")

        # Step 2: 查 pods，带 nodeName 解析 AZ
        resp = requests.get(
            f"{k8s_endpoint}/api/v1/pods",
            headers=headers, verify=ca_file, timeout=15
        )
        if resp.status_code == 200:
            for item in resp.json().get('items', []):
                pod_ip = item.get('status', {}).get('podIP', '')
                ns = item.get('metadata', {}).get('namespace', 'default')
                labels = item.get('metadata', {}).get('labels', {})
                pod_name = item.get('metadata', {}).get('name', '')
                node_name = item.get('spec', {}).get('nodeName', '')
                app_label = (
                    labels.get('app') or
                    labels.get('app.kubernetes.io/name') or
                    labels.get('name') or ''
                )
                svc_name = app_label if app_label else pod_name.rsplit('-', 2)[0]
                # 应用 K8s pod label → Neptune 微服务名别名映射
                svc_name = K8S_SERVICE_ALIAS.get(svc_name, svc_name)
                az = node_az_map.get(node_name, '')
                if pod_ip and svc_name:
                    ip_map[pod_ip] = {
                        'name': svc_name,
                        'namespace': ns,
                        'type': 'Microservice',
                        'az': az,
                    }
        logger.info(f"IP→Service map: {len(ip_map)} entries (with AZ info)")
    finally:
        try:
            os.unlink(ca_file)
        except Exception:
            pass
    return ip_map

def fetch_resource_limits(ip_map: dict) -> dict:
    """从 K8s Deployments API 获取 resource limits，返回 {svc_name: {cpu, memory}}"""
    resource_limits = {}
    import requests
    k8s_endpoint, token, ca_file = _get_eks_k8s_session()
    if not k8s_endpoint:
        return resource_limits
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    try:
        resp = requests.get(f"{k8s_endpoint}/apis/apps/v1/deployments",
                            headers=headers, verify=ca_file, timeout=15)
        if resp.status_code == 200:
            for item in resp.json().get('items', []):
                labels = item.get('metadata', {}).get('labels', {})
                svc_name = (labels.get('app') or labels.get('app.kubernetes.io/name')
                            or labels.get('name') or item.get('metadata', {}).get('name', ''))
                containers = (item.get('spec', {}).get('template', {})
                              .get('spec', {}).get('containers', []))
                if containers and svc_name:
                    limits = containers[0].get('resources', {}).get('limits', {})
                    resource_limits[svc_name] = {
                        'cpu': limits.get('cpu', ''),
                        'memory': limits.get('memory', ''),
                    }
        logger.info(f"resource_limits fetched for {len(resource_limits)} services")
    except Exception as e:
        logger.warning(f"fetch_resource_limits failed: {e}")
    finally:
        try:
            if ca_file:
                os.unlink(ca_file)
        except Exception:
            pass
    return resource_limits


def fetch_replica_counts(ip_map: dict) -> dict:
    replica_map = {}
    import requests
    k8s_endpoint, token, ca_file = _get_eks_k8s_session()
    if not k8s_endpoint:
        return replica_map
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    try:
        resp = requests.get(f"{k8s_endpoint}/apis/apps/v1/deployments", headers=headers, verify=ca_file, timeout=15)
        if resp.status_code == 200:
            for item in resp.json().get('items', []):
                labels = item.get('metadata', {}).get('labels', {})
                svc_name = (labels.get('app') or labels.get('app.kubernetes.io/name')
                            or labels.get('name') or item.get('metadata', {}).get('name', ''))
                ready = item.get('status', {}).get('readyReplicas', 0) or 0
                if svc_name:
                    replica_map[svc_name] = ready
    finally:
        try:
            os.unlink(ca_file)
        except Exception:
            pass
    return replica_map

# ===== 安全字符串 =====

def safe_str(s: str) -> str:
    return str(s).replace("'", "\\'").replace('"', '\\"')[:128]

# ===== 批量 Neptune 操作 =====

def batch_upsert_nodes(services: list):
    """批量 upsert Microservice 节点，每批 BATCH_SIZE 个"""
    for i in range(0, len(services), BATCH_SIZE):
        batch = services[i:i + BATCH_SIZE]
        # 用链式 mergeV 一次请求写多个节点
        parts = []
        for svc in batch:
            n, ns, ip = safe_str(svc['name']), safe_str(svc['namespace']), safe_str(svc['ip'])
            az = safe_str(svc.get('az', ''))
            priority = MICROSERVICE_RECOVERY_PRIORITY.get(n, 'Tier2')
            create_props = (
                f"'namespace': '{ns}', 'ip': '{ip}', 'source': 'deepflow', "
                f"'environment': '{ENVIRONMENT}', 'recovery_priority': '{priority}', "
                f"'fault_boundary': 'az', 'region': '{REGION}'"
            )
            match_props = (
                f"'ip': '{ip}', 'namespace': '{ns}', "
                f"'environment': '{ENVIRONMENT}', 'recovery_priority': '{priority}', "
                f"'fault_boundary': 'az'"
            )
            if az:
                create_props += f", 'az': '{az}'"
                # NOTE: az 不放入 match_props（list cardinality），改为后置 property(single) 更新
            parts.append(
                f"mergeV([(T.label): 'Microservice', 'name': '{n}'])"
                f".option(Merge.onCreate, [(T.label): 'Microservice', 'name': '{n}', "
                f"{create_props}])"
                f".option(Merge.onMatch, [{match_props}])"
            )
        gremlin = "g." + ".".join(parts)
        try:
            neptune_query(gremlin)
        except Exception as e:
            logger.error(f"batch_upsert_nodes failed (batch {i}): {e}")
            # 回退到逐条写入
            for svc in batch:
                try:
                    n, ns, ip = safe_str(svc['name']), safe_str(svc['namespace']), safe_str(svc['ip'])
                    priority = MICROSERVICE_RECOVERY_PRIORITY.get(n, 'Tier2')
                    neptune_query(
                        f"g.mergeV([(T.label): 'Microservice', 'name': '{n}'])"
                        f".option(Merge.onCreate, [(T.label): 'Microservice', 'name': '{n}', "
                        f"'namespace': '{ns}', 'ip': '{ip}', 'source': 'deepflow', "
                        f"'environment': '{ENVIRONMENT}', 'recovery_priority': '{priority}', "
                        f"'fault_boundary': 'az', 'region': '{REGION}'])"
                        f".option(Merge.onMatch, ['ip': '{ip}', 'namespace': '{ns}', "
                        f"'environment': '{ENVIRONMENT}', 'recovery_priority': '{priority}', "
                        f"'fault_boundary': 'az'])"
                    )
                except Exception as e2:
                    logger.error(f"single upsert node {svc['name']} failed: {e2}")

def batch_upsert_edges(edges: list):
    """批量 upsert Calls 边 — 直接用属性匹配，不再查 vertex ID"""
    ts = int(time.time())
    for e in edges:
        src, dst = safe_str(e['src']), safe_str(e['dst'])
        proto = safe_str(e['protocol'])
        calls = e['calls']
        errors = e['errors']
        error_rate = round(errors / calls, 4) if calls > 0 else 0.0
        p99 = float(e.get('p99_latency_ms', -1))
        gremlin = (
            f"g.V().has('Microservice','name','{src}').as('s')"
            f".V().has('Microservice','name','{dst}')"
            f".coalesce("
            f"  __.inE('Calls').where(__.outV().has('name','{src}')),"
            f"  __.addE('Calls').from('s')"
            f")"
            f".property('protocol','{proto}')"
            f".property('port',{e['port']})"
            f".property('calls',{calls})"
            f".property('avg_latency_us',{e['avg_latency']:.0f})"
            f".property('p99_latency_ms',{p99:.4f})"
            f".property('error_count',{errors})"
            f".property('error_rate',{error_rate})"
            f".property('active',true)"
            f".property('last_seen',{ts})"
        )
        try:
            neptune_query(gremlin)
        except Exception as ex:
            logger.error(f"upsert edge {e['src']}->{e['dst']} failed: {ex}")

def batch_fetch_dependency_and_update(service_names: list, l7_metrics: dict,
                                       ip_map_by_name: dict, replica_counts: dict,
                                       resource_limits: dict, active_connections_map: dict):
    """合并 dependency 查询：每个服务 1 次请求（原来 4 次）+ 1 次 metrics 更新"""
    ts = int(time.time())
    for name in service_names:
        n = safe_str(name)
        # 1 次 project 查询替代原来 4 次独立查询
        gremlin = (
            f"g.V().has('Microservice','name','{n}')"
            f".project('up','down','db','cache')"
            f".by(out('Calls').count())"
            f".by(in('Calls').count())"
            f".by(out('Calls').hasLabel('RDSCluster','DynamoDBTable').count())"
            f".by(out('Calls').hasLabel('ElastiCache','Redis','CacheCluster').count())"
        )
        dep = {'upstream_count': 0, 'downstream_count': 0,
               'is_entry_point': False, 'has_db_dependency': False, 'has_cache_dependency': False}
        try:
            res = neptune_query(gremlin)
            data = res.get('result', {}).get('data', {}).get('@value', [])
            if data:
                item = data[0] if isinstance(data, list) else data
                if isinstance(item, dict) and '@value' in item:
                    vals = item['@value']
                    # GraphSON map format: [key, value, key, value, ...]
                    if isinstance(vals, list) and len(vals) >= 8:
                        dep['upstream_count'] = int(extract_value(vals[1]))
                        dep['downstream_count'] = int(extract_value(vals[3]))
                        dep['has_db_dependency'] = int(extract_value(vals[5])) > 0
                        dep['has_cache_dependency'] = int(extract_value(vals[7])) > 0
                        dep['is_entry_point'] = dep['downstream_count'] == 0
        except Exception as e:
            logger.error(f"dependency query {name}: {e}")

        # 构建 metrics
        svc_ip = ip_map_by_name.get(name, '')
        l7 = l7_metrics.get(svc_ip, {})
        priority = MICROSERVICE_RECOVERY_PRIORITY.get(name, 'Tier2')
        props = [f".property(single,'metrics_updated_at',{ts})",
                 f".property(single,'environment','{ENVIRONMENT}')",
                 f".property(single,'recovery_priority','{priority}')",
                 f".property(single,'fault_boundary','az')",
                 f".property(single,'region','{REGION}')"]
        for key in ['p50_latency_ms', 'p99_latency_ms', 'rps', 'error_rate']:
            v = l7.get(key, -1)
            props.append(f".property(single,'{key}',{float(v):.4f})")
        # active_connections（来自 network_map L4 数据）
        ac = active_connections_map.get(svc_ip, -1)
        props.append(f".property(single,'active_connections',{int(ac)})")
        props.append(f".property(single,'replica_count',{int(replica_counts.get(name, -1))})")
        # K8s resource limits
        limits = resource_limits.get(name, {})
        if limits.get('cpu'):
            cpu_s = safe_str(limits['cpu'])
            props.append(f".property(single,'resource_limit_cpu','{cpu_s}')")
        if limits.get('memory'):
            mem_s = safe_str(limits['memory'])
            props.append(f".property(single,'resource_limit_memory','{mem_s}')")
        for key in ['upstream_count', 'downstream_count']:
            props.append(f".property(single,'{key}',{int(dep.get(key, -1))})")
        for key in ['is_entry_point', 'has_db_dependency', 'has_cache_dependency']:
            props.append(f".property(single,'{key}',{'true' if dep.get(key) else 'false'})")

        try:
            neptune_query(f"g.V().has('Microservice','name','{n}'){''.join(props)}")
        except Exception as e:
            logger.error(f"update metrics {name}: {e}")

# ===== 主处理逻辑 =====

def run_etl():
    logger.info("=== neptune-etl-from-deepflow 开始 (optimized) ===")
    t0 = time.time()

    # 1. 构建 IP→服务名映射
    ip_map = build_ip_service_map()
    if not ip_map:
        logger.warning("Empty IP map, will use IP-based names as fallback")

    # 2. 查询 ClickHouse - L7 流量关系
    # 注意：去掉 type=0 过滤（DeepFlow 中 type=2 是响应日志，占绝大多数有效数据）
    # server_ip 用 ip4_1，error 用 response_status=3（服务端异常）
    sql = f"""
SELECT IPv4NumToString(ip4_0) as src_ip, IPv4NumToString(ip4_1) as dst_ip,
    server_port, l7_protocol_str, count() as calls,
    avg(response_duration) as avg_latency_us,
    countIf(response_status = 3) as error_count,
    quantile(0.99)(response_duration)/1000 AS p99_latency_ms
FROM flow_log.l7_flow_log
WHERE ip4_0 != 0 AND ip4_1 != 0
    AND toUnixTimestamp(time) > toUnixTimestamp(now()) - {INTERVAL_MIN}*60
    AND ip4_0 != ip4_1
GROUP BY src_ip, dst_ip, server_port, l7_protocol_str
HAVING calls >= 2
ORDER BY calls DESC LIMIT 100 FORMAT TSV
"""
    rows = ch_query(sql)
    logger.info(f"发现 {len(rows)} 条调用关系")

    # 3. L7 性能指标
    l7_metrics = fetch_l7_metrics()

    # 3b. 活跃连接数（from network_map L4）
    active_connections_map = fetch_active_connections()

    if not rows:
        logger.info("No flow data, skipping")
        return {"nodes": 0, "edges": 0, "duration_ms": int((time.time() - t0) * 1000)}

    # 4. 收集所有需要 upsert 的节点和边
    nodes_set = {}  # name -> {name, namespace, ip}
    edges_list = []
    ip_map_by_name = {}  # name -> ip

    for row in rows:
        if len(row) < 7:
            continue
        src_ip, dst_ip, port, protocol, calls_s, avg_lat_s, errors_s = row[:7]
        p99_ms = float(row[7]) if len(row) > 7 else -1.0
        try:
            calls, avg_lat, errors = int(calls_s), float(avg_lat_s), int(errors_s)
        except ValueError:
            continue

        src_info = ip_map.get(src_ip, {'name': f"svc-{src_ip.replace('.', '-')}", 'namespace': 'unknown'})
        dst_info = ip_map.get(dst_ip, {'name': f"svc-{dst_ip.replace('.', '-')}", 'namespace': 'unknown'})
        src_name, dst_name = src_info['name'], dst_info['name']
        if src_name == dst_name:
            continue

        nodes_set[src_name] = {'name': src_name, 'namespace': src_info['namespace'], 'ip': src_ip, 'az': src_info.get('az', '')}
        nodes_set[dst_name] = {'name': dst_name, 'namespace': dst_info['namespace'], 'ip': dst_ip, 'az': dst_info.get('az', '')}
        ip_map_by_name[src_name] = src_ip
        ip_map_by_name[dst_name] = dst_ip
        edges_list.append({
            'src': src_name, 'dst': dst_name, 'protocol': protocol,
            'port': port, 'calls': calls, 'avg_latency': avg_lat,
            'errors': errors, 'p99_latency_ms': p99_ms,
        })

    # 5. 批量写入节点
    nodes_list = list(nodes_set.values())
    logger.info(f"批量 upsert {len(nodes_list)} 个节点...")
    batch_upsert_nodes(nodes_list)

    # 5b. 更新 az 属性（set cardinality，保留多 AZ 信息）
    # 设计原则：Microservice 可能跨多 AZ 部署（多副本），az 应存所有实际 AZ 的集合
    # 用 set cardinality：property(set,'az','az-1a') / property(set,'az','az-1c')
    # 每次 ETL 先 drop 旧 az（反映当前真实部署），再写入本次 ip_map 中的 AZ
    #
    # 查询示例：az-1a 故障影响哪些服务？
    #   g.V().hasLabel('Microservice').has('az','ap-northeast-1a').values('name')
    # → 返回有 pod 在 1a 的所有服务（含多 AZ 部署的服务，如 petsite）

    # 构建 service → {az1, az2, ...} 映射（从所有 pod 的 node_az 信息）
    svc_azs_map: dict = {}  # service_name → set of azs
    for pod_ip, info in ip_map.items():
        sn = info['name']
        az_val = info.get('az', '')
        if az_val:
            if sn not in svc_azs_map:
                svc_azs_map[sn] = set()
            svc_azs_map[sn].add(az_val)

    az_updated = 0
    for sn, az_set in svc_azs_map.items():
        n = safe_str(sn)
        try:
            # Step 1: drop 旧 az 属性（清除过期的 AZ 信息，如 pod 被重新调度到其他 AZ）
            neptune_query(
                f"g.V().hasLabel('Microservice').has('name','{n}').properties('az').drop()"
            )
            # Step 2: 逐一添加当前所有 AZ（set cardinality = 自动去重）
            for az_val in az_set:
                az_s = safe_str(az_val)
                neptune_query(
                    f"g.V().hasLabel('Microservice').has('name','{n}')"
                    f".property(set,'az','{az_s}')"
                )
            az_updated += 1
        except Exception as e:
            logger.error(f"Update az {sn}: {e}")
    logger.info(f"az 属性更新完成: {az_updated} 个服务，az_sets={{{', '.join(f'{k}:{v}' for k,v in list(svc_azs_map.items())[:5])}}}")

    # 6. 写入边（复用连接，无需 get_vertex_id）
    logger.info(f"upsert {len(edges_list)} 条边...")
    batch_upsert_edges(edges_list)

    # 7. 副本数 + resource limits
    replica_counts = fetch_replica_counts(ip_map)
    resource_limits = fetch_resource_limits(ip_map)

    # 8. 合并 dependency 查询 + metrics 更新（每服务 2 次请求，原来 5 次）
    service_names = list(nodes_set.keys())
    logger.info(f"更新 {len(service_names)} 个服务的指标...")
    batch_fetch_dependency_and_update(service_names, l7_metrics, ip_map_by_name,
                                       replica_counts, resource_limits, active_connections_map)

    # 9. 验证
    try:
        v_res = neptune_query("g.V().count()")
        e_res = neptune_query("g.E().count()")
        v_count = extract_value(v_res['result']['data']['@value'][0])
        e_count = extract_value(e_res['result']['data']['@value'][0])
        logger.info(f"Neptune 图状态: 顶点={v_count}, 边={e_count}")
    except Exception as e:
        logger.warning(f"Verification failed: {e}")

    duration = int((time.time() - t0) * 1000)
    logger.info(f"=== ETL 完成: nodes={len(nodes_list)}, edges={len(edges_list)}, {duration}ms ===")
    return {"nodes": len(nodes_list), "edges": len(edges_list), "duration_ms": duration}


def handler(event, context):
    try:
        return {"statusCode": 200, "body": run_etl()}
    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        raise

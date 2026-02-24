#!/usr/bin/env python3
"""fix_neptune_data.py - 修复 Neptune 图中的已知脏数据"""
import sys, time, boto3
sys.path.insert(0, '/home/ubuntu/tech/graph-dp-cdk/lambda/etl_aws')
from neptune_etl_aws import neptune_query, safe_str, extract_value

def fix_petfood_priority():
    print("Fix 1: petfood recovery_priority Tier2→Tier1")
    r = neptune_query("g.V().has('Microservice','name','petfood').property(single,'recovery_priority','Tier1').values('recovery_priority')")
    vals = r.get('result',{}).get('data',{}).get('@value',[])
    print(f"  Result: {vals}")

def fix_rds_reader_role():
    print("Fix 2: RDSInstance reader/writer role correction")
    rds = boto3.client('rds', region_name='ap-northeast-1')
    clusters = rds.describe_db_clusters()['DBClusters']
    for cluster in clusters:
        if cluster.get('Engine') == 'neptune':
            continue
        for member in cluster.get('DBClusterMembers', []):
            inst_id = member['DBInstanceIdentifier']
            correct_role = 'writer' if member['IsClusterWriter'] else 'reader'
            n = safe_str(inst_id)
            r = neptune_query(
                f"g.V().has('RDSInstance','name','{n}')"
                f".property(single,'role','{correct_role}')"
                f".values('name')"
            )
            vals = r.get('result',{}).get('data',{}).get('@value',[])
            print(f"  {inst_id}: set role={correct_role}, found={bool(vals)}")

def fix_last_updated_cardinality():
    print("Fix 3: last_updated list cardinality cleanup")
    r = neptune_query("""
        g.V().has('last_updated')
         .filter(__.properties('last_updated').count().is(gt(1)))
         .project('vid','lu_vals')
         .by(id())
         .by(__.properties('last_updated').value().fold())
         .limit(500)
    """)
    nodes_data = r.get('result',{}).get('data',{}).get('@value',[])
    print(f"  Found {len(nodes_data)} nodes with list last_updated")
    fixed = 0
    for item in nodes_data:
        if not isinstance(item, dict) or '@value' not in item:
            continue
        vals = item['@value']
        d = {}
        for i in range(0, len(vals)-1, 2):
            k = vals[i]
            v = vals[i+1]
            if isinstance(v, dict) and '@value' in v:
                vl = v['@value']
                if isinstance(vl, list):
                    v = [x.get('@value',x) if isinstance(x,dict) else x for x in vl]
                else:
                    v = vl
            d[str(k)] = v
        vid = d.get('vid','')
        lu_list = d.get('lu_vals',[])
        if not vid or not lu_list:
            continue
        max_ts = max(int(x) for x in lu_list if x is not None)
        neptune_query(f"g.V('{vid}').properties('last_updated').drop()")
        neptune_query(f"g.V('{vid}').property(single,'last_updated',{max_ts})")
        fixed += 1
    print(f"  Fixed {fixed} nodes")

if __name__ == '__main__':
    fix_petfood_priority()
    fix_rds_reader_role()
    fix_last_updated_cardinality()
    print("All done!")

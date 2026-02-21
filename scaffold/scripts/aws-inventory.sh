#!/bin/bash
# aws-inventory.sh — Scan AWS regions for all non-default resources.
# Usage: aws-inventory.sh [region ...]
# If no regions given, scans all regions.
set -euo pipefail

if [ $# -gt 0 ]; then
    REGIONS="$@"
else
    REGIONS=$(aws ec2 describe-regions --query 'Regions[].RegionName' --output text)
fi

echo "=== Scanning regions: $REGIONS ==="
echo ""

for R in $REGIONS; do
    BUF=""

    # EC2 instances (non-terminated)
    INSTANCES=$(aws ec2 describe-instances --region "$R" \
        --filters 'Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down' \
        --output json 2>/dev/null)
    PARSED=$(echo "$INSTANCES" | python3 -c "
import json,sys
data=json.load(sys.stdin)
items=[]
for r in data['Reservations']:
    for i in r['Instances']:
        name=next((t['Value'] for t in i.get('Tags',[]) if t['Key']=='Name'),'(unnamed)')
        items.append(f\"    {i['InstanceId']} {i['InstanceType']:12s} {i['State']['Name']:12s} {i.get('LaunchTime','')} {name}\")
print(len(items))
for x in items: print(x)
")
    N=$(echo "$PARSED" | head -1)
    if [ "$N" -gt 0 ]; then
        BUF+="  EC2 Instances:
$(echo "$PARSED" | tail -n +2)
"
    fi

    # EBS volumes
    VOLS=$(aws ec2 describe-volumes --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
for v in data['Volumes']:
    tags = {t['Key']:t['Value'] for t in v.get('Tags',[])}
    name = tags.get('Name','')
    print(f\"    {v['VolumeId']} {v['State']:12s} {v['Size']:>4d}GB {v['CreateTime']} {name}\")
print(f'COUNT={len(data[\"Volumes\"])}')
")
    if echo "$VOLS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  EBS Volumes:
$(echo "$VOLS" | grep -v '^COUNT=')
"
    fi

    # EBS snapshots
    SNAPS=$(aws ec2 describe-snapshots --owner-ids self --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
for s in data['Snapshots']:
    print(f\"    {s['SnapshotId']} {s['StartTime']} {s['VolumeSize']:>4d}GB {s.get('Description','')[:50]}\")
print(f'COUNT={len(data[\"Snapshots\"])}')
")
    if echo "$SNAPS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  EBS Snapshots:
$(echo "$SNAPS" | grep -v '^COUNT=')
"
    fi

    # AMIs
    AMIS=$(aws ec2 describe-images --owners self --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
for i in data['Images']:
    print(f\"    {i['ImageId']} {i.get('Name','unnamed'):40s} {i['CreationDate']}\")
print(f'COUNT={len(data[\"Images\"])}')
")
    if echo "$AMIS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  AMIs:
$(echo "$AMIS" | grep -v '^COUNT=')
"
    fi

    # Elastic IPs
    EIPS=$(aws ec2 describe-addresses --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
for a in data['Addresses']:
    assoc = a.get('InstanceId', a.get('AssociationId', 'unassociated'))
    print(f\"    {a['PublicIp']:16s} {a.get('AllocationId','')} -> {assoc}\")
print(f'COUNT={len(data[\"Addresses\"])}')
")
    if echo "$EIPS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  Elastic IPs:
$(echo "$EIPS" | grep -v '^COUNT=')
"
    fi

    # Security groups (non-default)
    SGS=$(aws ec2 describe-security-groups --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
items=[sg for sg in data['SecurityGroups'] if sg['GroupName'] != 'default']
for sg in items:
    print(f\"    {sg['GroupName']:30s} {sg['GroupId']:20s} {sg.get('Description','')[:40]}\")
print(f'COUNT={len(items)}')
")
    if echo "$SGS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  Security Groups:
$(echo "$SGS" | grep -v '^COUNT=')
"
    fi

    # Key pairs
    KPS=$(aws ec2 describe-key-pairs --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
for kp in data.get('KeyPairs',[]):
    print(f\"    {kp['KeyName']:30s} {kp.get('CreateTime','unknown')}\")
print(f'COUNT={len(data.get(\"KeyPairs\",[]))}')
")
    if echo "$KPS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  Key Pairs:
$(echo "$KPS" | grep -v '^COUNT=')
"
    fi

    # NAT Gateways (non-deleted)
    NATS=$(aws ec2 describe-nat-gateways --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
items=[n for n in data['NatGateways'] if n['State'] != 'deleted']
for n in items:
    print(f\"    {n['NatGatewayId']} {n['State']} {n['CreateTime']}\")
print(f'COUNT={len(items)}')
")
    if echo "$NATS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  NAT Gateways:
$(echo "$NATS" | grep -v '^COUNT=')
"
    fi

    # Load Balancers
    LBS=$(aws elbv2 describe-load-balancers --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
for lb in data['LoadBalancers']:
    print(f\"    {lb['LoadBalancerName']:30s} {lb['CreatedTime']} {lb['State']['Code']}\")
print(f'COUNT={len(data[\"LoadBalancers\"])}')
")
    if echo "$LBS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  Load Balancers:
$(echo "$LBS" | grep -v '^COUNT=')
"
    fi

    # Non-default VPCs
    VPCS=$(aws ec2 describe-vpcs --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
items=[v for v in data['Vpcs'] if not v.get('IsDefault',False)]
for v in items:
    tags = {t['Key']:t['Value'] for t in v.get('Tags',[])}
    name = tags.get('Name','')
    print(f\"    {v['VpcId']} {v['CidrBlock']} {name}\")
print(f'COUNT={len(items)}')
")
    if echo "$VPCS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  Non-Default VPCs:
$(echo "$VPCS" | grep -v '^COUNT=')
"
    fi

    # Lambda
    LAMBDAS=$(aws lambda list-functions --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
for f in data['Functions']:
    print(f\"    {f['FunctionName']:40s} {f.get('Runtime','N/A'):12s} {f['LastModified']}\")
print(f'COUNT={len(data[\"Functions\"])}')
")
    if echo "$LAMBDAS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  Lambda Functions:
$(echo "$LAMBDAS" | grep -v '^COUNT=')
"
    fi

    # RDS
    RDS=$(aws rds describe-db-instances --region "$R" --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
for db in data['DBInstances']:
    print(f\"    {db['DBInstanceIdentifier']:30s} {db['DBInstanceClass']:15s} {db['DBInstanceStatus']} {db.get('InstanceCreateTime','')}\")
print(f'COUNT={len(data[\"DBInstances\"])}')
")
    if echo "$RDS" | grep -q 'COUNT=[1-9]'; then
        BUF+="  RDS Instances:
$(echo "$RDS" | grep -v '^COUNT=')
"
    fi

    # S3 is global, only check once
    if [ "$R" = "us-east-1" ] || [ "$R" = "$1" ] 2>/dev/null; then
        S3=$(aws s3api list-buckets --output json 2>/dev/null | python3 -c "
import json,sys
data=json.load(sys.stdin)
for b in data.get('Buckets',[]):
    print(f\"    {b['Name']:40s} {b['CreationDate']}\")
print(f'COUNT={len(data.get(\"Buckets\",[]))}')
")
        if echo "$S3" | grep -q 'COUNT=[1-9]'; then
            BUF+="  S3 Buckets (global):
$(echo "$S3" | grep -v '^COUNT=')
"
        fi
    fi

    if [ -n "$BUF" ]; then
        echo "--- $R ---"
        echo "$BUF"
    fi
done

echo ""
echo "=== IAM (global) ==="

echo "  Users:"
aws iam list-users --output json | python3 -c "
import json,sys
data=json.load(sys.stdin)
for u in data['Users']:
    print(f\"    {u['UserName']:30s} {u['CreateDate']}\")
if not data['Users']: print('    (none)')
"

echo "  Customer-Managed Policies:"
aws iam list-policies --scope Local --output json | python3 -c "
import json,sys
data=json.load(sys.stdin)
for p in data['Policies']:
    print(f\"    {p['PolicyName']:30s} {p['CreateDate']} attached={p['AttachmentCount']}\")
if not data['Policies']: print('    (none)')
"

echo "  Roles (non-service-linked):"
aws iam list-roles --output json | python3 -c "
import json,sys
data=json.load(sys.stdin)
for r in data['Roles']:
    if '/aws-service-role/' not in r['Path']:
        print(f\"    {r['RoleName']:40s} {r['CreateDate']}\")
"

echo ""
echo "=== Done ==="

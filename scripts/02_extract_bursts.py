#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
import subprocess
import os
import time

# ==========================================================
# CONFIGURATION
# ==========================================================

BASE_DIR = Path(
    "/mnt/CEPH_PROJECTS/ARGOS/01_abhishek/po_valley_ssm"
)

PLAN_FILE = (
    BASE_DIR /
    "metadata" /
    "s1_aoi_s3_extraction_plan.csv"
)

LOG_FILE = (
    BASE_DIR /
    "metadata" /
    "s1_aoi_s3_extraction_log.csv"
)

LOCAL_MANIFEST = (
    BASE_DIR /
    "metadata" /
    "s1_aoi_s3_local_manifest.csv"
)

ENV_FILE = BASE_DIR / ".env"

DOCKER_IMAGE = "cdse_utilities"

# ==========================================================
# DOWNLOAD WINDOW
# ==========================================================

START_DATE = "2021-04-01"
END_DATE = "2021-04-03"     # exclusive

# None = download everything in window
MAX_EXTRACTIONS = None

SKIP_SUCCEEDED = True
SKIP_EXISTING_OUTPUTS = True

# ==========================================================
# LOAD ENV
# ==========================================================

def load_env(path):

    creds = {}

    for line in path.read_text().splitlines():

        line = line.strip()

        if (
            not line or
            line.startswith("#") or
            "=" not in line
        ):
            continue

        k, v = line.split("=", 1)

        creds[k.strip()] = (
            v.strip()
            .strip('"')
            .strip("'")
        )

    return creds


creds = load_env(ENV_FILE)

AWS_ACCESS_KEY_ID = creds.get(
    "CDSE_S3_ACCESS_KEY"
)

AWS_SECRET_ACCESS_KEY = creds.get(
    "CDSE_S3_SECRET_KEY"
)

if not AWS_ACCESS_KEY_ID:
    raise RuntimeError(
        "Missing CDSE_S3_ACCESS_KEY"
    )

if not AWS_SECRET_ACCESS_KEY:
    raise RuntimeError(
        "Missing CDSE_S3_SECRET_KEY"
    )

print(
    "Credentials loaded:",
    AWS_ACCESS_KEY_ID[:8] + "..."
)

# ==========================================================
# VERIFY DOCKER
# ==========================================================

test = subprocess.run(
    ["docker", "images"],
    capture_output=True,
    text=True
)

if test.returncode != 0:

    raise RuntimeError(
        f"Docker unavailable:\n{test.stderr}"
    )

# ==========================================================
# LOAD PLAN
# ==========================================================

plan = pd.read_csv(
    PLAN_FILE,
    low_memory=False
)

print(
    f"Total plan size: {len(plan):,}"
)

# ==========================================================
# FILTER DATE RANGE
# ==========================================================

if "acquisition_datetime" not in plan.columns:

    raise ValueError(
        "acquisition_datetime missing "
        "from extraction plan."
    )

plan["acquisition_datetime"] = pd.to_datetime(
    plan["acquisition_datetime"],
    utc=True,
    errors="coerce"
)

plan = plan[
    (
        plan["acquisition_datetime"]
        >= START_DATE
    )
    &
    (
        plan["acquisition_datetime"]
        < END_DATE
    )
].copy()

print(
    f"Jobs in date window "
    f"{START_DATE} ? {END_DATE}: "
    f"{len(plan):,}"
)

# ==========================================================
# RESUME SUPPORT
# ==========================================================

successful_jobs = set()

if LOG_FILE.exists():

    log_df = pd.read_csv(
        LOG_FILE,
        low_memory=False
    )

    successful_jobs = set(
        log_df.loc[
            log_df["status"] == "success",
            "output_prefix"
        ]
    )

    print(
        f"Found {len(successful_jobs):,} "
        "previous successful jobs"
    )

# ==========================================================
# SKIP COMPLETED
# ==========================================================

if SKIP_SUCCEEDED and successful_jobs:

    before = len(plan)

    plan = plan[
        ~plan["output_prefix"]
        .isin(successful_jobs)
    ].copy()

    print(
        f"Skipped "
        f"{before-len(plan):,} "
        "completed jobs"
    )

# ==========================================================
# LIMIT DOWNLOADS
# ==========================================================

if MAX_EXTRACTIONS is not None:

    plan = plan.head(
        int(MAX_EXTRACTIONS)
    )

print(
    f"Jobs to execute: {len(plan):,}"
)

# ==========================================================
# EXTRACTION FUNCTION
# ==========================================================

def run_extractor(row):

    output_dir = Path(
        row["expected_output_dir"]
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    output_in_container = (
        "/workspace/"
        + str(
            output_dir.relative_to(
                BASE_DIR
            )
        )
    )

    cmd = [

        "docker",
        "run",
        "--rm",

        "-v",
        f"{BASE_DIR}:/workspace",

        "-e",
        "AWS_ACCESS_KEY_ID",

        "-e",
        "AWS_SECRET_ACCESS_KEY",

        DOCKER_IMAGE,

        "sentinel1_burst_extractor.sh",

        "-o",
        output_in_container,

        "-n",
        str(
            row["parent_product_name"]
        ),

        "-p",
        str(
            row["polarization"]
        ),

        "-s",
        str(
            row["swath"]
        ),

        "-r",
        str(
            row["relative_burst_id"]
        )
    ]

    env = os.environ.copy()

    env["AWS_ACCESS_KEY_ID"] = (
        AWS_ACCESS_KEY_ID
    )

    env["AWS_SECRET_ACCESS_KEY"] = (
        AWS_SECRET_ACCESS_KEY
    )

    start = time.time()

    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True
    )

    elapsed = round(
        time.time() - start,
        2
    )

    return result, elapsed


# ==========================================================
# RUN EXTRACTION
# ==========================================================

for idx, row in plan.iterrows():

    print(
        f"\n[{idx+1}/{len(plan)}]"
    )

    print(
        row["parent_product_name"]
    )

    outdir = Path(
        row["expected_output_dir"]
    )

    if (
        SKIP_EXISTING_OUTPUTS
        and outdir.exists()
        and any(outdir.iterdir())
    ):
        print(
            "Output already exists. Skipping."
        )
        continue

    result, elapsed = run_extractor(row)

    status = (
        "success"
        if result.returncode == 0
        else "failed"
    )

    rec = {

        "parent_product_name":
            row["parent_product_name"],

        "swath":
            row["swath"],

        "polarization":
            row["polarization"],

        "relative_burst_id":
            row["relative_burst_id"],

        "output_prefix":
            row["output_prefix"],

        "status":
            status,

        "return_code":
            result.returncode,

        "elapsed_seconds":
            elapsed,

        "stdout_tail":
            result.stdout[-2000:],

        "stderr_tail":
            result.stderr[-2000:],

        "run_timestamp_utc":
            pd.Timestamp.utcnow()
            .isoformat()
    }

    pd.DataFrame([rec]).to_csv(
        LOG_FILE,
        mode="a",
        header=not LOG_FILE.exists(),
        index=False
    )

    print(
        f"{status} "
        f"({elapsed}s)"
    )

    if status == "failed":

        print(rec["stderr_tail"])

        break

# ==========================================================
# LOCAL INVENTORY
# ==========================================================

inventory = pd.read_csv(
    PLAN_FILE,
    low_memory=False
)

inventory["output_dir_exists"] = (
    inventory["expected_output_dir"]
    .map(
        lambda p: Path(p).exists()
    )
)

inventory.to_csv(
    LOCAL_MANIFEST,
    index=False
)

print()
print("Inventory written:")
print(LOCAL_MANIFEST)
print()

print(
    "Output folders present:",
    inventory["output_dir_exists"]
    .sum(),
    "/",
    len(inventory)
)
##!/usr/bin/env python3
#"""extract_bursts.py
#Run CDSE Docker extraction from a previously generated plan.
#"""
#from pathlib import Path
#import pandas as pd, subprocess, os, time
#
#BASE_DIR = Path('/mnt/CEPH_PROJECTS/ARGOS/01_abhishek/po_valley_ssm')
#PLAN_FILE = BASE_DIR/'metadata'/'s1_aoi_s3_extraction_plan.csv'
#LOG_FILE = BASE_DIR/'metadata'/'s1_aoi_s3_extraction_log.csv'
#ENV_FILE = BASE_DIR/'.env'
#DOCKER_IMAGE='cdse_utilities'
#MAX_EXTRACTIONS=2
#
#creds={}
#for line in ENV_FILE.read_text().splitlines():
#    if '=' in line and not line.strip().startswith('#'):
#        k,v=line.split('=',1)
#        creds[k.strip()]=v.strip().strip('"').strip("'")
#
#aws_key=creds.get('CDSE_S3_ACCESS_KEY')
#aws_secret=creds.get('CDSE_S3_SECRET_KEY')
#if not aws_key or not aws_secret:
#    raise RuntimeError('Missing CDSE_S3_ACCESS_KEY/CDSE_S3_SECRET_KEY')
#
#plan=pd.read_csv(PLAN_FILE)
#if MAX_EXTRACTIONS is not None:
#    plan=plan.head(MAX_EXTRACTIONS)
#
#
#for _,row in plan.iterrows():
#    outdir=Path(row['expected_output_dir'])
#    outdir.mkdir(parents=True, exist_ok=True)
#    cmd=['docker','run','--rm','-v',f'{BASE_DIR}:/workspace','-e','AWS_ACCESS_KEY_ID','-e','AWS_SECRET_ACCESS_KEY',DOCKER_IMAGE,'sentinel1_burst_extractor.sh','-o','/workspace/'+str(outdir.relative_to(BASE_DIR)),'-n',str(row['parent_product_name']),'-p',str(row['polarization']),'-s',str(row['swath']),'-r',str(row['relative_burst_id'])]
#    env=os.environ.copy()
#    env['AWS_ACCESS_KEY_ID']=aws_key
#    env['AWS_SECRET_ACCESS_KEY']=aws_secret
#    r=subprocess.run(cmd,env=env,capture_output=True,text=True)
#    rec={'parent_product_name':row['parent_product_name'],'status':'success' if r.returncode==0 else 'failed','return_code':r.returncode,'stderr_tail':r.stderr[-2000:],'timestamp':pd.Timestamp.utcnow().isoformat()}
#    pd.DataFrame([rec]).to_csv(LOG_FILE,mode='a',header=not LOG_FILE.exists(),index=False)
#    print(rec)
#    if r.returncode!=0:
#        break

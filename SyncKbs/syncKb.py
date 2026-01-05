import boto3
import time
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env from parent directory
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

REGION = os.getenv("AWS_REGION", "us-east-1")

bedrock = boto3.client(
    "bedrock-agent",
    region_name=REGION
)

# ----------------------------
# Discovery helpers
# ----------------------------

def get_kb_id_by_name(kb_name):
    paginator = bedrock.get_paginator("list_knowledge_bases")

    for page in paginator.paginate():
        for kb in page.get("knowledgeBaseSummaries", []):
            if kb["name"] == kb_name:
                return kb["knowledgeBaseId"]

    raise ValueError(f"Knowledge Base with name '{kb_name}' not found")


def get_data_sources_for_kb(kb_id):
    data_sources = []
    paginator = bedrock.get_paginator("list_data_sources")

    for page in paginator.paginate(knowledgeBaseId=kb_id):
        data_sources.extend(page.get("dataSourceSummaries", []))

    if not data_sources:
        raise ValueError(f"No data sources found for KB {kb_id}")

    return data_sources


# ----------------------------
# Ingestion helpers
# ----------------------------

def start_ingestion(kb_id, ds_id):
    response = bedrock.start_ingestion_job(
        knowledgeBaseId=kb_id,
        dataSourceId=ds_id
    )
    job_id = response["ingestionJob"]["ingestionJobId"]
    print(f"✅ Ingestion started (DS: {ds_id}, Job: {job_id})")
    return job_id


def wait_for_completion(kb_id, ds_id, job_id):
    while True:
        response = bedrock.get_ingestion_job(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id,
            ingestionJobId=job_id
        )

        status = response["ingestionJob"]["status"]
        print(f"⏳ Status (DS: {ds_id}): {status}")

        if status in ["COMPLETE", "FAILED"]:
            print(f"🎯 Final status (DS: {ds_id}): {status}\n")
            return status

        time.sleep(10)


# ----------------------------
# Main flow
# ----------------------------

def main():
    try:
        count = int(input("How many Knowledge Bases do you want to sync? ").strip())
        if count <= 0:
            print("Nothing to sync. Exiting.")
            return
    except ValueError:
        print("Invalid number. Exiting.")
        return

    kb_names = []

    for i in range(count):
        name = input(f"Enter Knowledge Base {i + 1} NAME: ").strip()
        kb_names.append(name)

    print("\n🔍 Discovering Knowledge Bases & Data Sources...\n")

    for kb_name in kb_names:
        try:
            print(f"📘 KB: {kb_name}")
            kb_id = get_kb_id_by_name(kb_name)
            print(f"   ↳ KB ID: {kb_id}")

            data_sources = get_data_sources_for_kb(kb_id)
            print(f"   ↳ Found {len(data_sources)} data source(s)\n")

            for ds in data_sources:
                ds_id = ds["dataSourceId"]
                ds_name = ds.get("name", "Unnamed")

                print(f"🚀 Syncing Data Source: {ds_name} ({ds_id})")
                job_id = start_ingestion(kb_id, ds_id)
                wait_for_completion(kb_id, ds_id, job_id)

        except Exception as e:
            print(f"❌ Failed for KB '{kb_name}': {e}\n")


if __name__ == "__main__":
    main()

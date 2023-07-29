# A script to create or update or retrieve a file in the Google Bucket associated with this workspace of Research All Of Us
# Usage: At command line, enter python createorupdateorretrievefileingooglebucket.py <source path (e.g., data/Feature_Matrix.csv)> <destination path (e.g., data/Feature_Matrix.csv)>

import argparse
import os
import subprocess

def create_or_update_or_retrieve(indicator_of_whether_to_create_or_update_or_retrieve, source_path, destination_path):
    name_of_bucket = os.getenv("WORKSPACE_BUCKET")
    print("name of bucket: " + name_of_bucket)
    if indicator_of_whether_to_create_or_update_or_retrieve == "create_or_update":
        args = ["gsutil", "cp", source_path, f"{name_of_bucket}/{destination_path}"]
    elif indicator_of_whether_to_create_or_update_or_retrieve == "retrieve":
        args = ["gsutil", "cp", f"{name_of_bucket}/{source_path}", destination_path]
    else:
        raise Exception("indicator of whether to create or update or retrieve is not one of [\"retrieve\", \"create_or_update\"]")
    output_of_run = subprocess.run(args, capture_output = True)
    print(output_of_run)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('indicator_of_whether_to_create_or_update_or_retrieve', choices = ["create_or_update", "retrieve"])
    parser.add_argument('source_path')
    parser.add_argument('destination_path')
    args = parser.parse_args()
    print("indicator of whether to create or update or retrieve: " + args.indicator_of_whether_to_create_or_update_or_retrieve)
    print("source path: " + args.source_path)
    print("destination path: " + args.destination_path)
    create_or_update_or_retrieve(args.indicator_of_whether_to_create_or_update_or_retrieve, args.source_path, args.destination_path)

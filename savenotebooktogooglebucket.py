# A script to save a JuPyteR notebook to the Google Bucket associated with this workspace of Research All Of Us
# Usage: At command line, enter python savenotebooktogooglebucket.py <name of notebook>

import os
import subprocess
import sys

def save_notebook_to_google_bucket(name_of_notebook):
    name_of_bucket = os.getenv("WORKSPACE_BUCKET")
    print("name of bucket: " + name_of_bucket)
    args = ["gsutil", "cp", name_of_notebook, f"{name_of_bucket}/notebooks/"]
    output_of_run = subprocess.run(args, capture_output = True)
    output_of_run.stderr

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("\nPass interpreter 'python' the name of this script and the name of a JuPyteR notebook.\n")
        print("Include single or double quotes around the name of the notebook.")
        assert(False)
    name_of_notebook = sys.argv[1]
    print("name of notebook: \"" + name_of_notebook + "\"")
    save_notebook_to_google_bucket(name_of_notebook)

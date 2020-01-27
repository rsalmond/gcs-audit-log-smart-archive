# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Implementation of "install" command.
"""

import json
import logging
from subprocess import run, PIPE, CompletedProcess

from gcs_sa.config import get_config

LOG = logging.getLogger(__name__)
SETTINGS = dict()


def install_command(auto_agree: bool) -> None:
    SETTINGS["auto_agree"] = auto_agree
    # Steps of installation are...
    intro()
    enable_audit_logging()
    set_up_log_to_bq()
    # TODO -- Is it worthwhile to optionally do a log exclusion? Are they retained in SD despite being routed to the BQ sink?
    interactive_message("""
    You're all set!
    """)


def intro() -> None:
    message = """
    This is an interactive installation of the GCS Smart Archiver. To perform the installation without interaction, use the -y switch.
    """ if SETTINGS["auto_agree"] else ""

    interactive_message(message + """
    This installation is a lightweight alternative for those who don't wish to use declarative Infrastructure-as-Code, such as Google Cloud Deployment 
    Manager or Hashicorp Terraform.

    To keep the code light and compatibility high, most of the work will be handled using the GCloud SDK's CLI, `gcloud`. Make sure that it is installed
    and configured with the correct authentication settings. The project argument will be provided based on this program's current configuration.

    The following steps will be performed:

        - Enable Google Cloud Storage audit logging.
        - Enable a sink of those log entries into BigQuery.
    """)
    check_continue(call_if_false=abort)


def enable_audit_logging() -> None:
    config = get_config()
    project_id = config.get('GCP', 'PROJECT')
    desired_storage_log = {
        "service": "storage.googleapis.com",
        "auditLogConfigs": [{
            "logType": "DATA_READ"
        }, {
            "logType": "DATA_WRITE"
        }]
    }
    interactive_message("""
    First, we will patch your project-level IAM policy to log all storage.googleapis.com requests, both reads and writes. The configuration will look like this:

    {}

    And will be applied to the project: {}
    """.format(json.dumps(desired_storage_log), project_id))
    check_continue(call_if_false=abort)

    # Get IAM policy
    interactive_message("""
    Getting IAM policy for project: {}
    """.format(project_id))
    iam_policy_string = safe_run([
        'gcloud', 'projects', 'get-iam-policy', project_id, '--format', 'json'
    ],
                                 stdout=PIPE).stdout.decode('utf-8')
    policy = json.loads(iam_policy_string)

    # Check and modify auditConfigs
    audit_configs = policy["auditConfigs"]
    existing_storage_logs = list(
        filter(lambda x: x["service"] == "storage.googleapis.com",
               audit_configs))
    if existing_storage_logs:
        if desired_storage_log in existing_storage_logs:
            interactive_message("""
            Found existing storage audit log config matching desired output. No changes are needed.
            """)
            return
        else:
            interactive_message("""
            Found existing storage audit log config. Outputting the existing configuration for reference before overwriting:

            {}
            """.format(existing_storage_logs))

    # Set the IAM policy.
    interactive_message("""
    Setting IAM policy for project: {}
    """.format(project_id))
    # Create a list without storage settings.
    final_audit_configs = list(
        filter(lambda x: x["service"] != "storage.googleapis.com",
               audit_configs))
    # Append, update, and dump the policy to a temp file.
    final_audit_configs.append(desired_storage_log)
    policy["auditConfigs"] = final_audit_configs
    tempfile = safe_run(['tempfile'], stdout=PIPE).stdout.decode('utf-8')
    with open(tempfile, "w") as tmp:
        json.dump(policy, tmp)

    safe_run([
        'gcloud', 'projects', 'set-iam-policy', project_id, '--format', 'json',
        tempfile
    ])
    interactive_message("""
    Google Cloud Storage audit logging enabled.
    """)


def set_up_log_to_bq() -> bool:
    config = get_config()
    project_id = config.get('GCP', 'PROJECT')
    dataset_location = config.get('BIGQUERY', 'DATASET_LOCATION')
    dataset_name = config.get('BIGQUERY', 'DATASET_NAME')
    sink_name = config.get('AUDITLOGS', 'SINK_NAME')
    fq_dataset_name = project_id + ':' + dataset_name

    interactive_message("""
    Next, we will set up a log sink to BigQuery. To do this, we'll create a dataset in BigQuery, with the following characteristics:

    Project: {}
    Location: {}
    Dataset Name: {}

    Following that, we will create a sink with this dataset as the specified destination. 
    
    Then finally, we will modify the dataset's permissions to allow the sink service account to write to it.
    """.format(project_id, dataset_location, dataset_name))
    check_continue(call_if_false=abort)

    ds_info = create_and_describe_dataset(dataset_location, fq_dataset_name)
    sink_info = create_and_describe_sink(project_id, sink_name, dataset_name)

    writer_identity = sink_info["writerIdentity"].split(':')[1]
    desired_access = {'role': 'WRITER', 'userByEmail': writer_identity}

    if desired_access in ds_info["access"]:
        interactive_message("""
        The sink writer service account already has write access to the dataset.
        """)
    else:
        accesses = list(ds_info["access"])
        accesses.append(desired_access)
        ds_info["access"] = accesses
        update_dataset(ds_info, project_id, dataset_name)


def create_and_describe_dataset(dataset_location: str, fq_dataset_name: str):
    interactive_message("""
    Creating the dataset...
    """)
    ds_create_result = run([
        'bq', '--location=' + dataset_location, 'mk', '--dataset',
        fq_dataset_name
    ],
                           stdout=PIPE,
                           stderr=PIPE)
    if check_already_exists(ds_create_result):
        interactive_message("""
        Looks like the desired dataset already exists.
        """)
    elif ds_create_result.returncode:
        abort(reason="Could not create dataset. {}".format(
            ds_create_result.stderr.decode('utf-8')))

    interactive_message("""
    Getting the dataset's information...
    """)
    return json.loads(
        safe_run(['bq', 'show', '--format=json', fq_dataset_name],
                 stdout=PIPE).stdout.decode('utf-8'))


def create_and_describe_sink(project_id: str, sink_name: str, dataset_name: str):
    interactive_message("""
    Creating the sink...
    """)
    # TODO -- add parameter for sink name
    sink_create_result = run([
        'gcloud',
        'logging',
        'sinks',
        'create',
        sink_name,
        'bigquery.googleapis.com/projects/' + project_id + '/datasets/' +
        dataset_name,
        '--log-filter',
        'resource.type="gcs_bucket" (protoPayload.methodName="storage.objects.get" OR protoPayload.methodName="storage.objects.create")',
    ],
                             stdout=PIPE,
                             stderr=PIPE)
    if check_already_exists(sink_create_result):
        interactive_message("""
        Looks like the desired sink already exists.
        """)
    elif sink_create_result.returncode:
        abort(reason="Could not create sink. {}".format(
            sink_create_result.stderr.decode('utf-8')))

    interactive_message("""
    Getting the sink's information...
    """)
    return json.loads(
        safe_run([
            'gcloud',
            'logging',
            'sinks',
            'describe',
            sink_name,
            '--format',
            'json',
        ],
                 stdout=PIPE).stdout.decode('utf-8'))


def update_dataset(updated_info: dict, project_id: str, dataset_name: str):
    interactive_message("""
    Updating the dataset to add writer permission for the log sink's writer service account.
    """)

    tempfile = safe_run(['tempfile'], stdout=PIPE).stdout.decode('utf-8')
    with open(tempfile, "w") as tmp:
        json.dump(updated_info, tmp)

    safe_run(
        ['bq', 'update', '--source', tempfile, project_id + ':' + dataset_name],
        stdout=PIPE)


def interactive_message(msg: str) -> None:
    msg_decorator = '=========================================='
    print('\n'.join(['', msg_decorator, msg, msg_decorator]))


def check_continue(call_if_false: callable = None) -> bool:
    # short-circuit if auto_agree
    if SETTINGS["auto_agree"]:
        return True
    response = input('\n'.join(['', "Continue? (Y/n) "]))
    print('')
    response = response.strip()
    if not response or response.upper() in ["Y", "YES"]:
        return True
    # handle false response
    if call_if_false:
        call_if_false()
    return False


def check_already_exists(completed: CompletedProcess) -> bool:
    return completed.returncode and (
        'already exists' in completed.stdout.decode('utf-8') or
        'already exists' in completed.stderr.decode('utf-8'))


def abort(reason: str = "User cancelled installation.") -> int:
    print("Aborting installation. Reason: {}".format(reason))
    exit(1)


def safe_run(*args, **kwargs) -> CompletedProcess:
    result = run(*args, **kwargs)
    if result.returncode:
        abort("Process exited with code {}".format(result.returncode))
    return result
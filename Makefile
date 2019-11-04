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

CONFIG_FILE=$(shell if [ -z $$SMART_ARCHIVE_CONFIG ]; then echo default.cfg; else echo $$SMART_ARCHIVE_CONFIG; fi)
PROJECT=$(shell cat $(CONFIG_FILE) | grep PROJECT | cut -d '=' -f 2)
DATASET_NAME=$(shell cat $(CONFIG_FILE) | grep DATASET_NAME | cut -d '=' -f 2)

default: step_explain step_set_up_audit_logging step_set_up_bq_log_sink 

CHECK_CONTINUE = \
	read -p "Continue? (Y/n) " continue; \
	case "$$continue" in \
		n|N ) echo "Stopping." && exit 1 ;; \
		* ) echo -n ;; \
	esac

MESSAGE = \
	echo ========================================== ;\
	echo $1 ;\
	echo ========================================== ;

# Macro for a comma in arguments. This gets expanded after the arguments are parsed.
, := ,

reset:
	@$(call MESSAGE, Erase all step markers and start from the beginning?)
	@$(CHECK_CONTINUE) 
	rm -f step_*

step_explain:
	@echo Using config file: $(CONFIG_FILE)
	@echo ==========================================
	@echo This Makefile will set up logging of GCS object access into a BigQuery dataset.
	@echo
	@echo As it works, it will create step_* files in this directory to save progress. If a step errors, you can fix the underlying issue and resume by running make. Run 'make reset' to start over.
	@echo
	@echo First, it will set up read audit logging on all GCS buckets in the active project.
	@echo Next, it will set up a sink of those logs into BigQuery tables.
	@echo ==========================================
	@$(CHECK_CONTINUE) 
	@touch step_explain

step_set_up_audit_logging:
	@$(call MESSAGE, First$(,) we will patch your project-level IAM policy to turn on DATA_READ admin logging for all storage.googleapis.com requests.)
	@$(CHECK_CONTINUE) 
	
	# stash the iam policy
	gcloud projects get-iam-policy $(PROJECT) --format json \
	| jq '. | if has("auditConfigs") then . else . += {"auditConfigs":[]} end' \
	> /tmp/projectiampolicy
	@echo

	# patch the iam policy
	cat /tmp/projectiampolicy | jq '.auditConfigs += [{"service":"storage.googleapis.com","auditLogConfigs":[{"logType": "DATA_READ"},{"logType": "DATA_WRITE"}]}]' | jq '. + {"auditConfigs":.auditConfigs|unique}' > /tmp/projectiampolicy_patched
	@echo

	# set the iam policy to the patched one
	gcloud projects set-iam-policy $(PROJECT) --format json /tmp/projectiampolicy_patched
	@echo

	@$(call MESSAGE, Success!) 
	@touch step_set_up_audit_logging
	@$(call MESSAGE, Note: Your old and patched IAM policies are stored in /tmp/projectiampolicy*$(,) if you need them.)

step_set_up_bq_log_sink:
	@$(call MESSAGE, Next$(,) we will set up a log sink to BigQuery.)
	@$(CHECK_CONTINUE) 
	
	# make the dataset
	bq --location=US mk --dataset $(PROJECT):$(DATASET_NAME)
	@echo

	# stash dataset info
	bq show --format=prettyjson $(PROJECT):$(DATASET_NAME) > /tmp/dsinfo
	@echo
	
	# make the sink
	gcloud logging sinks create \
	test_sink 'bigquery.googleapis.com/projects/$(PROJECT)/datasets/$(DATASET_NAME)' \
	--log-filter 'resource.type="gcs_bucket" (protoPayload.methodName="storage.objects.get" OR protoPayload.methodName="storage.objects.create")'
	@echo

    # stash sink info
	gcloud logging sinks describe test_sink --format json > /tmp/sinkinfo
	cat /tmp/sinkinfo | jq -r .writerIdentity | awk '{split($$0,arr,":"); print arr[2]}' > /tmp/logwriteridentity
	@echo

	# modify dataset access info
	cat /tmp/dsinfo | jq '.access += [{"role":"WRITER","userByEmail":"'$$(cat /tmp/logwriteridentity)'"}]' | jq '. + {"access":.access|unique}' > /tmp/dsinfo_patched
	@echo

	# modify dataset with new access info
	bq update --source /tmp/dsinfo_patched $(PROJECT):$(DATASET_NAME)
	@echo

	@$(call MESSAGE, Success!) 
	@rm /tmp/dsinfo /tmp/sinkinfo /tmp/logwriteridentity /tmp/dsinfo_patched
	@touch step_set_up_bq_log_sink


teardown:
	@$(call MESSAGE, This will remove the smart archiver and supporting resources.)
	@$(CHECK_CONTINUE) 

	yes | gcloud logging sinks delete test_sink
	yes | bq --location=US rm -r --dataset $(PROJECT):$(DATASET_NAME)

	@$(call MESSAGE, If you continue$(,) audit logging for GCS will be turned off. Stop now if you use audit logging for other applications.)
	@$(CHECK_CONTINUE) 
	# stash the iam policy
	gcloud projects get-iam-policy $(PROJECT) --format json \
	| jq '. | if has("auditConfigs") then . else . += {"auditConfigs":[]} end' \
	> /tmp/projectiampolicy
	@echo
	# patch the iam policy
	cat /tmp/projectiampolicy | jq '.auditConfigs -= [{"service":"storage.googleapis.com","auditLogConfigs":[{"logType": "DATA_READ"},{"logType": "DATA_WRITE"}]}]' | jq '. + {"auditConfigs":.auditConfigs|unique}' > /tmp/projectiampolicy_patched
	@echo
	# set the iam policy to the patched one
	gcloud projects set-iam-policy $(PROJECT) --format json /tmp/projectiampolicy_patched
	@echo

	@$(call MESSAGE, Teardown complete.)
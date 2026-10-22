# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#         http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the Redshift ClusterSubnetGroup resource."""

import time
import pytest
import logging

from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_redshift_resource
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "clustersubnetgroups"

CREATE_WAIT_AFTER_SECONDS = 10
UPDATE_WAIT_AFTER_SECONDS = 10
DELETE_WAIT_AFTER_SECONDS = 10


@pytest.fixture(scope="module")
def redshift_client():
    import boto3
    return boto3.client("redshift")


@pytest.fixture(scope="module")
def ec2_client():
    import boto3
    return boto3.client("ec2")


def get_subnet_id(ec2_client):
    """Get a subnet ID from the default VPC for testing."""
    resp = ec2_client.describe_subnets(
        Filters=[{"Name": "default-for-az", "Values": ["true"]}],
    )
    subnets = resp.get("Subnets", [])
    assert len(subnets) > 0, "No default subnets found for testing"
    return subnets[0]["SubnetId"]


@service_marker
@pytest.mark.canary
class TestClusterSubnetGroup:
    def test_create_update_delete(self, redshift_client, ec2_client):
        subnet_id = get_subnet_id(ec2_client)
        resource_name = random_suffix_name("ack-test-csg", 24)
        description = "ACK test cluster subnet group"

        replacements = REPLACEMENT_VALUES.copy()
        replacements["CLUSTER_SUBNET_GROUP_NAME"] = resource_name
        replacements["DESCRIPTION"] = description
        replacements["SUBNET_ID_1"] = subnet_id

        resource_data = load_redshift_resource(
            "cluster_subnet_group",
            additional_replacements=replacements,
        )
        logging.info(f"Creating ClusterSubnetGroup {resource_name}")

        # Create the resource
        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            resource_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)

        assert cr is not None
        assert k8s.get_resource_exists(ref)

        time.sleep(CREATE_WAIT_AFTER_SECONDS)

        # Wait for the resource to be synced
        cr = k8s.get_resource(ref)
        condition_synced = k8s.get_resource_condition(ref, "ACK.ResourceSynced")
        # The custom_sync pattern may cause an initial unsynced state; wait for sync
        for _ in range(6):
            time.sleep(5)
            condition_synced = k8s.get_resource_condition(ref, "ACK.ResourceSynced")
            if condition_synced is not None and condition_synced["status"] == "True":
                break
        assert condition_synced is not None
        assert condition_synced["status"] == "True"

        # Verify the resource exists in AWS
        cr = k8s.get_resource(ref)
        aws_resp = redshift_client.describe_cluster_subnet_groups(
            ClusterSubnetGroupName=resource_name,
        )
        aws_groups = aws_resp["ClusterSubnetGroups"]
        assert len(aws_groups) == 1
        assert aws_groups[0]["ClusterSubnetGroupName"] == resource_name
        assert aws_groups[0]["Description"] == description

        # Verify status fields are populated
        assert cr["status"].get("subnetGroupStatus") is not None
        assert cr["status"].get("vpcID") is not None
        assert cr["status"].get("subnets") is not None
        assert len(cr["status"]["subnets"]) > 0

        # Verify tags in AWS
        aws_tags = {t["Key"]: t["Value"] for t in aws_groups[0].get("Tags", [])}
        assert "environment" in aws_tags
        assert aws_tags["environment"] == "test"

        # --- Update: change description ---
        new_description = "Updated ACK test cluster subnet group"
        updates = {
            "spec": {
                "description": new_description,
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(UPDATE_WAIT_AFTER_SECONDS)

        # Wait for sync after update
        for _ in range(6):
            time.sleep(5)
            condition_synced = k8s.get_resource_condition(ref, "ACK.ResourceSynced")
            if condition_synced is not None and condition_synced["status"] == "True":
                break
        assert condition_synced is not None
        assert condition_synced["status"] == "True"

        # Verify update in AWS
        aws_resp = redshift_client.describe_cluster_subnet_groups(
            ClusterSubnetGroupName=resource_name,
        )
        assert aws_resp["ClusterSubnetGroups"][0]["Description"] == new_description

        # --- Update: change tags ---
        tag_updates = {
            "spec": {
                "tags": [
                    {"key": "environment", "value": "staging"},
                    {"key": "team", "value": "platform"},
                ],
            },
        }
        k8s.patch_custom_resource(ref, tag_updates)
        time.sleep(UPDATE_WAIT_AFTER_SECONDS)

        # Wait for sync after tag update
        for _ in range(6):
            time.sleep(5)
            condition_synced = k8s.get_resource_condition(ref, "ACK.ResourceSynced")
            if condition_synced is not None and condition_synced["status"] == "True":
                break
        assert condition_synced is not None
        assert condition_synced["status"] == "True"

        # Verify tags in AWS
        aws_resp = redshift_client.describe_cluster_subnet_groups(
            ClusterSubnetGroupName=resource_name,
        )
        aws_tags = {
            t["Key"]: t["Value"]
            for t in aws_resp["ClusterSubnetGroups"][0].get("Tags", [])
        }
        assert aws_tags.get("environment") == "staging"
        assert aws_tags.get("team") == "platform"

        # --- Delete ---
        k8s.delete_custom_resource(ref)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)

        # Verify resource is deleted from Kubernetes
        assert not k8s.get_resource_exists(ref)

        # Verify resource is deleted from AWS
        try:
            redshift_client.describe_cluster_subnet_groups(
                ClusterSubnetGroupName=resource_name,
            )
            assert False, "Expected ClusterSubnetGroupNotFoundFault"
        except redshift_client.exceptions.ClusterSubnetGroupNotFoundFault:
            pass

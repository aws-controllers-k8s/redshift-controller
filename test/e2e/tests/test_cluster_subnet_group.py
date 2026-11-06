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

from acktest import tags
from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_redshift_resource
from e2e.bootstrap_resources import get_bootstrap_resources
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "clustersubnetgroups"

DELETE_WAIT_AFTER_SECONDS = 10
DELETE_WAIT_PERIODS = 3
DELETE_PERIOD_LENGTH = 10

# Creating a resource with tags requires a secondary sync, which the
# controller requeues after 30 seconds.
SYNC_WAIT_PERIODS = 12
SYNC_PERIOD_LENGTH = 5


@pytest.fixture(scope="module")
def redshift_client():
    import boto3
    return boto3.client("redshift")


@pytest.fixture
def cluster_subnet_group():
    subnet_ids = get_bootstrap_resources().ClusterSubnetGroupVPC.public_subnets.subnet_ids
    assert len(subnet_ids) > 1, "At least two bootstrapped subnets are needed for testing"
    resource_name = random_suffix_name("ack-test-csg", 24)
    description = "ACK test cluster subnet group"

    replacements = REPLACEMENT_VALUES.copy()
    replacements["CLUSTER_SUBNET_GROUP_NAME"] = resource_name
    replacements["DESCRIPTION"] = description
    replacements["SUBNET_ID_1"] = subnet_ids[0]

    resource_data = load_redshift_resource(
        "cluster_subnet_group",
        additional_replacements=replacements,
    )
    logging.info(f"Creating ClusterSubnetGroup {resource_name}")

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        resource_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    yield ref, cr, resource_name, description, subnet_ids

    # Clean up in case the test failed before its own delete step.
    if k8s.get_resource_exists(ref):
        _, deleted = k8s.delete_custom_resource(
            ref, DELETE_WAIT_PERIODS, DELETE_PERIOD_LENGTH,
        )
        assert deleted


def patch_and_wait_for_sync(ref, updates):
    """Patch the resource and wait for a reconcile that happened after the patch.

    ACK.ResourceSynced is usually still True from the previous reconcile right
    after a patch, so only accept a condition with a newer lastTransitionTime.
    """
    last_synced = condition.get_synced_last_transition_time(ref)
    k8s.patch_custom_resource(ref, updates)
    assert k8s.wait_on_condition_after(
        ref,
        condition.CONDITION_TYPE_RESOURCE_SYNCED,
        "True",
        last_transition_after=last_synced,
        wait_periods=SYNC_WAIT_PERIODS,
        period_length=SYNC_PERIOD_LENGTH,
    )


def describe_subnet_group(redshift_client, name):
    resp = redshift_client.describe_cluster_subnet_groups(
        ClusterSubnetGroupName=name,
    )
    groups = resp["ClusterSubnetGroups"]
    assert len(groups) == 1
    return groups[0]


@service_marker
@pytest.mark.canary
class TestClusterSubnetGroup:
    def test_create_update_delete(self, redshift_client, cluster_subnet_group):
        ref, cr, resource_name, description, subnet_ids = cluster_subnet_group
        subnet_id, second_subnet_id = subnet_ids[0], subnet_ids[1]

        assert cr is not None
        assert k8s.get_resource_exists(ref)

        assert k8s.wait_on_condition(
            ref,
            condition.CONDITION_TYPE_RESOURCE_SYNCED,
            "True",
            wait_periods=SYNC_WAIT_PERIODS,
            period_length=SYNC_PERIOD_LENGTH,
        )

        # Verify the resource exists in AWS
        cr = k8s.get_resource(ref)
        group = describe_subnet_group(redshift_client, resource_name)
        assert group["ClusterSubnetGroupName"] == resource_name
        assert group["Description"] == description

        # Verify status fields are populated
        assert cr["status"].get("status") is not None
        assert cr["status"].get("vpcID") is not None
        assert cr["status"].get("subnets") is not None
        assert len(cr["status"]["subnets"]) > 0

        # Verify the ARN is populated in status and recognized by Redshift
        arn = cr["status"]["ackResourceMetadata"].get("arn")
        assert arn is not None
        assert arn.startswith("arn:")
        assert ":redshift:" in arn
        assert arn.endswith(f":subnetgroup:{resource_name.lower()}")
        redshift_client.describe_tags(ResourceName=arn)

        # Verify tags in AWS
        tags.assert_equal_without_ack_tags(
            expected=[{"Key": "environment", "Value": "test"}],
            actual=group.get("Tags", []),
        )

        # --- Update: change description ---
        new_description = "Updated ACK test cluster subnet group"
        patch_and_wait_for_sync(ref, {"spec": {"description": new_description}})

        group = describe_subnet_group(redshift_client, resource_name)
        assert group["Description"] == new_description

        # --- Update: change tags ---
        patch_and_wait_for_sync(ref, {
            "spec": {
                "tags": [
                    {"key": "environment", "value": "staging"},
                    {"key": "team", "value": "platform"},
                ],
            },
        })

        group = describe_subnet_group(redshift_client, resource_name)
        tags.assert_equal_without_ack_tags(
            expected=[
                {"Key": "environment", "Value": "staging"},
                {"Key": "team", "Value": "platform"},
            ],
            actual=group.get("Tags", []),
        )

        # --- Update: remove a tag ---
        patch_and_wait_for_sync(ref, {
            "spec": {
                "tags": [
                    {"key": "team", "value": "platform"},
                ],
            },
        })

        group = describe_subnet_group(redshift_client, resource_name)
        tags.assert_equal_without_ack_tags(
            expected=[{"Key": "team", "Value": "platform"}],
            actual=group.get("Tags", []),
        )

        # --- Update: add a subnet ---
        patch_and_wait_for_sync(ref, {
            "spec": {
                "subnetIDs": [subnet_id, second_subnet_id],
            },
        })

        # Verify subnet update in AWS and in the CR
        group = describe_subnet_group(redshift_client, resource_name)
        aws_subnet_ids = {s["SubnetIdentifier"] for s in group["Subnets"]}
        assert aws_subnet_ids == {subnet_id, second_subnet_id}
        cr = k8s.get_resource(ref)
        assert set(cr["spec"]["subnetIDs"]) == {subnet_id, second_subnet_id}

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

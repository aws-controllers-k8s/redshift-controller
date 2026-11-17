"""Integration tests for the Redshift Cluster resource."""

import logging
import secrets
import time

import pytest

from acktest import tags
from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_redshift_resource
from e2e.bootstrap_resources import get_bootstrap_resources
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "clusters"
SUBNET_GROUP_PLURAL = "clustersubnetgroups"

# A cluster takes 5-15 minutes to become available.
CREATE_WAIT_PERIODS = 180
CREATE_PERIOD_LENGTH = 10
UPDATE_WAIT_PERIODS = 120
UPDATE_PERIOD_LENGTH = 10

DELETE_WAIT_PERIODS = 12
DELETE_PERIOD_LENGTH = 10
# The finalizer is removed once DeleteCluster is accepted; the cluster then
# spends a few more minutes in "deleting".
AWS_DELETE_WAITER_DELAY = 30
AWS_DELETE_WAITER_ATTEMPTS = 60


@pytest.fixture(scope="module")
def redshift_client():
    import boto3
    return boto3.client("redshift")


@pytest.fixture
def password_secret():
    name = random_suffix_name("ack-test-pw", 32)
    # Redshift requires 8-64 characters with an upper case letter, a lower
    # case letter and a digit.
    k8s.create_opaque_secret("default", name, "password", "Aa1" + secrets.token_hex(8))
    yield name
    k8s.delete_secret("default", name)


@pytest.fixture
def subnet_group():
    subnet_ids = get_bootstrap_resources().ClusterSubnetGroupVPC.public_subnets.subnet_ids
    assert len(subnet_ids) > 1, "At least two bootstrapped subnets are needed for testing"
    name = random_suffix_name("ack-test-cluster-sg", 32)

    replacements = REPLACEMENT_VALUES.copy()
    replacements["CLUSTER_SUBNET_GROUP_NAME"] = name
    replacements["DESCRIPTION"] = "ACK test cluster subnet group for Cluster"
    replacements["SUBNET_ID_1"] = subnet_ids[0]
    resource_data = load_redshift_resource(
        "cluster_subnet_group",
        additional_replacements=replacements,
    )
    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, SUBNET_GROUP_PLURAL, name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    k8s.wait_resource_consumed_by_controller(ref)
    assert k8s.wait_on_condition(
        ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True",
        wait_periods=12, period_length=5,
    )

    yield ref, name

    if k8s.get_resource_exists(ref):
        _, deleted = k8s.delete_custom_resource(ref, DELETE_WAIT_PERIODS, DELETE_PERIOD_LENGTH)
        assert deleted


@pytest.fixture
def cluster(redshift_client, subnet_group, password_secret):
    _, subnet_group_name = subnet_group
    name = random_suffix_name("ack-test-cluster", 32)

    replacements = REPLACEMENT_VALUES.copy()
    replacements["CLUSTER_NAME"] = name
    replacements["CLUSTER_SUBNET_GROUP_NAME"] = subnet_group_name
    replacements["PASSWORD_SECRET_NAME"] = password_secret
    resource_data = load_redshift_resource("cluster", additional_replacements=replacements)
    logging.info(f"Creating Cluster {name}")

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL, name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    yield ref, cr, name, subnet_group_name

    # Teardown order matters: the subnet group cannot be deleted while the
    # cluster still exists, so wait for AWS before the dependent fixtures run.
    if k8s.get_resource_exists(ref):
        k8s.delete_custom_resource(ref, DELETE_WAIT_PERIODS, DELETE_PERIOD_LENGTH)
    wait_for_aws_cluster_deleted(redshift_client, name)


def wait_for_aws_cluster_deleted(redshift_client, name):
    # Safe here: DeleteCluster was already accepted, so the cluster is
    # "deleting" and not "available" (which the waiter treats as a failure).
    redshift_client.get_waiter("cluster_deleted").wait(
        ClusterIdentifier=name,
        WaiterConfig={
            "Delay": AWS_DELETE_WAITER_DELAY,
            "MaxAttempts": AWS_DELETE_WAITER_ATTEMPTS,
        },
    )


def patch_and_wait_for_sync(ref, updates):
    """Patch the resource and wait for a reconcile that happened after the patch."""
    last_synced = condition.get_synced_last_transition_time(ref)
    k8s.patch_custom_resource(ref, updates)
    assert k8s.wait_on_condition_after(
        ref,
        condition.CONDITION_TYPE_RESOURCE_SYNCED,
        "True",
        last_transition_after=last_synced,
        wait_periods=UPDATE_WAIT_PERIODS,
        period_length=UPDATE_PERIOD_LENGTH,
    )


def describe_cluster(redshift_client, name):
    resp = redshift_client.describe_clusters(ClusterIdentifier=name)
    assert len(resp["Clusters"]) == 1
    return resp["Clusters"][0]


@service_marker
@pytest.mark.canary
class TestCluster:
    def test_create_update_delete(self, redshift_client, cluster):
        ref, cr, name, subnet_group_name = cluster
        assert cr is not None
        assert k8s.get_resource_exists(ref)

        assert k8s.wait_on_condition(
            ref,
            condition.CONDITION_TYPE_RESOURCE_SYNCED,
            "True",
            wait_periods=CREATE_WAIT_PERIODS,
            period_length=CREATE_PERIOD_LENGTH,
        )
        condition.assert_type_status(ref, condition.CONDITION_TYPE_REFERENCES_RESOLVED)

        cr = k8s.get_resource(ref)
        assert cr["spec"]["identifier"] == name
        assert cr["status"]["status"] == "available"
        arn = cr["status"]["ackResourceMetadata"].get("arn")
        assert arn is not None
        assert arn.endswith(f":cluster:{name}")
        # The runtime does not write the resolved reference back to
        # spec.clusterSubnetGroupName, so assert the reference and AWS instead.
        assert cr["spec"]["clusterSubnetGroupRef"]["from"]["name"] == subnet_group_name

        aws_cluster = describe_cluster(redshift_client, name)
        assert aws_cluster["ClusterSubnetGroupName"] == subnet_group_name
        tags.assert_equal_without_ack_tags(
            expected=[{"Key": "environment", "Value": "test"}],
            actual=aws_cluster.get("Tags", []),
        )
        redshift_client.describe_tags(ResourceName=arn)

        # Regression guard: AvailabilityZone is assigned by AWS after create
        # and late-initialized; an empty ModifyCluster must not block that.
        for _ in range(6):
            cr = k8s.get_resource(ref)
            if cr["spec"].get("availabilityZone") == aws_cluster["AvailabilityZone"]:
                break
            time.sleep(10)
        assert cr["spec"].get("availabilityZone") == aws_cluster["AvailabilityZone"]

        # --- Update: retention period ---
        patch_and_wait_for_sync(ref, {"spec": {"automatedSnapshotRetentionPeriod": 2}})
        aws_cluster = describe_cluster(redshift_client, name)
        assert aws_cluster["AutomatedSnapshotRetentionPeriod"] == 2
        cr = k8s.get_resource(ref)
        assert cr["spec"]["automatedSnapshotRetentionPeriod"] == 2

        # --- Update: change tags ---
        patch_and_wait_for_sync(ref, {
            "spec": {
                "tags": [
                    {"key": "environment", "value": "staging"},
                    {"key": "team", "value": "platform"},
                ],
            },
        })
        aws_cluster = describe_cluster(redshift_client, name)
        tags.assert_equal_without_ack_tags(
            expected=[
                {"Key": "environment", "Value": "staging"},
                {"Key": "team", "Value": "platform"},
            ],
            actual=aws_cluster.get("Tags", []),
        )

        # --- Update: remove a tag ---
        patch_and_wait_for_sync(ref, {"spec": {"tags": [{"key": "team", "value": "platform"}]}})
        aws_cluster = describe_cluster(redshift_client, name)
        tags.assert_equal_without_ack_tags(
            expected=[{"Key": "team", "Value": "platform"}],
            actual=aws_cluster.get("Tags", []),
        )

        # --- Delete ---
        # The skip-final-snapshot annotation is set in the resource template.
        _, deleted = k8s.delete_custom_resource(ref, DELETE_WAIT_PERIODS, DELETE_PERIOD_LENGTH)
        assert deleted
        assert not k8s.get_resource_exists(ref)
        wait_for_aws_cluster_deleted(redshift_client, name)
        with pytest.raises(redshift_client.exceptions.ClusterNotFoundFault):
            describe_cluster(redshift_client, name)

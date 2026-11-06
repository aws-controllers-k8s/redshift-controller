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

"""Integration tests for the Redshift ClusterParameterGroup resource."""

import json
import logging
import time

import pytest
from botocore.exceptions import ClientError

from acktest import tags as tagutil
from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_redshift_resource
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "clusterparametergroups"

MODIFY_WAIT_AFTER_SECONDS = 10
DELETE_WAIT_AFTER_SECONDS = 10
SYNC_WAIT_PERIODS = 12
SYNC_PERIOD_LENGTH = 10

STATEMENT_TIMEOUT = "statement_timeout"
USER_ACTIVITY_LOGGING = "enable_user_activity_logging"
WLM_JSON_CONFIGURATION = "wlm_json_configuration"
# One manual WLM queue with a concurrency level of five.
WLM_VALUE = '[{"query_concurrency":5}]'
STABILITY_WAIT_SECONDS = 30


FAMILY_CANDIDATES = ("redshift-2.0", "redshift-1.0")


def get_family(redshift_client):
    """Return the first candidate family AWS has default parameters for.

    The account's default.* groups are not a reliable source: an account that
    has never created a cluster may list none, so ask
    DescribeDefaultClusterParameters directly instead."""
    for family in FAMILY_CANDIDATES:
        try:
            resp = redshift_client.describe_default_cluster_parameters(
                ParameterGroupFamily=family, MaxRecords=20
            )
            if resp["DefaultClusterParameters"].get("Parameters"):
                return family
        except ClientError as e:
            logging.info("family %s unavailable: %s", family, e)
    pytest.fail(f"none of {FAMILY_CANDIDATES} has default parameters")


def get_default_parameters(redshift_client, family):
    defaults = {}
    marker = None
    while True:
        kwargs = {"ParameterGroupFamily": family}
        if marker:
            kwargs["Marker"] = marker
        resp = redshift_client.describe_default_cluster_parameters(**kwargs)
        page = resp["DefaultClusterParameters"]
        for p in page.get("Parameters", []):
            defaults[p["ParameterName"]] = p
        marker = page.get("Marker")
        if not marker:
            break
    return defaults


def get_aws_parameters(redshift_client, name):
    params = {}
    marker = None
    while True:
        kwargs = {"ParameterGroupName": name}
        if marker:
            kwargs["Marker"] = marker
        resp = redshift_client.describe_cluster_parameters(**kwargs)
        for p in resp.get("Parameters", []):
            params[p["ParameterName"]] = p
        marker = resp.get("Marker")
        if not marker:
            break
    return params


def get_aws_group(redshift_client, name):
    resp = redshift_client.describe_cluster_parameter_groups(
        ParameterGroupName=name,
    )
    assert len(resp["ParameterGroups"]) == 1
    return resp["ParameterGroups"][0]


def pick_different(default, *candidates):
    return next(c for c in candidates if c != default)


def status_by_name(cr):
    statuses = cr.get("status", {}).get("parameterOverrideStatuses") or []
    return {s["parameterName"]: s for s in statuses}


def assert_synced(ref):
    assert k8s.wait_on_condition(
        ref, "ACK.ResourceSynced", "True",
        wait_periods=SYNC_WAIT_PERIODS, period_length=SYNC_PERIOD_LENGTH,
    )


def patch_and_assert_synced(ref, patch):
    # Wait for a Synced=True written by a reconcile that ran after the patch,
    # not one left over from the previous generation.
    before = condition.get_synced_last_transition_time(ref)
    k8s.patch_custom_resource(ref, patch)
    time.sleep(MODIFY_WAIT_AFTER_SECONDS)
    assert k8s.wait_on_condition_after(
        ref, "ACK.ResourceSynced", "True",
        last_transition_after=before,
        wait_periods=SYNC_WAIT_PERIODS, period_length=SYNC_PERIOD_LENGTH,
    )


def make_parameter_group(redshift_client, spec_name=None):
    """Build the CR for a new parameter group and the values the test checks.

    spec_name overrides spec.name (metadata.name must stay lower case)."""
    family = get_family(redshift_client)
    defaults = get_default_parameters(redshift_client, family)
    for p in (STATEMENT_TIMEOUT, USER_ACTIVITY_LOGGING, WLM_JSON_CONFIGURATION):
        assert p in defaults, f"{p} missing from {family} defaults"
        assert defaults[p]["IsModifiable"], f"{p} is not modifiable"

    timeout_default = defaults[STATEMENT_TIMEOUT].get("ParameterValue")
    logging_default = defaults[USER_ACTIVITY_LOGGING].get("ParameterValue")
    values = {
        "family": family,
        "defaults": defaults,
        "description": "ACK test cluster parameter group",
        "timeout_1": pick_different(timeout_default, "30000", "45000"),
        "timeout_2": pick_different(timeout_default, "60000", "75000"),
        "logging_default": logging_default,
        "logging_value": "true" if logging_default == "false" else "false",
        "wlm_value": pick_different(
            defaults[WLM_JSON_CONFIGURATION].get("ParameterValue"), WLM_VALUE,
        ),
    }

    resource_name = random_suffix_name("ack-test-cpg", 24)
    replacements = REPLACEMENT_VALUES.copy()
    replacements["CLUSTER_PARAMETER_GROUP_NAME"] = resource_name
    replacements["FAMILY"] = family
    replacements["DESCRIPTION"] = values["description"]
    replacements["PARAM_STATEMENT_TIMEOUT"] = values["timeout_1"]
    replacements["PARAM_ENABLE_USER_ACTIVITY_LOGGING"] = values["logging_value"]
    resource_data = load_redshift_resource(
        "cluster_parameter_group",
        additional_replacements=replacements,
    )
    if spec_name is not None:
        resource_data["spec"]["name"] = spec_name
    values["aws_name"] = resource_data["spec"]["name"].lower()

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        resource_name, namespace="default",
    )
    return ref, resource_data, values


def create_parameter_group(ref, resource_data):
    logging.info(f"Creating ClusterParameterGroup {resource_data['spec']['name']}")
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)
    assert cr is not None
    assert k8s.get_resource_exists(ref)


def cleanup_parameter_group(ref):
    # Clean up in case the test failed before its own delete step.
    if k8s.get_resource_exists(ref):
        _, deleted = k8s.delete_custom_resource(
            ref, wait_periods=SYNC_WAIT_PERIODS, period_length=5,
        )
        assert deleted


def delete_and_assert_gone(redshift_client, ref, aws_name):
    _, deleted = k8s.delete_custom_resource(
        ref, wait_periods=SYNC_WAIT_PERIODS, period_length=5,
    )
    assert deleted
    time.sleep(DELETE_WAIT_AFTER_SECONDS)
    assert not k8s.get_resource_exists(ref)
    with pytest.raises(ClientError) as exc:
        redshift_client.describe_cluster_parameter_groups(
            ParameterGroupName=aws_name,
        )
    assert exc.value.response["Error"]["Code"] == "ClusterParameterGroupNotFound"


def assert_aws_tags(redshift_client, aws_name, expected):
    # assert_equal_without_ack_tags drops services.k8s.aws/* tags. The other
    # helm resourceTags defaults (app.kubernetes.io/managed-by,
    # kro.run/kro-version) are filled from CR labels and skipped when empty;
    # these CRs carry no such labels.
    group = get_aws_group(redshift_client, aws_name)
    tagutil.assert_equal_without_ack_tags(
        expected,
        {t["Key"]: t["Value"] for t in group.get("Tags", [])},
    )


@pytest.fixture
def parameter_group(redshift_client):
    ref, resource_data, values = make_parameter_group(redshift_client)
    try:
        create_parameter_group(ref, resource_data)
        yield ref, values
    finally:
        cleanup_parameter_group(ref)


@pytest.fixture
def upper_case_parameter_group(redshift_client):
    # Redshift stores the name in lower case but matches it case-insensitively
    # on Describe, so an upper-case spec.name is accepted as written.
    name = random_suffix_name("ACK-Test-CPG-UC", 24)
    ref, resource_data, values = make_parameter_group(redshift_client, spec_name=name)
    try:
        create_parameter_group(ref, resource_data)
        yield ref, values
    finally:
        cleanup_parameter_group(ref)


@service_marker
@pytest.mark.canary
class TestClusterParameterGroup:
    def test_create_update_delete(self, redshift_client, parameter_group):
        ref, v = parameter_group
        name = v["aws_name"]

        # --- Create: description, parameters and tags reach AWS ---
        assert_synced(ref)
        group = get_aws_group(redshift_client, name)
        assert group["ParameterGroupFamily"] == v["family"]
        assert group["Description"] == v["description"]
        assert_aws_tags(redshift_client, name, {"environment": "test", "team": "data"})
        params = get_aws_parameters(redshift_client, name)
        assert params[STATEMENT_TIMEOUT]["ParameterValue"] == v["timeout_1"]
        assert params[STATEMENT_TIMEOUT]["Source"] == "user"
        assert params[USER_ACTIVITY_LOGGING]["ParameterValue"] == v["logging_value"]
        assert params[USER_ACTIVITY_LOGGING]["Source"] == "user"
        cr = k8s.get_resource(ref)
        assert cr["status"]["ackResourceMetadata"]["arn"].endswith(
            f":parametergroup:{name}"
        )
        statuses = status_by_name(cr)
        assert set(statuses) == {STATEMENT_TIMEOUT, USER_ACTIVITY_LOGGING}
        assert statuses[STATEMENT_TIMEOUT]["parameterValue"] == v["timeout_1"]
        assert statuses[USER_ACTIVITY_LOGGING]["parameterValue"] == v["logging_value"]
        for s in statuses.values():
            assert s["source"] == "user"
            assert s["isModifiable"] is True
            assert s["applyType"] in {"static", "dynamic"}

        # --- Update parameters: change one, remove the other ---
        # A merge-patch null deletes the map key, which must reset that
        # parameter alone and leave the changed one in place.
        patch_and_assert_synced(ref, {
            "spec": {
                "parameterOverrides": {
                    STATEMENT_TIMEOUT: v["timeout_2"],
                    USER_ACTIVITY_LOGGING: None,
                },
            },
        })
        params = get_aws_parameters(redshift_client, name)
        assert params[STATEMENT_TIMEOUT]["ParameterValue"] == v["timeout_2"]
        assert params[STATEMENT_TIMEOUT]["Source"] == "user"
        assert params[USER_ACTIVITY_LOGGING]["ParameterValue"] == v["logging_default"]
        assert params[USER_ACTIVITY_LOGGING]["Source"] == "engine-default"
        # Without the post-sync requeue this would read the pre-update
        # status (rds-controller#298).
        statuses = status_by_name(k8s.get_resource(ref))
        assert statuses[STATEMENT_TIMEOUT]["parameterValue"] == v["timeout_2"]
        assert USER_ACTIVITY_LOGGING not in statuses

        # --- WLM JSON: AWS may re-serialize the body, which would make the
        # string comparison loop instead of settling ---
        wlm_value = v["wlm_value"]
        patch_and_assert_synced(ref, {
            "spec": {"parameterOverrides": {WLM_JSON_CONFIGURATION: wlm_value}},
        })
        params = get_aws_parameters(redshift_client, name)
        observed = params[WLM_JSON_CONFIGURATION]["ParameterValue"]
        assert json.loads(observed) == json.loads(wlm_value)
        assert params[WLM_JSON_CONFIGURATION]["Source"] == "user"
        logging.info(
            f"wlm_json_configuration returned byte-identical: {observed == wlm_value}"
        )
        # Every reconcile clears conditions and stamps Synced with the current
        # time, so an unchanged lastTransitionTime means no reconcile ran,
        # i.e. no perpetual diff.
        settled = condition.get_synced_last_transition_time(ref)
        time.sleep(STABILITY_WAIT_SECONDS)
        assert k8s.wait_on_condition(
            ref, "ACK.ResourceSynced", "True", wait_periods=1, period_length=1,
        )
        assert condition.get_synced_last_transition_time(ref) == settled
        statuses = status_by_name(k8s.get_resource(ref))
        assert statuses[WLM_JSON_CONFIGURATION]["source"] == "user"

        # --- Update tags: change, remove and add ---
        patch_and_assert_synced(ref, {
            "spec": {
                "tags": [
                    {"key": "environment", "value": "staging"},
                    {"key": "owner", "value": "ack"},
                ],
            },
        })
        assert_aws_tags(redshift_client, name, {"environment": "staging", "owner": "ack"})

        # --- Description is create-only: an edit settles without effect ---
        patch_and_assert_synced(ref, {
            "spec": {"description": "an edited description"},
        })
        group = get_aws_group(redshift_client, name)
        assert group["Description"] == v["description"]

        # --- Delete ---
        delete_and_assert_gone(redshift_client, ref, name)

    def test_upper_case_name(self, redshift_client, upper_case_parameter_group):
        ref, v = upper_case_parameter_group
        name = v["aws_name"]

        # The group is created under the lower-case name, and spec.name keeps
        # the user's value rather than being overwritten by the API response.
        assert_synced(ref)
        cr = k8s.get_resource(ref)
        assert cr["spec"]["name"] != name
        assert cr["spec"]["name"].lower() == name
        assert get_aws_group(redshift_client, name)["ParameterGroupName"] == name
        # The ARN uses the lower-case name, which the tag APIs require.
        assert cr["status"]["ackResourceMetadata"]["arn"].endswith(
            f":parametergroup:{name}"
        )
        params = get_aws_parameters(redshift_client, name)
        assert params[STATEMENT_TIMEOUT]["ParameterValue"] == v["timeout_1"]

        # Parameter and tag updates both address the group by name or ARN.
        patch_and_assert_synced(ref, {
            "spec": {
                "parameterOverrides": {
                    STATEMENT_TIMEOUT: v["timeout_2"],
                    USER_ACTIVITY_LOGGING: None,
                },
                "tags": [{"key": "environment", "value": "staging"}],
            },
        })
        params = get_aws_parameters(redshift_client, name)
        assert params[STATEMENT_TIMEOUT]["ParameterValue"] == v["timeout_2"]
        assert params[USER_ACTIVITY_LOGGING]["Source"] == "engine-default"
        assert USER_ACTIVITY_LOGGING not in status_by_name(k8s.get_resource(ref))
        assert_aws_tags(redshift_client, name, {"environment": "staging"})

        delete_and_assert_gone(redshift_client, ref, name)

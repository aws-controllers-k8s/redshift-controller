// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package cluster_subnet_group

import (
	"context"
	"fmt"
	"strings"

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
)

// subnetGroupARN returns the ARN of the cluster subnet group with the
// given name. The Redshift DescribeClusterSubnetGroups API does not return
// an ARN, so it is constructed from the resource manager's partition, region
// and account. Redshift stores subnet group names in lowercase, so the name
// is lowercased to match the ARN the service uses.
func (rm *resourceManager) subnetGroupARN(name string) string {
	return fmt.Sprintf("arn:%s:redshift:%s:%s:subnetgroup:%s",
		rm.awsPartition, rm.awsRegion, rm.awsAccountID, strings.ToLower(name))
}

// syncTags reconciles the Tags field for a ClusterSubnetGroup resource
// by computing the diff between desired and latest tags, then calling
// the Redshift CreateTags/DeleteTags APIs as needed.
//
// This method is called by the generated sdkUpdate code when the
// custom_sync configuration detects a difference in Spec.Tags.
func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTags")
	defer func() {
		exit(err)
	}()

	// The ARN is set on Status.ACKResourceMetadata by sdkFind
	// (see templates/hooks/cluster_subnet_group/sdk_read_many_post_set_output.go.tpl).
	if latest.ko.Status.ACKResourceMetadata == nil || latest.ko.Status.ACKResourceMetadata.ARN == nil {
		return fmt.Errorf("cannot sync tags: resource ARN is not available")
	}
	arn := string(*latest.ko.Status.ACKResourceMetadata.ARN)

	desiredTags, _ := convertToOrderedACKTags(desired.ko.Spec.Tags)
	latestTags, _ := convertToOrderedACKTags(latest.ko.Spec.Tags)

	// Compute tags to add or update: tags that are in desired but not in
	// latest, or tags whose values have changed.
	var tagsToAdd []svcsdktypes.Tag
	for k, v := range desiredTags {
		latestVal, exists := latestTags[k]
		if !exists || latestVal != v {
			tagKey := k
			tagVal := v
			tagsToAdd = append(tagsToAdd, svcsdktypes.Tag{
				Key:   &tagKey,
				Value: &tagVal,
			})
		}
	}

	// Compute tags to remove: tags that are in latest but not in desired.
	var tagKeysToRemove []string
	for k := range latestTags {
		if _, exists := desiredTags[k]; !exists {
			tagKeysToRemove = append(tagKeysToRemove, k)
		}
	}

	// Remove tags that are no longer desired
	if len(tagKeysToRemove) > 0 {
		_, err = rm.sdkapi.DeleteTags(ctx, &svcsdk.DeleteTagsInput{
			ResourceName: &arn,
			TagKeys:      tagKeysToRemove,
		})
		rm.metrics.RecordAPICall("UPDATE", "DeleteTags", err)
		if err != nil {
			return err
		}
	}

	// Add or update tags
	if len(tagsToAdd) > 0 {
		_, err = rm.sdkapi.CreateTags(ctx, &svcsdk.CreateTagsInput{
			ResourceName: &arn,
			Tags:         tagsToAdd,
		})
		rm.metrics.RecordAPICall("UPDATE", "CreateTags", err)
		if err != nil {
			return err
		}
	}

	return nil
}

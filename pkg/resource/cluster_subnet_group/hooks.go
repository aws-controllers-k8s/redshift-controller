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

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
)

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

	// The Redshift DescribeClusterSubnetGroups API does not return an ARN,
	// so ACKResourceMetadata.ARN is never populated by the generated code.
	// We construct the ARN from the resource metadata (account, region,
	// partition) and the subnet group name.
	var arn string
	if desired.ko.Status.ACKResourceMetadata != nil && desired.ko.Status.ACKResourceMetadata.ARN != nil {
		arn = string(*desired.ko.Status.ACKResourceMetadata.ARN)
	} else {
		// Build the ARN: arn:{partition}:redshift:{region}:{account}:subnetgroup:{name}
		md := desired.ko.Status.ACKResourceMetadata
		if md == nil {
			md = latest.ko.Status.ACKResourceMetadata
		}
		if md == nil || md.OwnerAccountID == nil || md.Region == nil || md.Partition == nil {
			return fmt.Errorf("cannot sync tags: resource metadata is not available")
		}
		if desired.ko.Spec.Name == nil {
			return fmt.Errorf("cannot sync tags: resource name is not available")
		}
		arn = fmt.Sprintf("arn:%s:redshift:%s:%s:subnetgroup:%s",
			*md.Partition, *md.Region, *md.OwnerAccountID, *desired.ko.Spec.Name)
	}

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

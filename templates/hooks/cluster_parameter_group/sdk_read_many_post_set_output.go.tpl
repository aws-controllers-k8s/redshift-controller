	// DescribeClusterParameterGroups does not return an ARN, so build it from
	// the group name (lowercased, as stored by Redshift).
	if ko.Spec.Name != nil {
		if ko.Status.ACKResourceMetadata == nil {
			ko.Status.ACKResourceMetadata = &ackv1alpha1.ResourceMetadata{}
		}
		arn := ackv1alpha1.AWSResourceName(rm.parameterGroupARN(*ko.Spec.Name))
		ko.Status.ACKResourceMetadata.ARN = &arn
	}
	// DescribeClusterParameterGroups does not return parameter values, so read
	// them with DescribeClusterParameters.
	overrides, statuses, err := rm.getParameterOverrides(ctx, ko.Spec.Name, r.ko.Spec.ParameterOverrides)
	if err != nil {
		return nil, err
	}
	ko.Spec.ParameterOverrides = overrides
	ko.Status.ParameterOverrideStatuses = statuses

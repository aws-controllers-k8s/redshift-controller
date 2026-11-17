	// CreateCluster returns no cluster ARN, so build it for syncTags.
	if ko.Spec.Identifier != nil {
		arn := ackv1alpha1.AWSResourceName(rm.clusterARN(*ko.Spec.Identifier))
		ko.Status.ACKResourceMetadata.ARN = &arn
	}

	subnets := make([]*string, 0, len(ko.Status.Subnets))
	for _, subnetIdIter := range ko.Status.Subnets {
		if subnetIdIter.SubnetIdentifier != nil {
			subnets = append(subnets, subnetIdIter.SubnetIdentifier)
		}
	}
	ko.Spec.SubnetIDs = subnets
	// DescribeClusterSubnetGroups does not return an ARN, so build it from
	// the subnet group name (lowercased, as stored by Redshift).
	if ko.Spec.Name != nil {
		arn := ackv1alpha1.AWSResourceName(rm.subnetGroupARN(*ko.Spec.Name))
		ko.Status.ACKResourceMetadata.ARN = &arn
	}

	if err = applyResizeCoupling(input, desired.ko, latest.ko, delta); err != nil {
		return nil, err
	}
	// updatedDesired exists because Cluster has custom_sync fields. A delta
	// that only flags a nil-vs-assigned late-initialized field (AvailabilityZone)
	// leaves the request identifier-only, which AWS rejects.
	if modifyClusterInputIsEmpty(input) {
		return rm.concreteResource(updatedDesired), nil
	}

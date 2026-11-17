	// ModifyCluster returns current values and reports requested ones as
	// pending, so keep the user's declared Spec rather than reverting it.
	desired.ko.Spec.DeepCopyInto(&ko.Spec)

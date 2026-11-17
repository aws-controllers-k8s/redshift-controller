	if err = setDeleteOptions(r.ko.GetAnnotations(), input); err != nil {
		return nil, err
	}

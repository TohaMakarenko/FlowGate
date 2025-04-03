package main

func Map[TIn any, TOut any](arr []TIn, selector func(val TIn) TOut) []TOut {
	output := []TOut{}
	for i := range arr {
		out := selector(arr[i])
		output = append(output, out)
	}
	return output
}

func Find[TIn any](arr []TIn, predicate func(val TIn) bool) *TIn {
	for i := range arr {
		el := &arr[i]
		if predicate(*el) {
			return el
		}
	}
	return nil
}

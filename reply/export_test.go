package reply

// Retained returns how many results the collector of the Publish that sent r still holds.
func Retained[Q, R any](r Request[Q, R]) int {
	var n int
	if r.to != nil {
		r.to.results.Range(func(any, any) bool {
			n++

			return true
		})
	}

	return n
}

package types

import "errors"

var (
	// StreamsListIsEmpty is returned when no streams are available in the balancer.
	StreamsListIsEmpty = errors.New("Streams list is empty.")
)

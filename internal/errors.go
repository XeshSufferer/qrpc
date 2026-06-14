package internal

import (
	"errors"
)

var (
	// ClientNotConnectedError is returned when the client is not connected to a host.
	ClientNotConnectedError = errors.New("QRpc Client not connected to the host.")
	// StreamsListIsEmpty is returned when no streams are available in the balancer.
	StreamsListIsEmpty = errors.New("Streams list is empty.")
)

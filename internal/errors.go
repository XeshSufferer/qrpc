package internal

import (
	"errors"
)

var (
	ClientNotConnectedError = errors.New("QRpc Client not connected to the host.")
	StreamsListIsEmpty      = errors.New("Streams list is empty.")
)

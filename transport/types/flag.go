package types

var (
	// REQUEST_FLAG marks a request frame.
	REQUEST_FLAG = byte(1)
	// RESPONSE_FLAG marks a response frame.
	RESPONSE_FLAG = byte(2)
	// EVENT_FLAG marks a one-way event frame.
	EVENT_FLAG = byte(3)
	// REQUEST_ZSTD_FLAG marks a zstd-compressed request frame.
	REQUEST_ZSTD_FLAG = byte(4)
	// RESPONSE_ZSTD_FLAG marks a zstd-compressed response frame.
	RESPONSE_ZSTD_FLAG = byte(5)
	// EVENT_ZSTD_FLAG marks a zstd-compressed event frame.
	EVENT_ZSTD_FLAG = byte(6)
)

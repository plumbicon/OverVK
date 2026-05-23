package overvk

type SendItem struct {
	SessionID string
	Sequence  int
	Data      []byte
	Target    Target
}

type Packet struct {
	Sequence int
	Data     []byte
	Close    bool
}

type DocumentAttachment struct {
	URL string
}

type ParsedMessage struct {
	PeerID     int
	Headers    map[string]string
	Payload    string
	Attachment *DocumentAttachment
}

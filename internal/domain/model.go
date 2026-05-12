package domain

type InboxStatus string

const (
	StatusReceived  InboxStatus = "RECEIVED"
	StatusProcessed InboxStatus = "PROCESSED"
	StatusFailed    InboxStatus = "FAILED"
)

type TransferMessage struct {
	Amount      int    `json:"amount"`
	ToAccount   string `json:"to_account"`
	TransferID  string `json:"transfer_id"`
	FromAccount string `json:"from_account"`
}

type InboxRecord struct {
	ID         int64
	TransferID string
	Status     InboxStatus
	Payload    []byte
}

type ProcessedPayment struct {
	TransferID  string
	Amount      int
	FromAccount string
	ToAccount   string
}

type DeadLetterMessage struct {
	TransferID string `json:"transfer_id"`
	Payload    string `json:"payload"`
	Error      string `json:"error"`
	ErrorType  string `json:"error_type"`
	Topic      string `json:"topic"`
	Timestamp  int64  `json:"timestamp"`
}

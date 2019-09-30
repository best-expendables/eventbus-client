package eventbusclient

type RetryError error

func NewRetryError(err error) RetryError {
	return RetryError(err)
}

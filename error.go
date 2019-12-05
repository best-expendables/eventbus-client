package eventbusclient

type RetryErrorType interface {
	IsRetryErrorType() bool
	Error() string
}

type retryError struct {
	err error
}

func (r retryError) IsRetryErrorType() bool {
	return true
}

func (r retryError) Error() string {
	return r.err.Error()
}

func NewRetryError(err error) RetryErrorType {
	return retryError{err: err}
}

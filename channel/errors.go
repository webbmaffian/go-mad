package channel

type channelError string

var _ error = channelError("")

func (err channelError) Error() string {
	return string(err)
}

const ErrEmpty = channelError("channel is empty")
const ErrClosed = channelError("channel is closed")
const ErrWritingClosed = channelError("channel is closed for writing")

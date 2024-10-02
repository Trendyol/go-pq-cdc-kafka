package kafka

type SinkResponseHandlerContext struct {
	Message *Event
	Err     error
}

type SinkResponseHandler interface {
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
}

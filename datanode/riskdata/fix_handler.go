package riskdata

type fixHandler struct {
	name   string
	handle func() FixResult
}
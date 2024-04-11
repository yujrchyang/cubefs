package riskdata

type FixerStatus struct {
	Fragments []*Fragment
	Count     int
	Running   bool
}
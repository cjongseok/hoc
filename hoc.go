package hoc

// Higher-Order Channel

// nil is not allowed to the channel,
// since nil is used as stop signal to filter
type Filtered <-chan interface{}
type Filter func(in <-chan interface{}, out Filtered)
func FilterChannel(in <-chan interface{}, f Filter) Filtered {
	out := make(Filtered)
	go f(in, out)
	return out
}
func FilterDemuxed(f Filter, in Demuxed) Demuxed {
	out := make(Demuxed)
	for id, _ := range in {
		out[id] = make(chan interface{})
		go f(in[id], out[id])
	}
	return out
}

type Demuxed map[int32]<-chan interface{}
type Demuxer func(in <-chan interface{}, out Demuxed)
func Demultiplex(in <-chan interface{}, d Demuxer, ids []int32) Demuxed {
	out := make(Demuxed)
	for _, id := range ids {
		out[id] = make(chan interface{})
	}
	go d(in, out)
	return out
}

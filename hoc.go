package hoc

// Higher-Order Channel

// nil is not allowed to the channel,
// since nil is used as stop signal to filter
//type Filtered <-chan interface{}
type Filter func(in chan interface{}, out chan interface{})
func FilterChannel(in chan interface{}, f Filter) <-chan interface{} {
	out := make(chan interface{})
	go f(in, out)
	return out
}
func FilterDemuxed(in map[int32]chan interface{}, f Filter) map[int32]chan interface{} {
	out := make(map[int32]chan interface{})
	for id, _ := range in {
		out[id] = make(chan interface{})
		go f(in[id], out[id])
	}
	return out
}

//type Demuxed map[int32]<-chan interface{}
type Demuxer func(in chan interface{}, out map[int32]chan interface{})
func Demultiplex(in chan interface{}, d Demuxer, ids []int32) map[int32]chan interface{} {
	out := make(map[int32]chan interface{})
	for _, id := range ids {
		out[id] = make(chan interface{})
	}
	go d(in, out)
	return out
}

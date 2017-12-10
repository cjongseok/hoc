package hoc

import (
	"sync"
)

// Higher-Order Channel

// nil means End-Of-Channel(EOC)
// EOC on a channel stops subordinate HOCs

//type Filtered <-chan interface{}
type Filter func(in chan interface{}, out chan interface{}, wg *sync.WaitGroup)
func FilterChannel(in chan interface{}, f Filter, wg *sync.WaitGroup) chan interface{} {
	out := make(chan interface{})
	go f(in, out, wg)
	return out
}
func FilterDemuxed(in map[int32]chan interface{}, f Filter, wg *sync.WaitGroup) map[int32]chan interface{} {
	out := make(map[int32]chan interface{})
	for id, _ := range in {
		out[id] = make(chan interface{})
		go f(in[id], out[id], wg)
	}
	return out
}
func FilterDemuxedRespectively(in map[int32]chan interface{}, filterMap map[int32]Filter, wg *sync.WaitGroup) map[int32]chan interface{} {
	out := make(map[int32]chan interface{})
	for id, _ := range in {
		out[id] = make(chan interface{})
		go filterMap[id](in[id], out[id], wg)
	}
	return out
}

type Demuxer func(in chan interface{}, out map[int32]chan interface{}, wg *sync.WaitGroup)
func Demultiplex(in chan interface{}, d Demuxer, ids []int32, wg *sync.WaitGroup) map[int32]chan interface{} {
	out := make(map[int32]chan interface{})
	for _, id := range ids {
		out[id] = make(chan interface{})
	}
	go d(in, out, wg)
	return out
}

type Muxer func(in map[int32]chan interface{}, out chan interface{}, wg *sync.WaitGroup)
func Multiplex(in map[int32]chan interface{}, m Muxer, wg *sync.WaitGroup) chan interface{} {
	out := make(chan interface{})
	go m(in, out, wg)
	return out
}
func MergeMuxer (in map[int32]chan interface{}, out chan interface{}, wg *sync.WaitGroup) {
	for _, ch := range in {
		go func(c chan interface{}) {
			for {
				select {
				case x := <-c:
					if x == nil {
						return
					}
					out <- x
				}
			}
		}(ch)
	}
}

type Pipeline struct {
	In 		chan interface{}
	pipes	[]interface{}
	wg 		*sync.WaitGroup
}
func NewPipeline(in chan interface{}) *Pipeline {
	pl := new(Pipeline)
	pl.In = in
	pl.wg = &(sync.WaitGroup{})
	return pl
}
func (pl *Pipeline) FilterChannel(in chan interface{}, f Filter) chan interface{} {
	out := FilterChannel(in, f, pl.wg)
	pl.pipes = append(pl.pipes, out)
	return out
}
func (pl *Pipeline) FilterDemuxed(in map[int32]chan interface{}, f Filter) map[int32]chan interface{} {
	out := FilterDemuxed(in, f, pl.wg)
	pl.pipes = append(pl.pipes, out)
	return out
}
func (pl *Pipeline) FilterDemuxedRespectively(in map[int32]chan interface{}, filterMap map[int32]Filter) map[int32]chan interface{} {
	out := FilterDemuxedRespectively(in, filterMap, pl.wg)
	pl.pipes = append(pl.pipes, out)
	return out
}
func (pl *Pipeline) Demultiplex(in chan interface{}, d Demuxer, ids []int32) map[int32]chan interface{} {
	out := Demultiplex(in, d, ids, pl.wg)
	pl.pipes = append(pl.pipes, out)
	return out
}
func (pl *Pipeline) Multiplex(in map[int32]chan interface{}, m Muxer) chan interface{} {
	out := Multiplex(in, m, pl.wg)
	pl.pipes = append(pl.pipes, out)
	return out
}
func (pl *Pipeline) Close() {
	close(pl.In)
	for _, pipe := range pl.pipes {
		switch p := pipe.(type) {
		case chan interface{}:
			close(p)
		case map[int32]chan interface{}:
			for _, v := range p {
				close(v)
			}
		}
	}
}
func (pl *Pipeline) Wait() {
	pl.wg.Wait()
}

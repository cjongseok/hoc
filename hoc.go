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
	wg.Add(1)
	out := make(chan interface{})
	go f(in, out, wg)
	return out
}
func FilterDemuxed(in map[int32]chan interface{}, f Filter, wg *sync.WaitGroup) map[int32]chan interface{} {
	out := make(map[int32]chan interface{})
	for id, _ := range in {
		wg.Add(1)
		out[id] = make(chan interface{})
		go f(in[id], out[id], wg)
	}
	return out
}
func FilterDemuxedRespectively(in map[int32]chan interface{}, filterMap map[int32]Filter, wg *sync.WaitGroup) map[int32]chan interface{} {
	out := make(map[int32]chan interface{})
	for id, _ := range in {
		wg.Add(1)
		out[id] = make(chan interface{})
		go filterMap[id](in[id], out[id], wg)
	}
	return out
}

type Converter Filter
func ConvertChannel(in chan interface{}, c Converter, wg *sync.WaitGroup) chan interface{} {
	return FilterChannel(in, Filter(c), wg)
}
func ConvertDemuxed(in map[int32]chan interface{}, c Converter, wg *sync.WaitGroup) map[int32]chan interface{} {
	return FilterDemuxed(in, Filter(c), wg)
}
func ConvertDemuxedRespectively(in map[int32]chan interface{}, converterMap map[int32]Converter, wg *sync.WaitGroup) map[int32]chan interface{} {
	filterMap := make(map[int32]Filter, len(converterMap))
	for id, c := range converterMap {
		filterMap[id] = Filter(c)
	}
	return FilterDemuxedRespectively(in, filterMap, wg)
}

type Demuxer func(in chan interface{}, out map[int32]chan interface{}, wg *sync.WaitGroup)
func Demultiplex(in chan interface{}, d Demuxer, ids []int32, wg *sync.WaitGroup) map[int32]chan interface{} {
	wg.Add(1)
	out := make(map[int32]chan interface{})
	for _, id := range ids {
		out[id] = make(chan interface{})
	}
	go d(in, out, wg)
	return out
}

type Muxer func(in map[int32]chan interface{}, out chan interface{}, wg *sync.WaitGroup)
func Multiplex(in map[int32]chan interface{}, m Muxer, wg *sync.WaitGroup) chan interface{} {
	wg.Add(1)
	out := make(chan interface{})
	go m(in, out, wg)
	return out
}
func MergeMuxer (in map[int32]chan interface{}, out chan interface{}, wg *sync.WaitGroup) {
	mergedChanWg := sync.WaitGroup{}
	for _, ch := range in {
		mergedChanWg.Add(1)
		go func(c chan interface{}) {
			defer mergedChanWg.Done()
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
	go func() {
		defer wg.Done()
		mergedChanWg.Wait()
	}()
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
func (pl *Pipeline) ConvertChannel(in chan interface{}, c Converter) chan interface{} {
	out := ConvertChannel(in, c, pl.wg)
	pl.pipes = append(pl.pipes, out)
	return out
}
func (pl *Pipeline) ConvertDemuxed(in map[int32]chan interface{}, c Converter) map[int32]chan interface{} {
	out := ConvertDemuxed(in, c, pl.wg)
	pl.pipes = append(pl.pipes, out)
	return out
}
func (pl *Pipeline) ConvertDemuxedRespectively(in map[int32]chan interface{}, converterMap map[int32]Converter) map[int32]chan interface{} {
	out := ConvertDemuxedRespectively(in, converterMap, pl.wg)
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
func (pl *Pipeline) Close() *sync.WaitGroup{
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
	return pl.wg
}
func (pl *Pipeline) CloseAndWait() {
	pl.Close()
	pl.wg.Wait()
}

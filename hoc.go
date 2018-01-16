package hoc

import (
  "sync"
  "reflect"
)

// Higher-Order Channel

// nil means End-Of-Channel(EOC)
// EOC on a channel stops subordinate HOCs
func Distribute(s ...interface{}) []distribute {
  distributes := make([]distribute, len(s))
  for i, x := range s {
    distributes[i] = x
  }
  return distributes
}
type distribute interface{}
func pushToChannel(x interface{}, out chan interface{}) {
  if out == nil {
    return
  }
  switch x.(type) {
  case []distribute:
    for _, d := range x.([]distribute) {
      out <- d
    }
  default:
    out <-x
  }
}

type Filter func(in interface{}) (out interface{}, err error)
func filterLoop(in chan interface{}, out chan interface{}, f Filter, wg *sync.WaitGroup) {
  defer wg.Done()
  for {
    select {
    case x := <-in:
      if x == nil {
        return
      }
      o, err := f(x)
      if err == nil && o != nil {
        pushToChannel(o, out)
      }
    }
  }
}
func FilterChannel(in chan interface{}, f Filter, wg *sync.WaitGroup) chan interface{} {
  wg.Add(1)
  out := make(chan interface{})
  go filterLoop(in, out, f, wg)
  return out
}
//TODO: filter factory
func FilterDemuxed(in map[int32]chan interface{}, f Filter, wg *sync.WaitGroup) map[int32]chan interface{} {
  out := make(map[int32]chan interface{})
  for id, _ := range in {
    wg.Add(1)
    out[id] = make(chan interface{})
    go filterLoop(in[id], out[id], f, wg)
  }
  return out
}
func FilterDemuxedRespectively(in map[int32]chan interface{}, filterMap map[int32]Filter, wg *sync.WaitGroup) map[int32]chan interface{} {
  out := make(map[int32]chan interface{})
  for id, _ := range in {
    if f, ok := filterMap[id]; ok {
      wg.Add(1)
      out[id] = make(chan interface{})
      go filterLoop(in[id], out[id], f, wg)
    }
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

const (
  DstOfUnregisteredSrc = 0
)
type Demuxer func(in interface{}) (from int32, out interface{}, err error)
func demuxerLoop(in chan interface{}, out map[int32]chan interface{}, d Demuxer, routeMap map[int32]int32, wg *sync.WaitGroup) {
  defer wg.Done()
  for {
    select {
    case x := <-in:
      if x == nil {
        return
      }
      from, o, err := d(x)
      if err == nil && o != nil{
        var ok bool
        var dst int32
        if dst, ok = routeMap[from]; !ok {
          dst = DstOfUnregisteredSrc
        }
        if _, ok = out[dst]; ok {
          pushToChannel(o, out[dst])
        }
      }
    }
  }
}
//type Demuxer func(in chan interface{}, out map[int32]chan interface{}, wg *sync.WaitGroup)
func Demultiplex(in chan interface{}, d Demuxer, routeMap map[int32]int32, wg *sync.WaitGroup) map[int32]chan interface{} {
  wg.Add(1)
  out := make(map[int32]chan interface{})
  for _, to := range routeMap {
    out[to] = make(chan interface{})
  }
  go demuxerLoop(in, out, d, routeMap, wg)
  return out
}

type Replicator func(in interface{}) (out interface{}, err error)
func broadcastLoop(in chan interface{}, outs []chan interface{}, replicate Replicator, wg *sync.WaitGroup) {
  defer wg.Done()
  for {
    select {
    case x := <- in:
      if x == nil {
        return
      }
      for _, out := range outs {
        r, err := replicate(x)
        if err == nil && r != nil {
          pushToChannel(r, out)
        }
      }
    }
  }
}
func Broadcast(in chan interface{}, replicate Replicator, size int, wg *sync.WaitGroup) []chan interface{} {
  wg.Add(1)
  out := make([]chan interface{}, size)
  for i := 0; i < size; i++ {
    out[i] = make(chan interface{})
  }
  go broadcastLoop(in, out, replicate, wg)
  return out
}
func PtrReplicator(x interface{}) (interface{}, error) {
  toPtr := func(x interface{}) interface{} {
    rt := reflect.TypeOf(x)
    switch rt.Kind() {
    case reflect.Ptr:
      return x
    }
    return &x
  }

  return toPtr(x), nil
}

type ListMuxer func(index int, in interface{}) (out interface{}, err error)
func listMuxerLoop(in []chan interface{}, out chan interface{}, m ListMuxer, wg *sync.WaitGroup) {
  muxerWg := sync.WaitGroup{}
  for index, ch := range in {
    muxerWg.Add(1)
    go func(c chan interface{}) {
      defer muxerWg.Done()
      for {
        select {
        case x := <-c:
          if x == nil {
            return
          }
          o, err := m(index, x)
          if err == nil && o != nil {
            //out <-o
            pushToChannel(o, out)
          }
        }
      }
    }(ch)
  }
  go func() {
    defer wg.Done()
    muxerWg.Wait()
  }()
}
type MapMuxer func(port int32, in interface{}) (out interface{}, err error)
func mapMuxerLoop(in map[int32]chan interface{}, out chan interface{}, m MapMuxer, wg *sync.WaitGroup) {
  muxerWg := sync.WaitGroup{}
  for port, ch := range in {
    muxerWg.Add(1)
    go func(c chan interface{}) {
      defer muxerWg.Done()
      for {
        select {
        case x := <-c:
          if x == nil {
            return
          }
          o, err := m(port, x)
          if err == nil && o != nil {
            pushToChannel(o, out)
          }
        }
      }
    }(ch)
  }
  go func() {
    defer wg.Done()
    muxerWg.Wait()
  }()
}
func MultiplexList(in []chan interface{}, m ListMuxer, wg *sync.WaitGroup) chan interface{} {
  wg.Add(1)
  out := make(chan interface{})
  go listMuxerLoop(in, out, m, wg)
  return out
}
func MultiplexMap(in map[int32]chan interface{}, m MapMuxer, wg *sync.WaitGroup) chan interface{} {
  wg.Add(1)
  out := make(chan interface{})
  go mapMuxerLoop(in, out, m, wg)
  return out
}
func MergeListMuxer(index int, in interface{}) (out interface{}, err error) {
  return in, nil
}
func MergeMapMuxer(port int32, in interface{}) (out interface{}, err error) {
  return in, nil
  //func MergeMuxer (in map[int32]chan interface{}, out chan interface{}, wg *sync.WaitGroup) {
  //	mergedChanWg := sync.WaitGroup{}
  //	for _, ch := range in {
  //		mergedChanWg.Add(1)
  //		go func(c chan interface{}) {
  //			defer mergedChanWg.Done()
  //			for {
  //				select {
  //				case x := <-c:
  //					if x == nil {
  //						return
  //					}
  //					out <- x
  //}
  //}
  //}(ch)
  //}
  //go func() {
  //	defer wg.Done()
  //	mergedChanWg.Wait()
  //}()
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

func (pl *Pipeline) Demultiplex(in chan interface{}, d Demuxer, routeMap map[int32]int32) map[int32]chan interface{} {
  out := Demultiplex(in, d, routeMap, pl.wg)
  pl.pipes = append(pl.pipes, out)
  return out
}
func (pl *Pipeline) Broadcast(in chan interface{}, r Replicator, size int) []chan interface{} {
  out := Broadcast(in, r, size, pl.wg)
  pl.pipes = append(pl.pipes, out)
  return out
}
func (pl *Pipeline) MultiplexList(in []chan interface{}, m ListMuxer) chan interface{} {
  out := MultiplexList(in, m, pl.wg)
  pl.pipes = append(pl.pipes, out)
  return out
}
func (pl *Pipeline) MultiplexMap(in map[int32]chan interface{}, m MapMuxer) chan interface{} {
  out := MultiplexMap(in, m, pl.wg)
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

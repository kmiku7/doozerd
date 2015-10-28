package store

import (
	"errors"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Special values for a revision.
// special 在 用来表示节点类型
const (
	Missing = int64(-iota)
	Clobber
	Dir
	nop
)

// TODO revisit this when package regexp is more complete (e.g. do Unicode)
const charPat = `[a-zA-Z0-9.\-]`

var pathRe = mustBuildRe(charPat)

var Any = MustCompileGlob("/**")

var ErrTooLate = errors.New("too late")

var (
	ErrBadMutation = errors.New("bad mutation")
	ErrRevMismatch = errors.New("rev mismatch")
	ErrBadPath     = errors.New("bad path")
)

func mustBuildRe(p string) *regexp.Regexp {
	return regexp.MustCompile(`^/$|^(/` + p + `+)+$`)
}

// Applies mutations sent on Ops in sequence according to field Seqn. Any
// errors that occur will be written to ErrorPath. Duplicate operations at a
// given position are sliently ignored.
type Store struct {
	// 新操作
	Ops     chan<- Op
	Seqns   <-chan int64
	Waiting <-chan int
	// 新 watcher
	watchCh chan *watch
	// 还未触发的 watcher, 未来触发
	watches []*watch
	// 未来的操作, 版本号 >= targetVer 时才触发。
	todo    []Op
	// 里面放存储
	state   *state
	head    int64
	// 历史版本数据
	log     map[int64]Event
	cleanCh chan int64
	flush   chan bool
}

// Represents an operation to apply to the store at position Seqn.
//
// If Mut is Nop, no change will be made, but an event will still be sent.
type Op struct {
	Seqn int64
	// Mut 是一个复杂的字符串
	Mut  string
}

type state struct {
	ver  int64
	root node
}

type watch struct {
	glob *Glob
	rev  int64
	c    chan<- Event
}

// Creates a new, empty data store. Mutations will be applied in order,
// starting at number 1 (number 0 can be thought of as the creation of the
// store).
func New() *Store {
	ops := make(chan Op)
	seqns := make(chan int64)
	watches := make(chan int)

	st := &Store{
		Ops:     ops,
		Seqns:   seqns,
		Waiting: watches,
		watchCh: make(chan *watch),
		watches: []*watch{},
		state:   &state{0, emptyDir},
		log:     map[int64]Event{},			// 历史版本数据
		cleanCh: make(chan int64),
		flush:   make(chan bool),
	}

	go st.process(ops, seqns, watches)
	return st
}

// path 末尾不带斜线
func split(path string) []string {
	if path == "/" {
		return []string{}
	}
	return strings.Split(path[1:], "/")
}

func join(parts []string) string {
	return "/" + strings.Join(parts, "/")
}

// 限制path里出现的字符
func checkPath(k string) error {
	if !pathRe.MatchString(k) {
		return ErrBadPath
	}
	return nil
}

// 跟猜测的一样。。。
// 这里说的 rev == Clobber 指的 函数参数为 Clobber 而不是 node.Rev==Clobber 吧
// Returns a mutation that can be applied to a `Store`. The mutation will set
// the contents of the file at `path` to `body` iff `rev` is greater than
// of equal to the file's revision at the time of application, with
// one exception: if `rev` is Clobber, the file will be set unconditionally.
func EncodeSet(path, body string, rev int64) (mutation string, err error) {
	if err = checkPath(path); err != nil {
		return
	}
	return strconv.FormatInt(rev, 10) + ":" + path + "=" + body, nil
}

// Returns a mutation that can be applied to a `Store`. The mutation will cause
// the file at `path` to be deleted iff `rev` is greater than
// of equal to the file's revision at the time of application, with
// one exception: if `rev` is Clobber, the file will be deleted
// unconditionally.
func EncodeDel(path string, rev int64) (mutation string, err error) {
	if err = checkPath(path); err != nil {
		return
	}
	return strconv.FormatInt(rev, 10) + ":" + path, nil
}

// MustEncodeSet is like EncodeSet but panics if the mutation cannot be
// encoded. It simplifies safe initialization of global variables holding
// mutations.
// 唯一可能失败的点在于path
func MustEncodeSet(path, body string, rev int64) (mutation string) {
	m, err := EncodeSet(path, body, rev)
	if err != nil {
		panic(err)
	}
	return m
}

// MustEncodeDel is like EncodeDel but panics if the mutation cannot be
// encoded. It simplifies safe initialization of global variables holding
// mutations.
func MustEncodeDel(path string, rev int64) (mutation string) {
	m, err := EncodeDel(path, rev)
	if err != nil {
		panic(err)
	}
	return m
}

// Mut 串结构：
//	REV:KEY=VALUE
//	REV:KEY		这是一个删除操作？？？
// keep 是个什么含义？
func decode(mutation string) (path, v string, rev int64, keep bool, err error) {
	cm := strings.SplitN(mutation, ":", 2)

	if len(cm) != 2 {
		err = ErrBadMutation
		return
	}

	rev, err = strconv.ParseInt(cm[0], 10, 64)
	if err != nil {
		return
	}

	kv := strings.SplitN(cm[1], "=", 2)

	if err = checkPath(kv[0]); err != nil {
		return
	}

	switch len(kv) {
	case 1:
		return kv[0], "", rev, false, nil
	case 2:
		return kv[0], kv[1], rev, true, nil
	}
	panic("unreachable")
}

// watch 是一次性的
// 看调用方，历史版本从小版本号开始比较
func (st *Store) notify(e Event, ws []*watch) (nws []*watch) {
	for _, w := range ws {
		if e.Seqn >= w.rev && w.glob.Match(e.Path) {
			w.c <- e
		} else {
			nws = append(nws, w)
		}
	}

	return nws
}

func (st *Store) closeWatches() {
	for _, w := range st.watches {
		close(w.c)
	}
}

func (st *Store) process(ops <-chan Op, seqns chan<- int64, watches chan<- int) {
	defer st.closeWatches()

	for {
		var flush bool
		ver, values := st.state.ver, st.state.root

		// Take any incoming requests and queue them up.
		select {
		case a, ok := <-ops:
			if !ok {
				return
			}

			// 版本好虽然 “挂” 在单个节点上， 但他是一个全局的序列化号码，也就是说多个客户端的多个操作之间应该是串行的。
			// 因此 读多于写 的场景下使用doozer会更好一些。
			if a.Seqn > ver {
				st.todo = append(st.todo, a)
			}
			// seqn <= ver 的直接丢弃？？
		case w := <-st.watchCh:
			n, ws := w.rev, []*watch{w}
			// 为何要封装到数组里，这段代码好奇怪。
			// if n < st.head {
			//		ws = []*watch{}
			//		n = n +1
			// }
			for ; len(ws) > 0 && n < st.head; n++ {
				ws = []*watch{}
			}
			for ; len(ws) > 0 && n <= ver; n++ {
				ws = st.notify(st.log[n], ws)
			}

			st.watches = append(st.watches, ws...)
		case seqn := <-st.cleanCh:
			for ; st.head <= seqn; st.head++ {
				delete(st.log, st.head)
			}
		case seqns <- ver:
			// nothing to do here
		case watches <- len(st.watches):
			// nothing to do here
		case flush = <-st.flush:
			// nothing
		}

		var ev Event
		// If we have any mutations that can be applied, do them.
		for len(st.todo) > 0 {
			i := firstTodo(st.todo)
			t := st.todo[i]
			if flush && ver < t.Seqn {
				ver = t.Seqn - 1
			}
			if t.Seqn > ver+1 {
				break
			}

			// slice 的副作用
			st.todo = append(st.todo[:i], st.todo[i+1:]...)
			if t.Seqn < ver+1 {
				continue
			}

			values, ev = values.apply(t.Seqn, t.Mut)
			st.state = &state{ev.Seqn, values}
			ver = ev.Seqn
			if !flush {
				st.log[ev.Seqn] = ev
				st.watches = st.notify(ev, st.watches)
			}
		}

		// A flush just gets one final event.
		if flush {
			st.log[ev.Seqn] = ev
			st.watches = st.notify(ev, st.watches)
			st.head = ver + 1
		}
	}
}

// Find Min Value
func firstTodo(a []Op) (pos int) {
	n := int64(math.MaxInt64)
	pos = -1
	for i, o := range a {
		if o.Seqn < n {
			n = o.Seqn
			pos = i
		}
	}
	return
}

// Returns a point-in-time snapshot of the contents of the store.
func (st *Store) Snap() (ver int64, g Getter) {
	// WARNING: Be sure to read the pointer value of st.state only once. If you
	// need multiple accesses, copy the pointer first.
	// 这是说 st.state.ver, st.state.root 有可能返回不匹配的值？
	p := st.state

	return p.ver, p.root
}

// Gets the value stored at `path`, if any.
//
// If no value is stored at `path`, `rev` will be `Missing` and `value` will be
// nil.
//
// if `path` is a directory, `rev` will be `Dir` and `value` will be a list of
// entries.
//
// Otherwise, `rev` is the revision and `value[0]` is the body.
func (st *Store) Get(path string) (value []string, rev int64) {
	_, g := st.Snap()
	return g.Get(path)
}

func (st *Store) Stat(path string) (int32, int64) {
	_, g := st.Snap()
	return g.Stat(path)
}

// Apply all operations in the internal queue, even if there are gaps in the
// sequence (gaps will be treated as no-ops). This is only useful for
// bootstrapping a store from a point-in-time snapshot of another store.
func (st *Store) Flush() {
	st.flush <- true
}

// Returns a chan that will receive a single event representing the
// first change made to any file matching glob on or after rev.
//
// If rev is less than any value passed to st.Clean, Wait will return
// ErrTooLate.
func (st *Store) Wait(glob *Glob, rev int64) (<-chan Event, error) {
	if rev < 1 {
		rev = 1
	}

	ch := make(chan Event, 1)
	wt := &watch{
		glob: glob,
		rev:  rev,
		c:    ch,
	}
	st.watchCh <- wt

	if rev < st.head {
		return nil, ErrTooLate
	}
	return ch, nil
}

func (st *Store) Clean(seqn int64) {
	st.cleanCh <- seqn
}

package godb

import (
	"errors"
	"sync"
	"time"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	Map                map[any]Page
	LRUQueue           []any
	NumPages           int
	MaxPages           int
	Mutex              sync.Mutex
	QueueMutex         sync.Mutex
	SharedLocks        map[any]([]TransactionID)
	ExclusiveLocks     map[any]TransactionID
	ActiveTransactions map[TransactionID]bool
	WaitsFor           map[TransactionID]([]TransactionID)
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	return &BufferPool{
		Map:                make(map[any]Page),
		LRUQueue:           make([]any, numPages),
		NumPages:           0,
		MaxPages:           numPages,
		SharedLocks:        make(map[any]([]TransactionID)),
		ExclusiveLocks:     make(map[any]TransactionID),
		ActiveTransactions: make(map[TransactionID]bool),
		WaitsFor:           make(map[TransactionID]([]TransactionID)),
	}
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	for i := 0; i < bp.NumPages; i++ {
		pageKey := bp.LRUQueue[i]
		p := bp.Map[pageKey]
		hp := p.(*heapPage)
		f := hp.getFile()
		hf := (*f).(*HeapFile)
		hf.flushPage(&p)
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.Mutex.Lock()
	delete(bp.ActiveTransactions, tid)
	bp.RemoveFromWaitsFor(tid)
	for pageKey, _tid := range bp.ExclusiveLocks {
		if _tid == tid {
			bp.DiscardPage(pageKey)
			delete(bp.ExclusiveLocks, pageKey)
		}
	}

	for pageKey, tids := range bp.SharedLocks {
		for i := 0; i < len(tids); i++ {
			if tids[i] == tid {
				newTids := removeTid(tids, i)
				bp.SharedLocks[pageKey] = newTids
				break
			}
		}
	}
	bp.Mutex.Unlock()
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	bp.Mutex.Lock()
	delete(bp.ActiveTransactions, tid)
	bp.RemoveFromWaitsFor(tid)
	for pageKey, _tid := range bp.ExclusiveLocks {
		if _tid == tid {
			delete(bp.ExclusiveLocks, pageKey)
			page, cached := bp.Map[pageKey]
			if !cached {
				continue
			}
			file := page.getFile()
			heapFile, _ := (*file).(*HeapFile)
			heapFile.flushPage(&page)
		}
	}

	for pageKey, tids := range bp.SharedLocks {
		for i := 0; i < len(tids); i++ {
			if tids[i] == tid {
				newTids := removeTid(tids, i)
				bp.SharedLocks[pageKey] = newTids
				break
			}
		}
	}
	bp.Mutex.Unlock()
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	return nil
}

func removeTid(slice []TransactionID, i int) []TransactionID {
	if i == 0 {
		return slice[1:]
	}
	return append(slice[:i], slice[i+1:]...)
}

func remove(slice []any, i int) []any {
	if i == 0 {
		return slice[1:]
	}
	return append(slice[:i], slice[i+1:]...)
}

func (bp *BufferPool) DiscardPage(pageKey any) {
	index := 0
	for index < bp.NumPages {
		_pageKey := bp.LRUQueue[index]
		if _pageKey == pageKey {
			newQueue := remove(bp.LRUQueue, index)
			newQueue = append(newQueue, 0)
			bp.LRUQueue = newQueue
			bp.NumPages--
			break
		}
		index++
	}
	delete(bp.Map, pageKey)
}

func (bp *BufferPool) Evict() error {
	index := 0
	for index < bp.NumPages {
		pageKey := bp.LRUQueue[index]
		p, found := bp.Map[pageKey]
		if !found {
			return errors.New("Page not found")
		}
		if !p.isDirty() {
			break
		}
		index++
	}
	if index == bp.NumPages {
		bp.FlushAllPages()
		index = 0
	}
	pageKey := bp.LRUQueue[index]
	page := bp.Map[pageKey]
	file := page.getFile()
	heapFile, _ := (*file).(*HeapFile)
	heapFile.flushPage(&page)
	newQueue := remove(bp.LRUQueue, index)
	newQueue = append(newQueue, 0)
	delete(bp.Map, pageKey)
	bp.LRUQueue = newQueue
	bp.NumPages--
	return nil
}

func (bp *BufferPool) UpdateQueue(pageKey any) {
	var index = bp.NumPages - 1
	for i := 0; i < bp.NumPages; i++ {
		if bp.LRUQueue[i] == pageKey {
			index = i
			break
		}
	}
	for i := index; i < bp.NumPages-1; i++ {
		bp.LRUQueue[i] = bp.LRUQueue[i+1]
	}
	bp.LRUQueue[bp.NumPages-1] = pageKey
}

func (bp *BufferPool) HasSharedLock(pageKey any, tid TransactionID) bool {
	tids, present := bp.SharedLocks[pageKey]
	if present {
		for i := 0; i < len(tids); i++ {
			if tids[i] == tid {
				return true
			}
		}
	}
	return false
}

// func (bp *BufferPool) AcquireExclusiveLock(pageNo int, tid TransactionID) {
// 	pageKey := file.pageKey(pageNo)
// 	for {
// 		bp.Mutex.Lock()
// 		_tid, held := bp.ExclusiveLocks[pageKey]
// 		if _tid == tid {
// 			bp.Mutex.Unlock()
// 			break
// 		}
// 	}
// }

func containsTransaction(s []TransactionID, e TransactionID) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (bp *BufferPool) AddTransactionsToWaitsFor(tid TransactionID, tids []TransactionID) {
	for i := 0; i < len(tids); i++ {
		if !containsTransaction(bp.WaitsFor[tid], tids[i]) {
			bp.WaitsFor[tid] = append(bp.WaitsFor[tid], tids[i])
		}
	}
}

func detectCycleHelper(graph map[TransactionID][]TransactionID, tid TransactionID, visiting map[TransactionID]bool, visited map[TransactionID]bool) bool {
	if len(graph[tid]) > 0 {
	}
	if _, found := visited[tid]; found {
		return false
	}

	if _, found := visiting[tid]; found {
		return true
	}

	visiting[tid] = true
	for _, descendant := range graph[tid] {
		if detectCycleHelper(graph, descendant, visiting, visited) {
			return true
		}
	}

	delete(visiting, tid)
	visited[tid] = true
	return false
}

func (bp *BufferPool) DetectCycle(tid TransactionID) bool {
	visiting := make(map[TransactionID]bool)
	visited := make(map[TransactionID]bool)
	return detectCycleHelper(bp.WaitsFor, tid, visiting, visited)
}

func (bp *BufferPool) RemoveFromWaitsFor(tid TransactionID) {
	delete(bp.WaitsFor, tid)
	for _tid, tids := range bp.WaitsFor {
		for i := 0; i < len(tids); i++ {
			if tids[i] == tid {
				newTids := removeTid(tids, i)
				bp.WaitsFor[_tid] = newTids
				break
			}
		}
	}
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	pageKey := file.pageKey(pageNo)
	for {
		bp.Mutex.Lock()
		_tid, held := bp.ExclusiveLocks[pageKey]
		if _tid == tid {
			bp.Mutex.Unlock()
			break
		}

		if held && !containsTransaction(bp.WaitsFor[tid], _tid) {
			bp.WaitsFor[tid] = append(bp.WaitsFor[tid], _tid)
		}

		if !held {
			if perm == WritePerm {
				tids, present := bp.SharedLocks[pageKey]
				// upgrade lock to exclusive lock
				if present && len(tids) == 1 && tids[0] == tid {
					delete(bp.SharedLocks, pageKey)
					bp.ExclusiveLocks[pageKey] = tid
					bp.ActiveTransactions[tid] = true
					bp.Mutex.Unlock()
					break
				} else if present && len(tids) > 0 {
					bp.AddTransactionsToWaitsFor(tid, tids)
					if bp.DetectCycle(tid) {
						bp.Mutex.Unlock()
						bp.AbortTransaction(tid)
						return nil, errors.New("deadlock found")
					}
					bp.Mutex.Unlock()
					time.Sleep(2 * time.Millisecond)
					continue
				} else {
					bp.ExclusiveLocks[pageKey] = tid
					bp.ActiveTransactions[tid] = true
				}
			} else if perm == ReadPerm {
				tids, present := bp.SharedLocks[pageKey]
				if present {
					found := false
					for i := 0; i < len(tids); i++ {
						if tids[i] == tid {
							found = true
							if bp.DetectCycle(tid) {
								bp.Mutex.Unlock()
								bp.AbortTransaction(tid)
								return nil, errors.New("deadlock found")
							}
							break
						}
					}
					if !found {
						bp.AddTransactionsToWaitsFor(tid, tids)
						bp.SharedLocks[pageKey] = append(tids, tid)
					}
				} else {
					bp.SharedLocks[pageKey] = []TransactionID{tid}
					bp.ActiveTransactions[tid] = true
				}
			}
			bp.Mutex.Unlock()
			break
		}
		if bp.DetectCycle(tid) {
			bp.Mutex.Unlock()
			bp.AbortTransaction(tid)
			return nil, errors.New("deadlock found")
		}
		bp.Mutex.Unlock()
		time.Sleep(2 * time.Millisecond)
	}

	page, cached := bp.Map[pageKey]
	if cached {
		bp.QueueMutex.Lock()
		bp.UpdateQueue(pageKey)
		bp.QueueMutex.Unlock()
		return &page, nil
	}
	// Cache miss
	_page, err := file.readPage(pageNo)
	if err != nil {
		return nil, err
	}

	bp.QueueMutex.Lock()
	defer bp.QueueMutex.Unlock()
	if bp.NumPages == bp.MaxPages {
		err := bp.Evict()
		if err != nil {
			return nil, err
		}
	}

	bp.Map[pageKey] = *_page
	bp.NumPages++
	bp.UpdateQueue(pageKey)

	return _page, nil
}

package godb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	bufPool *BufferPool
	desc    *TupleDesc
	file    string
	Mutex   sync.Mutex
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	_, err := os.Stat(fromFile)
	if err != nil {
		b := make([]byte, 0)
		err := os.WriteFile(fromFile, b, 0644)
		if err != nil {
			return nil, err
		}
	}
	return &HeapFile{bufPool: bp, desc: td, file: fromFile}, nil //replace me
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	f.Mutex.Lock()
	file, err := os.Open(f.file)
	if err != nil {
		log.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	bytes := info.Size()
	file.Close()
	f.Mutex.Unlock()
	return int(bytes) / PageSize
}

func (f *HeapFile) NumPagesWithoutLock() int {
	file, err := os.Open(f.file)
	if err != nil {
		log.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	bytes := info.Size()
	file.Close()
	return int(bytes) / PageSize
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	file, err := os.Open(f.file)
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(int64(pageNo*PageSize), 0)
	if err != nil {
		return nil, err
	}
	for pageNo >= f.NumPages() {
		return nil, errors.New("page does not exist")
	}
	b := make([]byte, PageSize)
	_, err = file.Read(b)
	if err != nil {
		return nil, err
	}
	var p Page = newHeapPage(f.desc, pageNo, f)
	hp, _ := p.(*heapPage)
	hp.initFromBuffer(bytes.NewBuffer(b))
	file.Close()
	return &p, nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	f.Mutex.Lock()
	numPages := f.bufPool.NumPages
	f.Mutex.Unlock()
	for i := numPages - 1; i >= 0; i-- {
		pageKey, valid := (f.bufPool.LRUQueue[i]).(heapHash)
		if !valid || pageKey.FileName != f.file {
			continue
		}
		num := pageKey.PageNo
		page, err := f.bufPool.GetPage(f, num, tid, WritePerm)
		if err != nil {
			return err
		}
		p, _ := (*page).(*heapPage)
		if p.File.file != f.file {
			continue
		}
		if p.NumberUsedSlots == p.NumberSlots {
			continue
		}

		_, err = p.insertTuple(t)
		if err != nil {
			return err
		}
		return nil
	}

	for i := 0; i < f.NumPages(); i++ {
		page, err := f.bufPool.GetPage(f, i, tid, WritePerm)
		if err != nil {
			return err
		}
		p, _ := (*page).(*heapPage)
		if p.NumberUsedSlots == p.NumberSlots {
			continue
		}

		p, _ = (*page).(*heapPage)
		_, err = p.insertTuple(t)
		if err != nil {
			return err
		}
		return nil

	}

	f.Mutex.Lock()
	pageNo := f.NumPagesWithoutLock()
	var p heapPage = *newHeapPage(f.desc, pageNo, f)
	var _p Page = &p
	err := f.flushPage(&_p)
	if err != nil {
		return err
	}
	f.Mutex.Unlock()
	page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
	if err != nil {
		return err
	}
	hp, _ := (*page).(*heapPage)
	_, err = hp.insertTuple(t)
	if err != nil {
		return err
	}

	return nil
}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	rid, _ := t.Rid.(TupleRecordID)
	pageNo := rid.PageNo
	p, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
	if err != nil {
		return err
	}

	hp, _ := (*p).(*heapPage)
	err = hp.deleteTuple(rid)
	if err != nil {
		return err
	}
	return nil
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	hp, _ := (*p).(*heapPage)
	hp.setDirty(false)
	pageNo := hp.PageNo
	buf, err := hp.toBuffer()
	if err != nil {
		return err
	}

	file, err := os.OpenFile(f.file, os.O_RDWR, 0755)
	if err != nil {
		return err
	}
	file.Seek(int64(pageNo*PageSize), 0)
	b := make([]byte, PageSize)
	copy(b, buf.Bytes())
	_, err = file.Write(b)
	if err != nil {
		return err
	}
	file.Close()
	return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.desc
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	n := -1
	var iter func() (*Tuple, error)
	return func() (*Tuple, error) {
		for {
			if f.NumPages() == 0 || n >= f.NumPages() {
				return nil, nil
			}

			if n == -1 {
				n++
				p, err := f.bufPool.GetPage(f, n, tid, ReadPerm)
				if err != nil {
					return nil, err
				}
				hp, _ := (*p).(*heapPage)
				iter = hp.tupleIter()
			}

			t, err := iter()
			if t == nil && n+1 < f.NumPages() {
				n++
				p, err := f.bufPool.GetPage(f, n, tid, ReadPerm)
				if err != nil {
					return nil, err
				}
				hp, _ := (*p).(*heapPage)
				iter = hp.tupleIter()
			} else {
				return t, err
			}
		}
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	return heapHash{FileName: f.file, PageNo: pgNo}
}

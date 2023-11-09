package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	PageNo          int
	NumberSlots     int32
	NumberUsedSlots int32
	Desc            TupleDesc
	Tuples          []Tuple
	UsedSlots       []int
	IsDirty         bool
	File            *HeapFile
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// Calculate size of tuples
	bytesPerTuple := 0
	for _, field := range desc.Fields {
		if field.Ftype == StringType {
			bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
		} else {
			bytesPerTuple += (int)(unsafe.Sizeof(int64(0)))
		}
	}
	numSlots := (int32)((PageSize - 8) / bytesPerTuple)
	return &heapPage{
		PageNo:          pageNo,
		NumberSlots:     numSlots,
		NumberUsedSlots: 0,
		Desc:            *desc,
		Tuples:          make([]Tuple, numSlots),
		UsedSlots:       make([]int, numSlots),
		IsDirty:         false,
		File:            f}
}

func (h *heapPage) getNumSlots() int {
	return int(h.NumberSlots)
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	h.setDirty(true)
	for i := 0; i < int(h.NumberSlots); i++ {
		if h.UsedSlots[i] == 0 {
			h.Tuples[i] = *t
			t.Rid = TupleRecordID{PageNo: h.PageNo, SlotNum: i}
			h.UsedSlots[i] = 1
			h.NumberUsedSlots++
			return t.Rid, nil
		}
	}
	return nil, errors.New("No free slot")
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	tupleRecordID, isTupleRecordID := rid.(TupleRecordID)

	if !isTupleRecordID {
		return errors.New("Wrong record ID type")
	}
	if tupleRecordID.SlotNum >= int(h.NumberSlots) {
		return errors.New("Invalid slot")
	}
	if h.UsedSlots[tupleRecordID.SlotNum] == 0 {
		return errors.New("No tuple at slot")
	}

	h.UsedSlots[tupleRecordID.SlotNum] = 0
	h.NumberUsedSlots--
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.IsDirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	h.IsDirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	var f DBFile = p.File
	return &f
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	b := new(bytes.Buffer)
	err := binary.Write(b, binary.LittleEndian, h.NumberSlots)
	if err != nil {
		return nil, err
	}
	err = binary.Write(b, binary.LittleEndian, h.NumberUsedSlots)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(h.Tuples); i++ {
		if h.UsedSlots[i] == 1 {
			err = h.Tuples[i].writeTo(b)
			if err != nil {
				return nil, err
			}
		}
	}
	return b, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var numberSlots int32
	var numberUsedSlots int32
	err := binary.Read(buf, binary.LittleEndian, &numberSlots)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &numberUsedSlots)
	if err != nil {
		return err
	}
	h.NumberSlots = numberSlots
	h.NumberUsedSlots = numberUsedSlots
	for i := 0; i < int(numberUsedSlots); i++ {
		tuple, err := readTupleFrom(buf, &h.Desc)
		if err != nil {
			return err
		}
		tuple.Rid = TupleRecordID{PageNo: h.PageNo, SlotNum: i}
		h.Tuples[i] = *tuple
		h.UsedSlots[i] = 1
	}
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (h *heapPage) tupleIter() func() (*Tuple, error) {
	i := -1
	return func() (*Tuple, error) {
		i++
		for i < int(h.NumberSlots) && h.UsedSlots[i] != 1 {
			i++
		}
		if i >= int(h.NumberSlots) {
			return nil, nil
		}
		return &h.Tuples[i], nil
	}
}

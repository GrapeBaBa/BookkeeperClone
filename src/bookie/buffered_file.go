package bookie

import (
	"os"
	"math"
	"errors"
	"sync"
	"sync/atomic"
)

type BufferedFile struct {
	File *os.File
}

type BufferedReadFile struct {
	*BufferedFile
	readCapacity        int
	readBuffer          []byte
	filledReadBufferLen int
	readBufferStartPos  int64
	invocationCount     int64
	cacheHitCount       int64
	sync.Mutex
}

type BufferedReadWriteFile struct {
	*BufferedReadFile
	writeCapacity            int
	writeBufferStartPosition int64
	writeBuffer              []byte
	filledWriteBufferLen     int
	nextWritePos             int64
	sync.Mutex
}

func New(file *os.File) *BufferedFile {
	return &BufferedFile{File: file}
}

func NewBufferedReadFile(file *os.File, readCapacity int) *BufferedReadFile {
	return &BufferedReadFile{
		BufferedFile:       New(file), readCapacity: readCapacity,
		readBuffer:         make([]byte, readCapacity), filledReadBufferLen: 0,
		readBufferStartPos: math.MinInt64, invocationCount: 0, cacheHitCount: 0}
}

func NewBufferedReadWriteFile(file *os.File, writeCapacity int, readCapacity int) *BufferedReadWriteFile {
	fi, _ := file.Stat()
	return &BufferedReadWriteFile{
		BufferedReadFile:         NewBufferedReadFile(file, readCapacity),
		writeCapacity:            writeCapacity, nextWritePos: fi.Size(),
		filledWriteBufferLen:     0, writeBuffer: make([]byte, writeCapacity),
		writeBufferStartPosition: fi.Size()}
}

func (bf *BufferedFile) GetFile() *os.File {
	return bf.File
}

func (bf *BufferedFile) Size() (int64, error) {
	fi, err := bf.File.Stat()
	if err != nil {
		return int64(0), err
	}

	size := fi.Size()
	return size, nil
}

func (brf *BufferedReadFile) ReadAt(dest []byte, startPos int64) (int, error) {
	brf.Lock()
	defer brf.Unlock()
	brf.invocationCount += 1
	filledDestLen := 0
	internalDest := make([]byte, 0, len(dest))
	currPos := startPos
	eof, err := brf.Size()
	if err != nil {
		return -1, err
	}

	if startPos >= eof {
		return -1, nil
	}

	for len(dest)-filledDestLen > 0 {
		if brf.readBufferStartPos <= currPos && currPos < brf.readBufferStartPos+int64(brf.filledReadBufferLen) {
			posInBuffer := currPos - brf.readBufferStartPos
			bytesToCopy := min(int64(len(dest)-filledDestLen), int64(brf.filledReadBufferLen)-posInBuffer)
			internalDest = append(internalDest, brf.readBuffer[posInBuffer:posInBuffer+int64(bytesToCopy)]...)
			currPos += bytesToCopy
			filledDestLen += int(bytesToCopy)
			brf.cacheHitCount += 1
		} else if currPos >= eof {
			break
		} else {
			brf.readBufferStartPos = currPos
			//TODO:check specific error
			readBytes, _ := brf.GetFile().ReadAt(brf.readBuffer, currPos)

			if readBytes <= 0 {
				return -1, errors.New("Reading from underlying file returned a non-positive value. Short read.")
			}

			brf.filledReadBufferLen = readBytes
		}
	}

	copy(dest, internalDest)
	return int(currPos - startPos), nil
}

func (brf *BufferedReadFile) clear() {
	brf.Lock()
	defer brf.Unlock()
	brf.readBuffer = make([]byte, brf.readCapacity)
}

func (brwf *BufferedReadWriteFile) Write(src []byte) error {
	brwf.Lock()
	defer brwf.Unlock()
	copied := 0
	for copied < len(src) {
		if len(brwf.writeBuffer)-brwf.filledWriteBufferLen < len(src)-copied {
			copy(brwf.writeBuffer, src[copied:copied+len(brwf.writeBuffer)-int(brwf.filledWriteBufferLen)])
			copied += len(brwf.writeBuffer) - int(brwf.filledWriteBufferLen)
			brwf.filledWriteBufferLen = len(brwf.writeBuffer)
		} else {
			copy(brwf.writeBuffer, src[copied:])
			brwf.filledWriteBufferLen += len(src) - copied
			copied = len(src)
		}
		if len(brwf.writeBuffer) == brwf.filledWriteBufferLen {
			brwf.flushInternal()
		}
	}
	brwf.nextWritePos = atomic.AddInt64(&brwf.nextWritePos, int64(copied))
	return nil
}

func (brwf *BufferedReadWriteFile) flushInternal() error {
	_, err := brwf.File.Write(brwf.writeBuffer)
	if err != nil {
		return err
	}
	fi, err := brwf.File.Stat()
	if err != nil {
		return err
	}
	brwf.writeBuffer = make([]byte, brwf.writeCapacity)
	brwf.filledWriteBufferLen = 0
	atomic.StoreInt64(&brwf.writeBufferStartPosition, fi.Size())
	return nil
}

func (brwf *BufferedReadWriteFile) GetFilePos() int64 {
	return atomic.LoadInt64(&brwf.writeBufferStartPosition)
}

func (brwf *BufferedReadWriteFile) GetPos() int64 {
	return atomic.LoadInt64(&brwf.nextWritePos)
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

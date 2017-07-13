package bookie

import (
	"testing"
	"os"
)

func TestBufferedReadFile_ReadAt(t *testing.T) {
	file, _ := os.Create("/tmp/bufferedfiletest")
	defer os.Remove("/tmp/bufferedfiletest")
	defer file.Close()
	input := []byte{byte(1), byte(1), byte(1), byte(1), byte(1), byte(1), byte(1), byte(1), byte(1),
					byte(2), byte(2), byte(2), byte(2), byte(2), byte(2), byte(2), byte(2), byte(2),
					byte(3), byte(3), byte(3), byte(3), byte(3), byte(3), byte(3), byte(3), byte(3)}
	file.Write(input)
	brf := NewBufferedReadFile(file, 10)
	dest := make([]byte, 200)
	brf.ReadAt(dest, 0)
	if dest[1] != byte(1) {
		t.Fail()
	}
	if dest[9] != byte(2) {
		t.Fail()
	}
	if dest[27] != byte(0) {
		t.Fail()
	}
}

func TestBufferedReadWriteFile_Write(t *testing.T) {
	file, _ := os.Create("/tmp/bufferedfiletest")
	defer os.Remove("/tmp/bufferedfiletest")
	defer file.Close()
	brwf := NewBufferedReadWriteFile(file, 16, 32)

	input := []byte{byte(1), byte(1), byte(1), byte(1), byte(1), byte(1), byte(1), byte(1), byte(1),
					byte(2), byte(2), byte(2), byte(2), byte(2), byte(2), byte(2), byte(2), byte(2),
					byte(3), byte(3), byte(3), byte(3), byte(3), byte(3), byte(3), byte(3), byte(3)}
	brwf.Write(input)
	if brwf.GetPos() != 27 {
		t.Fail()
	}

	if brwf.GetFilePos() != 16 {
		t.Fail()
	}
}

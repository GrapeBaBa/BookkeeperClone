package bookie

type EntryKeyValue struct {
	EntryKey
	Value  []byte
	Offset uint32
	Length uint32
}

func NewEntryKeyValue(ledgerId uint64, entryId uint64, value []byte, offset uint32, length uint32) EntryKeyValue {
	return EntryKeyValue{EntryKey{ledgerId, entryId}, value, offset, length}
}

package bookie

type EntryKey struct {
	LedgerId uint64
	EntryId  uint64
}

func NewEntryKey(ledgerId uint64, entryId uint64) EntryKey {
	return EntryKey{ledgerId, entryId}
}

func SameEntryKey(this EntryKey, that EntryKey) int {
	res := int(this.LedgerId - that.LedgerId)
	if res == 0 {
		res = int(this.EntryId - that.EntryId)
	}

	switch {
	case res < 0:
		res = -1
	case res > 0:
		res = 1
	default:
		res = 0
	}

	return int(res)
}

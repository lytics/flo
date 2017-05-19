package md5id

import (
	"crypto/md5"
)

// FromBytes create unique ID.
func FromBytes(data []byte) string {
	h := md5.New()
	h.Write(data)
	id := h.Sum(nil)
	return string(id)
}

// FromString create unique ID.
func FromString(data string) string {
	h := md5.New()
	h.Write([]byte(data))
	id := h.Sum(nil)
	return string(id)
}

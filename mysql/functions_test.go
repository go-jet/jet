package mysql

import (
	"testing"

	"github.com/google/uuid"
)

func TestUUIDToBin(t *testing.T) {
	assertSerialize(t, UUID_TO_BIN(String(uuid.Nil.String())), `uuid_to_bin(?)`, uuid.Nil.String())
}

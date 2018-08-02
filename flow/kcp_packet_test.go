package flow

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestKCPHeader(t *testing.T) {
	sign, _ := hex.DecodeString("B94D27B9934D3E08A52E52D7DA7DABFAC484EFE37A5380EE9088F7ACE2EFCDE9")

	kcpHeader := KCPPacketHeader{
		BLRQ:      [4]byte{0x42, 0x4C, 0x52, 0x51},
		Timestamp: 1533189652,
		Length:    0x07FC,
	}

	const expected = "424c5251000007fc000000005b629e14b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

	copy(kcpHeader.Signature[0:], sign)

	t.Run("Header Encoding", func(t *testing.T) {
		var buf bytes.Buffer

		binary.Write(&buf, binary.BigEndian, kcpHeader)

		binhex := fmt.Sprintf("%x", buf.Bytes())

		if binhex != expected {
			t.Fatalf("Binhex is not equal than expected")
		}
	})
}


func TestKCP

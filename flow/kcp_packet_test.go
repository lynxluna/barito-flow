package flow

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func TestKCPHeader(t *testing.T) {
	sign, _ := hex.DecodeString("B94D27B9934D3E08A52E52D7DA7DABFAC484EFE37A5380EE9088F7ACE2EFCDE9")

	kcpHeader := KCPPacketHeader{
		BLRQ:      [4]byte{0x42, 0x4C, 0x52, 0x51},
		Timestamp: 1533189652,
		Length:    0x07FC,
	}

	copy(kcpHeader.Signature[0:], sign)

	t.Run("Header Encoding", func(t *testing.T) {
		var buf bytes.Buffer

		binary.Write(&buf, binary.LittleEndian, kcpHeader)

		t.Logf("% x", buf.Bytes())
	})
}

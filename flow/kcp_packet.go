package flow

import (
	"bytes"
	"encoding/binary"
)

type KCPPacketHeader struct {
	BLRQ      [4]byte // 'BLRQ'
	Length    uint32  // 'Network Order'
	Timestamp uint64  // 'Network Order'
	Signature [32]byte
}

func (p KCPPacketHeader) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	_, err := buf.Write(p.BLRQ[0:])

	if err != nil {
		return nil, err
	}

	err = binary.Write(&buf, binary.BigEndian, p.Length)

	if err != nil {
		return nil, err
	}

	err = binary.Write(&buf, binary.BigEndian, p.Timestamp)

	if err != nil {
		return nil, err
	}

	_, err = buf.Write(p.Signature[0:])

	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (p *KCPPacketHeader) UnmarshalBinary(data []byte) error {

	var packet KCPPacketHeader

	buf := bytes.NewReader(data)

	_, err := buf.Read(packet.BLRQ[0:])

	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.BigEndian, &packet.Length)

	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.BigEndian, &packet.Timestamp)

	if err != nil {
		return err
	}

	_, err = buf.Read(packet.Signature[0:])

	if err != nil {
		return err
	}

	*p = packet

	return nil
}

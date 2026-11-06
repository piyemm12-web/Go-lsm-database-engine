package wal

import "errors"

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
)

var (
	ErrCorruptedEntry   = errors.New("wal entry is too short or malformed")
	ErrChecksumMismatch = errors.New("wal entry corrupted: checksum mismatch")
)

type Entry struct{
	Key []byte
	Value []byte
}

func (e *Entry) Serialize() ([]byte, error) {
	keySize := uint32(len(e.Key))
	valSize := uint32(len(e.Value))

	buf := new(bytes.Buffer)

	dataBuf := new(bytes.Buffer)
	if err := binary.Write(dataBuf, binary.BigEndian, keySize); err != nil {
		return nil, err
	}
	if err := binary.Write(dataBuf, binary.BigEndian, valSize); err != nil {
		return nil, err
	}
	if _, err := dataBuf.Write(e.Key); err != nil {
		return nil, err
	}
	if _, err := dataBuf.Write(e.Value); err != nil {
		return nil, err
	}

	rawPayload := dataBuf.Bytes()

	checksum := crc32.ChecksumIEEE(rawPayload)

	if err := binary.Write(buf, binary.BigEndian, checksum); err != nil {
		return nil, err
	}
	if _, err := buf.Write(rawPayload); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func Deserialize(data []byte) (*Entry, error) {

	if len(data) < 12 {
		return nil, ErrCorruptedEntry 
	}

	reader := bytes.NewReader(data)

	var storedChecksum uint32
	if err := binary.Read(reader, binary.BigEndian, &storedChecksum); err != nil {
		return nil, err
	}

	rawPayload := data[4:]

	calculatedChecksum := crc32.ChecksumIEEE(rawPayload)
	if calculatedChecksum != storedChecksum {
		return nil, ErrChecksumMismatch
	}

	payloadReader := bytes.NewReader(rawPayload)
	var keySize, valSize uint32
	binary.Read(payloadReader, binary.BigEndian, &keySize)
	binary.Read(payloadReader, binary.BigEndian, &valSize)

	key := make([]byte, keySize)
	if len(key) > 0 {
		if _, err := io.ReadFull(payloadReader, key); err != nil {
			return nil, err
		}
	}

	value := make([]byte, valSize)
	if len(value) > 0 {
		if _, err := io.ReadFull(payloadReader, value); err != nil {
			return nil, err
		}
	}

	return &Entry{Key: key, Value: value}, nil
}
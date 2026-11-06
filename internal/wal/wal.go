package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func OpenWAL(filePath string) (*WAL, error) {

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

func (w *WAL) Append(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := entry.Serialize()
	if err != nil {
		return err
	}

	if _, err := w.file.Write(data); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (w *WAL) ReadAll() ([]*Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var entries []*Entry

	for {
		headerBuf := make([]byte, 12)
		_, err := io.ReadFull(w.file, headerBuf)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		keySize := binary.BigEndian.Uint32(headerBuf[4:8])
		valSize := binary.BigEndian.Uint32(headerBuf[8:12])

		payloadSize := keySize + valSize
		payloadBuf := make([]byte, payloadSize)
		if _, err := io.ReadFull(w.file, payloadBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break 
			}
			return nil, err
		}

		fullRecord := append(headerBuf, payloadBuf...)
		entry, err := Deserialize(fullRecord)
		if err != nil {
			return nil, err 
		}

		entries = append(entries, entry)
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return entries, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
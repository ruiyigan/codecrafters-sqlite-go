package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

func readBytesAtOffset(file *os.File, offset int64, numBytes int) ([]byte, error) {
	_, err := file.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking to offset %d: %v", offset, err)
	}

	buffer := make([]byte, numBytes) // array of bytes

	n, err := file.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("error reading %d bytes at offset %d: %v", numBytes, offset, err)
	}
	if n != numBytes {
		return nil, fmt.Errorf("expected to read %d bytes, but read %d", numBytes, n)
	}

	return buffer, nil
}

func getNumTablesInBTree(databaseFile *os.File, pageNumber int32, pageSize int32) int {
	numTables := 0
	const headerSize int32 = 100
	pageOffset := (pageNumber-1)*pageSize + headerSize

	data, err := readBytesAtOffset(databaseFile, int64(pageOffset), 1)
	if err != nil {
		return 0 // Consider proper error handling
	}

	switch data[0] {
	case 0x0D: // Leaf page
		data, err = readBytesAtOffset(databaseFile, int64(pageOffset+3), 2)
		if err != nil {
			return 0
		}
		cellCount := binary.BigEndian.Uint16(data)
		numTables += int(cellCount)

	case 0x05: // Interior page
		data, err = readBytesAtOffset(databaseFile, int64(pageOffset+3), 2)
		if err != nil {
			return 0
		}
		cellCount := binary.BigEndian.Uint16(data)

		for i := int32(0); i < int32(cellCount); i++ {
			cellPointerOffset := pageOffset + 12 + (i * 2)
			data, err = readBytesAtOffset(databaseFile, int64(cellPointerOffset), 2)
			if err != nil {
				continue
			}
			cellContentOffset := pageOffset + int32(binary.BigEndian.Uint16(data))

			data, err = readBytesAtOffset(databaseFile, int64(cellContentOffset), 4)
			if err != nil {
				continue
			}
			leftChildPageNumber := int32(binary.BigEndian.Uint32(data))
			numTables += getNumTablesInBTree(databaseFile, leftChildPageNumber, pageSize)
		}

		// Rightmost pointer
		data, err = readBytesAtOffset(databaseFile, int64(pageOffset+8), 4)
		if err != nil {
			return numTables
		}
		rightChildPageNumber := int32(binary.BigEndian.Uint32(data))
		numTables += getNumTablesInBTree(databaseFile, rightChildPageNumber, pageSize)
	}

	return numTables
}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}
		defer databaseFile.Close() // Ensure file is closed

		header := make([]byte, 100)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		// Task 1: Getting page size
		var pageSize int16 // since reading two bytes
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		// Task 1: Getting number of tables
		var numTables int = getNumTablesInBTree(databaseFile, 1, int32(pageSize))

		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		fmt.Printf("database page size: %v", pageSize)
		fmt.Printf("number of tables: %v", numTables)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

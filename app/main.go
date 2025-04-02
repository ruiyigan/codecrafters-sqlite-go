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

func readVarint(data []byte, index int) (value int64, bytesRead int32) {
	if index >= len(data) {
		return 0, 0
	}

	maxBytes := 9
	if index+maxBytes > len(data) {
		maxBytes = len(data) - index
	}

	value = 0
	bytesRead = 0

	for i := 0; i < maxBytes && i < 9; i++ {
		bytesRead++

		// For the first 8 bytes, we only use the lower 7 bits
		if i < 8 {
			// Shift the existing value left by 7 bits and add the lower 7 bits of the current byte
			value = (value << 7) | int64(data[index+i]&0x7F)

			// If the high bit is not set, we've reached the end of the varint
			if (data[index+i] & 0x80) == 0 {
				return value, bytesRead
			}
		} else {
			// For the 9th byte, we use all 8 bits
			value = (value << 8) | int64(data[index+i])
			return value, bytesRead
		}
	}

	// If we've read 9 bytes or reached the end of the data, return what we have
	return value, bytesRead
}

func getSerialTypeSize(serialType int64) int {
	switch {
	case serialType == 0, serialType == 8, serialType == 9:
		return 0
	case serialType == 1:
		return 1
	case serialType == 2:
		return 2
	case serialType == 3:
		return 3
	case serialType == 4:
		return 4
	case serialType == 5:
		return 6
	case serialType == 6, serialType == 7:
		return 8
	case serialType >= 12 && serialType%2 == 0: // BLOB
		return int((serialType - 12) / 2)
	case serialType >= 13 && serialType%2 == 1: // String
		return int((serialType - 13) / 2)
	}
	return 0
}

func getTablesNamesInBTree(databaseFile *os.File, pageNumber int32, pageSize int32) []string {
	var tables []string
	var pageOffset int32
	if pageNumber == 1 {
		pageOffset = (pageNumber-1)*pageSize + 100
	} else {
		pageOffset = (pageNumber - 1) * pageSize
	}

	data, err := readBytesAtOffset(databaseFile, int64(pageOffset), 1)
	if err != nil {
		return tables
	}

	switch data[0] {
	case 0x0D: // Leaf page
		data, err = readBytesAtOffset(databaseFile, int64(pageOffset+3), 2)
		if err != nil {
			return tables
		}
		cellCount := binary.BigEndian.Uint16(data)
		// Task 2: Read table names

		// loop through cell count
		for i := int32(0); i < int32(cellCount); i++ {
			cellPointerOffset := pageOffset + 8 + (i * 2)
			data, err = readBytesAtOffset(databaseFile, int64(cellPointerOffset), 2)
			if err != nil {
				continue
			}
			cellContentOffset := int32(binary.BigEndian.Uint16(data)) // offset in the cell array is relative to 0

			// [varint] read size of the record
			data, _ = readBytesAtOffset(databaseFile, int64(cellContentOffset), 9) // read the size of the record
			recordSize, bytesReadRecordSize := readVarint(data, 0)

			// [varint] read size of rowid
			data, _ = readBytesAtOffset(databaseFile, int64(cellContentOffset+bytesReadRecordSize), 9)
			_, bytesReadRowId := readVarint(data, 0)

			// data here is the record data (with header) in bytes array
			data, _ = readBytesAtOffset(databaseFile, int64(cellContentOffset+bytesReadRecordSize+bytesReadRowId), int(recordSize))

			// [varint] read record header (use the first varint to determine how many times to read, ie how many values)
			headerSize, bytesReadHeader := readVarint(data, 0)
			headerOffset := bytesReadHeader
			bodyOffset := int64(headerSize) // Body starts after the header
			bytesLeft := int32(headerSize) - bytesReadHeader

			var serialTypes []int64
			for int32(headerOffset) < bytesLeft {
				serialType, bytesRead := readVarint(data, int(headerOffset))
				headerOffset += bytesRead
				serialTypes = append(serialTypes, serialType)
			}

			for colIdx, serialType := range serialTypes {
				size := getSerialTypeSize(serialType)
				value := data[bodyOffset : bodyOffset+int64(size)]

				var strValue string
				if serialType >= 13 && serialType%2 == 1 { // String
					strValue = string(value) // No null terminator in SQLite strings
				} else if serialType == 1 { // 8-bit integer
					strValue = fmt.Sprintf("%d", value[0])
				} // Add other types as needed (e.g., 4 for 32-bit int, etc.)

				if colIdx == 2 { // tbl_name column
					if serialType >= 13 && serialType%2 == 1 {
						tables = append(tables, strValue)
					}
				}

				bodyOffset += int64(size)
			}

		}

		// return tables
		return tables

	case 0x05: // Interior page
		data, err = readBytesAtOffset(databaseFile, int64(pageOffset+3), 2)
		if err != nil {
			return tables
		}
		cellCount := binary.BigEndian.Uint16(data)

		for i := int32(0); i < int32(cellCount); i++ {
			cellPointerOffset := pageOffset + 12 + (i * 2)
			data, err = readBytesAtOffset(databaseFile, int64(cellPointerOffset), 2)
			if err != nil {
				continue
			}
			cellContentOffset := int32(binary.BigEndian.Uint16(data))

			data, err = readBytesAtOffset(databaseFile, int64(cellContentOffset), 4)
			if err != nil {
				continue
			}
			leftChildPageNumber := int32(binary.BigEndian.Uint32(data))
			tempNames := getTablesNamesInBTree(databaseFile, leftChildPageNumber, pageSize)
			tables = append(tables, tempNames...)

		}

		// Rightmost pointer
		data, err = readBytesAtOffset(databaseFile, int64(pageOffset+8), 4)
		if err != nil {
			return tables
		}
		rightChildPageNumber := int32(binary.BigEndian.Uint32(data))
		tempNames := getTablesNamesInBTree(databaseFile, rightChildPageNumber, pageSize)
		tables = append(tables, tempNames...)
	}

	return tables
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

	case ".tables":
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
		var pageSize int16 // since reading two bytes
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}
		// Task 2: Get names of tables
		tableNames := getTablesNamesInBTree(databaseFile, 1, int32(pageSize))

		for i, name := range tableNames {
			if i != len(tableNames)-1 {
				fmt.Printf("%s", name+" ")
			} else {
				fmt.Print(name)
			}
		}

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

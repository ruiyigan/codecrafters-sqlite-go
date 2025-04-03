package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// HELPERS
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

func processSerialType(serialType int64, value []byte) string {
	var strValue string

	switch serialType {
	case 0: // NULL
		strValue = "NULL"
	case 1: // 8-bit twos-complement integer
		strValue = fmt.Sprintf("%d", int8(value[0]))
	case 2: // 16-bit twos-complement integer (big-endian)
		num := int16(binary.BigEndian.Uint16(value))
		strValue = fmt.Sprintf("%d", num)
	case 3: // 24-bit twos-complement integer (big-endian)
		num := int32(binary.BigEndian.Uint32(append([]byte{0}, value[:3]...))) >> 8 // Pad to 32-bit and shift
		strValue = fmt.Sprintf("%d", num)
	case 4: // 32-bit twos-complement integer (big-endian)
		num := int32(binary.BigEndian.Uint32(value))
		strValue = fmt.Sprintf("%d", num)
	case 5: // 48-bit twos-complement integer (big-endian)
		num := int64(binary.BigEndian.Uint64(append([]byte{0, 0}, value[:6]...))) >> 16 // Pad to 64-bit and shift
		strValue = fmt.Sprintf("%d", num)
	case 6: // 64-bit twos-complement integer (big-endian)
		num := int64(binary.BigEndian.Uint64(value))
		strValue = fmt.Sprintf("%d", num)
	case 7: // 64-bit IEEE 754-2008 floating point (big-endian)
		bits := binary.BigEndian.Uint64(value)
		num := math.Float64frombits(bits)
		strValue = fmt.Sprintf("%f", num)
	case 8: // Integer 0
		strValue = "0"
	case 9: // Integer 1
		strValue = "1"
	case 10, 11: // Reserved for internal use
		strValue = fmt.Sprintf("Reserved(%d)", serialType)
	default:
		if serialType >= 12 && serialType%2 == 0 { // BLOB (N-12)/2 bytes
			blobLen := (serialType - 12) / 2
			if int64(len(value)) >= blobLen {
				strValue = fmt.Sprintf("BLOB(%d bytes)", blobLen)
				// Optionally: strValue = hex.EncodeToString(value[:blobLen]) for hex representation
			} else {
				strValue = "Invalid BLOB"
			}
		} else if serialType >= 13 && serialType%2 == 1 { // String (N-13)/2 bytes
			strLen := (serialType - 13) / 2
			if int64(len(value)) >= strLen {
				strValue = string(value[:strLen]) // No null terminator
			} else {
				strValue = "Invalid String"
			}
		} else {
			strValue = "Unknown Type"
		}
	}

	return strValue
}

func getCellCount(databaseFile *os.File, pageOffset int32) uint16 {
	data, err := readBytesAtOffset(databaseFile, int64(pageOffset+3), 2)
	if err != nil {
		return 0
	}
	cellCount := binary.BigEndian.Uint16(data)
	return cellCount
}

func getRightmostChildPageNumber(databaseFile *os.File, pageOffset int32) int32 {
	data, err := readBytesAtOffset(databaseFile, int64(pageOffset+8), 4)
	if err != nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(data))
}

func getCellContentOffset(databaseFile *os.File, cellPointerOffset int32) int32 {
	data, err := readBytesAtOffset(databaseFile, int64(cellPointerOffset), 2)
	if err != nil {
		return 0
	}
	return int32(binary.BigEndian.Uint16(data)) // offset in the cell array is relative to 0
}

func processLeafCellRecord(databaseFile *os.File, cellContentOffset int32) ([]byte, []int64, int64) {
	// [varint] read size of the record
	data, err := readBytesAtOffset(databaseFile, int64(cellContentOffset), 9)
	if err != nil {
		return nil, nil, 0
	}
	recordSize, bytesReadRecordSize := readVarint(data, 0)

	// [varint] read size of rowid
	data, err = readBytesAtOffset(databaseFile, int64(cellContentOffset+bytesReadRecordSize), 9)
	if err != nil {
		return nil, nil, 0
	}
	_, bytesReadRowId := readVarint(data, 0)

	// Read the record data (with header)
	recordOffset := cellContentOffset + bytesReadRecordSize + bytesReadRowId
	data, err = readBytesAtOffset(databaseFile, int64(recordOffset), int(recordSize))
	if err != nil {
		return nil, nil, 0
	}

	// [varint] Parse record header
	headerSize, bytesReadHeader := readVarint(data, 0)
	headerOffset := bytesReadHeader
	bodyOffset := int64(headerSize) // Body starts after the header
	bytesLeft := int32(headerSize) - bytesReadHeader

	// Parse serial types
	var serialTypes []int64
	for int32(headerOffset) < bytesLeft {
		serialType, bytesRead := readVarint(data, int(headerOffset))
		headerOffset += bytesRead
		serialTypes = append(serialTypes, serialType)
	}

	return data, serialTypes, bodyOffset
}

// PROCESS
func getTablesNamesInBTree(databaseFile *os.File, pageNumber int32, pageSize int32) []string {
	var tables []string
	const headerSize int32 = 100
	var pageOffset int32 = (pageNumber - 1) * pageSize
	if pageNumber == 1 {
		pageOffset += headerSize
	}

	data, err := readBytesAtOffset(databaseFile, int64(pageOffset), 1)
	if err != nil {
		return tables
	}

	switch data[0] {
	case 0x0D: // Leaf page
		cellCount := getCellCount(databaseFile, pageOffset)
		// Task 2: Read table names

		// loop through cell count
		for i := int32(0); i < int32(cellCount); i++ {
			cellPointerOffset := pageOffset + 8 + (i * 2)
			cellContentOffset := getCellContentOffset(databaseFile, cellPointerOffset)

			data, serialTypes, bodyOffset := processLeafCellRecord(databaseFile, cellContentOffset)

			for colIdx, serialType := range serialTypes {
				size := getSerialTypeSize(serialType)
				value := data[bodyOffset : bodyOffset+int64(size)]

				strValue := processSerialType(serialType, value)
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
		cellCount := getCellCount(databaseFile, pageOffset)

		for i := int32(0); i < int32(cellCount); i++ {
			cellPointerOffset := pageOffset + 12 + (i * 2)
			cellContentOffset := getCellContentOffset(databaseFile, cellPointerOffset)

			data, err = readBytesAtOffset(databaseFile, int64(cellContentOffset), 4)
			if err != nil {
				continue
			}
			leftChildPageNumber := int32(binary.BigEndian.Uint32(data))
			tempNames := getTablesNamesInBTree(databaseFile, leftChildPageNumber, pageSize)
			tables = append(tables, tempNames...)

		}

		// Rightmost pointer
		rightChildPageNumber := getRightmostChildPageNumber(databaseFile, pageOffset)
		tempNames := getTablesNamesInBTree(databaseFile, rightChildPageNumber, pageSize)
		tables = append(tables, tempNames...)
	}

	return tables
}

func getCountInATable(databaseFile *os.File, pageNumber int32, pageSize int32, tableName string) int {
	const headerSize int32 = 100
	var pageOffset int32 = (pageNumber - 1) * pageSize
	if pageNumber == 1 {
		pageOffset += headerSize
	}

	data, err := readBytesAtOffset(databaseFile, int64(pageOffset), 1)
	if err != nil {
		return 0
	}

	switch data[0] {
	case 0x0D: // Leaf page
		cellCount := getCellCount(databaseFile, pageOffset)
		// Task 2: Read table names

		// loop through cell count
		for i := int32(0); i < int32(cellCount); i++ {
			cellPointerOffset := pageOffset + 8 + (i * 2)
			cellContentOffset := getCellContentOffset(databaseFile, cellPointerOffset) // offset in the cell array is relative to 0

			data, serialTypes, bodyOffset := processLeafCellRecord(databaseFile, cellContentOffset)

			var foundTable bool = false
			for colIdx, serialType := range serialTypes {
				size := getSerialTypeSize(serialType)
				value := data[bodyOffset : bodyOffset+int64(size)]

				strValue := processSerialType(serialType, value)

				if colIdx == 2 { // tbl_name column
					if serialType >= 13 && serialType%2 == 1 {
						if strValue == tableName {
							foundTable = true
						}
					}
				}
				if colIdx == 3 && foundTable {
					// get root page
					num, _ := strconv.Atoi(strValue)
					count := countRecordsInBTree(databaseFile, int32(num), pageSize)
					return count
				}

				bodyOffset += int64(size)
			}

		}

		return 0

	case 0x05: // Interior page
		cellCount := getCellCount(databaseFile, pageOffset)

		for i := int32(0); i < int32(cellCount); i++ {
			cellPointerOffset := pageOffset + 12 + (i * 2)
			cellContentOffset := getCellContentOffset(databaseFile, cellPointerOffset)

			data, err = readBytesAtOffset(databaseFile, int64(cellContentOffset), 4)
			if err != nil {
				continue
			}
			leftChildPageNumber := int32(binary.BigEndian.Uint32(data))
			return getCountInATable(databaseFile, leftChildPageNumber, pageSize, tableName)

		}

		// Rightmost pointer
		rightChildPageNumber := getRightmostChildPageNumber(databaseFile, pageOffset)
		return getCountInATable(databaseFile, rightChildPageNumber, pageSize, tableName)
	}

	return 0
}

func countRecordsInBTree(databaseFile *os.File, pageNumber int32, pageSize int32) int {
	numTables := 0
	const headerSize int32 = 100
	var pageOffset int32 = (pageNumber - 1) * pageSize
	if pageNumber == 1 {
		pageOffset += headerSize
	}

	data, err := readBytesAtOffset(databaseFile, int64(pageOffset), 1)
	if err != nil {
		return 0 // Consider proper error handling
	}

	switch data[0] {
	case 0x0D: // Leaf page
		cellCount := getCellCount(databaseFile, pageOffset)
		numTables += int(cellCount)

	case 0x05: // Interior page
		cellCount := getCellCount(databaseFile, pageOffset)

		for i := int32(0); i < int32(cellCount); i++ {
			cellPointerOffset := pageOffset + 12 + (i * 2)
			cellContentOffset := getCellContentOffset(databaseFile, cellPointerOffset)

			data, err = readBytesAtOffset(databaseFile, int64(cellContentOffset), 4)
			if err != nil {
				continue
			}
			leftChildPageNumber := int32(binary.BigEndian.Uint32(data))
			numTables += countRecordsInBTree(databaseFile, leftChildPageNumber, pageSize)
		}

		// Rightmost pointer
		rightChildPageNumber := getRightmostChildPageNumber(databaseFile, pageOffset)
		numTables += countRecordsInBTree(databaseFile, rightChildPageNumber, pageSize)
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
		var pageSize uint16 // since reading two bytes
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		// Task 1: Getting number of tables
		var numTables int = countRecordsInBTree(databaseFile, 1, int32(pageSize)) // Page 1 or root page stores the tables in the BTree

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
		var pageSize uint16 // since reading two bytes
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
		var pageSize uint16 // since reading two bytes
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		// Task 3: Process Count Command
		words := strings.Fields(command)
		tableName := words[len(words)-1]
		numRows := getCountInATable(databaseFile, 1, int32(pageSize), tableName)
		fmt.Printf("%d\n", numRows)
		os.Exit(1)
	}
}

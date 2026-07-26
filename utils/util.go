package utils

import (
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

func ExtractNumber(s string) int {
	numStr := strings.TrimPrefix(s, "wal_")
	num, err := strconv.Atoi(numStr)

	if err != nil {
		log.Println(`failed to get num`, err)
	}

	return num
}

func SortFilesArray(files []os.DirEntry) []os.DirEntry {
	sort.Slice(files, func(i, j int) bool {
		file_1, err := files[i].Info()

		if err != nil {
			log.Println("issue in reading file no :=", i, ",", err)
		}

		file_2, err := files[j].Info()

		if err != nil {
			log.Println("issue in reading file no :=", j, ",", err)
		}
		return ExtractNumber(file_1.Name()) < ExtractNumber(file_2.Name())
	})

	return files
}

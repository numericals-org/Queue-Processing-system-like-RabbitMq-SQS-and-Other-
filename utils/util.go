package utils

import (
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/numericals/queueSys/types"
)

func ExtractNumber(s string) int {
	numStr := strings.TrimPrefix(s, "wal_")
	numStr = strings.TrimSuffix(numStr, ".log")
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

func TestName(name string, pattern string) bool {
	re := regexp.MustCompile(pattern)
	got := re.MatchString(name)
	return got
}

func WithRequestConfig(cfg types.QueueConfig) types.QueueOption {
	return func(c *types.QueueConfig) {
		*c = cfg // Overwrites the defaults with the request values
	}
}

package utils

import (
	"log"
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

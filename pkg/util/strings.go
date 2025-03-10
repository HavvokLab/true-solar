package util

import "strings"

const EmptyString = ""

func IsEmpty(s string) bool {
	return strings.TrimSpace(s) == EmptyString
}

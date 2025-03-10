package solarman

import (
	"crypto/sha256"
	"fmt"
)

func DecodePassword(password string) string {
	hashPassword := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", hashPassword[:])
}

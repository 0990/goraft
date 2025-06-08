package util

import uuid "github.com/satori/go.uuid"

func GenerateUUID() uuid.UUID {
	id := uuid.NewV1()
	return id
}

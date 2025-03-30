package main

import "github.com/TohaMakarenko/FlowGate/shared"

type MessageRepository struct {
}

func (m MessageRepository) SaveMessage(msg *shared.Message) bool {
	return true
}

func (m MessageRepository) SaveMessageResult(msgResult *MessageResult) bool {
	return true
}

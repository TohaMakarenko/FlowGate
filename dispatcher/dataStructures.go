package main

import "time"

type MessageResult struct {
	MessageId    string
	ResultCode   uint16
	ResponseBody []byte
	Error        string
	CreatedAt    time.Time
}

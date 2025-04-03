package main

import "time"

type MessageResult struct {
	MessageId    string
	ResultCode   int
	ResponseBody []byte
	Error        string
	CreatedAt    time.Time
}

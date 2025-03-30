package main

type MessageResult struct {
	MessageId    string
	ResultCode   int
	ResponseBody []byte
	Error        string
}

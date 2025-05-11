package main

import (
	"errors"
	"github.com/xyproto/randomstring"
)

// todo: store tokens in db with cache
var userByToken map[string]int = make(map[string]int)

func createToken(userId int) (string, error) {
	newToken := randomstring.CookieFriendlyString(32)

	userByToken[newToken] = userId

	return newToken, nil
}

func getUserByToken(token string) (int, error) {
	userId, exist := userByToken[token]

	if !exist {
		return -1, errors.New("Token not found")
	}

	return userId, nil
}

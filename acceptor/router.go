package main

import (
	"github.com/gin-gonic/gin"
	"io"
	"log/slog"
	"strconv"
)

func InitRouter(router *gin.Engine) {
	router.POST("/v1/api-token/create", createApiTokenRoute)
	router.POST("/v1/event/submit", submitEvent)
}

func createApiTokenRoute(ctx *gin.Context) {
	// todo: read user id from auth header
	userIdStr := ctx.GetHeader("user_id")

	if len(userIdStr) == 0 {
		slog.Error("User id not found in request - unauthorized")
		ctx.Status(404)
		return
	}

	userId, err := strconv.Atoi(userIdStr)

	if err != nil {
		slog.Error("User id malformed.", "userId", userIdStr, "error", err)
		ctx.Status(400)
		return
	}

	token, err := createToken(userId)

	slog.Info("Successfully generated API token.", "userId", userId)
	ctx.JSON(200, gin.H{"api-token": token})
}

func submitEvent(ctx *gin.Context) {
	apiToken := ctx.GetHeader("api-token")

	if len(apiToken) == 0 {
		slog.Error("Api token missing")
		ctx.Status(404)
		return
	}

	userId, err := getUserByToken(apiToken)

	if err != nil {
		slog.Error("Token is invalid or expired.")
		ctx.String(400, "API token is invalid or expired.")
		return
	}

	eventType := ctx.GetHeader("event-type")

	if len(eventType) == 0 {
		slog.Error("Missing event-type header.", "userId", userId)
		ctx.String(400, "event-type header is required")
		return
	}

	eventBody, _ := io.ReadAll(ctx.Request.Body)

	_, err = submitEventToQueue(ctx, userId, eventType, eventBody)

	if err != nil {
		slog.Error("Unexpected error.", "userId", userId, "err", err)
		ctx.Status(500)
		return
	}

	ctx.Status(200)
}

// Copyright 2019 The ChubaoFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package objectnode

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/proto"

	"github.com/gorilla/mux"
)

const (
	ContextKeyRequestID     = "ctx_request_id"
	ContextKeyRequestAction = "ctx_request_action"
	ContextKeyStatusCode    = "status_code"
	ContextKeyErrorMessage  = "error_message"
)

type Context struct{
	parent context.Context
	valMap map[any]any
}

func (c *Context) Deadline() (deadline time.Time, ok bool) {
	return c.parent.Deadline()
}

func (c *Context) Done() <-chan struct{} {
	return c.Done()
}

func (c *Context) Err() error {
	return c.Err()
}

func (c *Context) Value(key any) any {
	val, found := c.valMap[key]
	if found {
		return val
	}
	return c.parent.Value(key)
}

func (c *Context) Set(key any, val any) {
	c.valMap[key] = val
}

func NewContext(parent context.Context) *Context {
	return &Context{
		parent: parent,
		valMap: make(map[any]any),
	}
}

func NewContextFromRequest(r *http.Request) context.Context {
	ctx := NewContext(context.Background())
	ctx.Set(ContextKeyRequestID, GetRequestID(r))
	ctx.Set(ContextKeyRequestAction, GetActionFromContext(r))
	return ctx
}

func RequestIdentityFromContext(ctx context.Context) string {
	var (
		requestID string
		action    string
	)
	if ctx != nil {
		if val, ok := ctx.Value(ContextKeyRequestID).(string); ok && len(val) > 0 {
			requestID = val
		}
		if val, ok := ctx.Value(ContextKeyRequestAction).(proto.Action); ok {
			action = val.Name()
		}
	}
	return fmt.Sprintf("requestID(%s) action(%s)", requestID, action)
}

func SetRequestID(r *http.Request, requestID string) {
	mux.Vars(r)[ContextKeyRequestID] = requestID
}

func GetRequestID(r *http.Request) (id string) {
	return mux.Vars(r)[ContextKeyRequestID]
}

func SetRequestAction(r *http.Request, action proto.Action) {
	mux.Vars(r)[ContextKeyRequestAction] = action.String()
}

func GetActionFromContext(r *http.Request) (action proto.Action) {
	return proto.ParseAction(mux.Vars(r)[ContextKeyRequestAction])
}

func SetResponseStatusCode(r *http.Request, statusCode int) {
	mux.Vars(r)[ContextKeyStatusCode] = strconv.Itoa(statusCode)
}

func GetStatusCodeFromContext(r *http.Request) int {
	code, err := strconv.Atoi(mux.Vars(r)[ContextKeyStatusCode])
	if err == nil {
		return code
	}
	return 0
}

func SetResponseErrorMessage(r *http.Request, message string) {
	mux.Vars(r)[ContextKeyErrorMessage] = message
}

func getResponseErrorMessage(r *http.Request) string {
	return mux.Vars(r)[ContextKeyErrorMessage]
}


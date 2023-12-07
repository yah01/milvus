// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracer

import (
	"context"

	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// WithLogCancel returns a context and a cancel function,
// the cancel function will log a message including the trace ID when called.
func WithLogCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	logCancel := func() {
		log.Ctx(ctx).Info("context canceled", zap.Stack("stack"))
		cancel()
	}

	return ctx, logCancel
}

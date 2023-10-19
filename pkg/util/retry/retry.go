// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package retry

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// Do will run function with retry mechanism.
// fn is the func to run.
// Option can control the retry times and timeout.
func Do(ctx context.Context, fn func() error, opts ...Option) error {
	if !funcutil.CheckCtxValid(ctx) {
		return ctx.Err()
	}

	log := log.Ctx(ctx)
	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}

	var err error
	for i := uint(0); i < c.attempts; i++ {
		if err = fn(); err != nil {
			if i%4 == 0 {
				log.Error("retry func failed", zap.Uint("retry time", i), zap.Error(err))
			}

			err = errors.Wrapf(err, "attempt #%d", i)

			if !merr.IsRetryableErr(err) {
				return err
			}

			select {
			case <-time.After(c.sleep):
			case <-ctx.Done():
				return err
			}

			c.sleep *= 2
			if c.sleep > c.maxSleepTime {
				c.sleep = c.maxSleepTime
			}
		} else {
			return nil
		}
	}
	return err
}

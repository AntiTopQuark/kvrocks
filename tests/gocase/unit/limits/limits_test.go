/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package limits

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestNetworkLimits(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"maxclients": "10",
	})
	defer srv.Close()

	t.Run("check if maxclients works refusing connections", func(t *testing.T) {
		var clean []func()
		defer func() {
			for _, f := range clean {
				f()
			}
		}()

		for i := 0; i < 50; i++ {
			c := srv.NewTCPClient()
			clean = append(clean, func() { require.NoError(t, c.Close()) })
			require.NoError(t, c.WriteArgs("PING"))
			r, err := c.ReadLine()
			require.NoError(t, err)
			if strings.Contains(r, "ERR") {
				require.Regexp(t, ".*ERR max.*reached.*", r)
				require.Contains(t, []int{9, 10}, i)
				return
			}
			require.Equal(t, "+PONG", r)
		}

		require.Fail(t, "maxclients doesn't work refusing connections")
	})
}

<<<<<<< HEAD
func getEvictedClients(rdb *redis.Client, ctx context.Context) (int, error) {
	info, err := rdb.Info(ctx, "stats").Result()
	if err != nil {
		return 0, err
	}

	lines := strings.Split(info, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "evicted_clients:") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				return strconv.Atoi(strings.TrimSpace(parts[1]))
			}
		}
	}
	return 0, fmt.Errorf("evicted_clients not found")
}
=======
>>>>>>> 3ce53f66 (feat: support maxmemory-clients)

func TestMaxMemoryClientsLimits(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"maxmemory-clients": "10m",
	})
	defer srv.Close()

	t.Run("check if maxmemory-clients works well", func(t *testing.T) {
		var clean []func()
		defer func() {
			for _, f := range clean {
				f()
			}
		}()

		ctx := context.Background()
		rdbA := srv.NewClient()
		defer rdbA.Close()
		elem := strings.Repeat("a", 10240)
		for i := 0; i < 1024; i++ {
			require.NoError(t, rdbA.RPush(ctx, "test_max_memory_clients", elem).Err())
		}

		rdbB := srv.NewClient()
		defer rdbB.Close()
		require.NoError(t, rdbB.LRange(ctx, "test_max_memory_clients", 0, -1).Err())

		time.Sleep(25 * time.Second)

		r, err := getEvictedClients(rdbA, ctx)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})
}

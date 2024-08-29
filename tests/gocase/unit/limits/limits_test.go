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

func getClientOutputBufferLimitDisconnections(rdb *redis.Client, ctx context.Context) (int, error) {
	info, err := rdb.Info(ctx, "stats").Result()
	if err != nil {
		return 0, err
	}

	lines := strings.Split(info, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "client_output_buffer_limit_disconnections:") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				return strconv.Atoi(strings.TrimSpace(parts[1]))
			}
		}
	}

	return 0, fmt.Errorf("client_output_buffer_limit_disconnections not found")
}

func TestClientOutputBufferLimitsNormal(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"client-output-buffer-limit": "normal 128K 64k 3 replica 0 0 0 pubsub 0 0 0",
	})
	defer srv.Close()

	t.Run("check if client-output-buffer-limit normal limit works", func(t *testing.T) {
		ctx := context.Background()
		rdb := srv.NewClient()
		defer rdb.Close()

		elem := strings.Repeat("a", 1024)
		for i := 0; i < 130; i++ {
			require.NoError(t, rdb.RPush(ctx, "test_client_obuf_limit", elem).Err())
		}

		oldDisconnections, err := getClientOutputBufferLimitDisconnections(rdb, ctx)
		require.NoError(t, err)
		r := rdb.LRange(ctx, "test_client_obuf_limit", 0, -1)
		require.NoError(t, r.Err())
		time.Sleep(10 * time.Second)
		newDisconnections, err := getClientOutputBufferLimitDisconnections(rdb, ctx)
		require.NoError(t, err)
		require.Equal(t, true, newDisconnections > oldDisconnections)

		oldDisconnections, err = getClientOutputBufferLimitDisconnections(rdb, ctx)
		require.NoError(t, err)
		r = rdb.LRange(ctx, "test_client_obuf_limit", 0, 34)
		require.NoError(t, r.Err())
		time.Sleep(10 * time.Second)
		newDisconnections, err = getClientOutputBufferLimitDisconnections(rdb, ctx)
		require.NoError(t, err)
		require.Equal(t, oldDisconnections+1, newDisconnections)
	})
}

func TestClientOutputBufferLimitsReplica(t *testing.T) {
	master := util.StartServer(t, map[string]string{
		"client-output-buffer-limit": "normal 0 0 0 replica 1150 0 0 pubsub 0 0 0",
	})
	defer master.Close()
	replica := util.StartServer(t, map[string]string{})
	defer replica.Close()

	t.Run("check if client-output-buffer-limit replica limit works", func(t *testing.T) {
		ctx := context.Background()
		rdbMaster := master.NewClient()
		defer rdbMaster.Close()
		rdbReplica := replica.NewClient()
		defer rdbReplica.Close()

		util.SlaveOf(t, rdbReplica, master)

		oldDisconnections, err := getClientOutputBufferLimitDisconnections(rdbMaster, ctx)
		require.NoError(t, err)

		elem := strings.Repeat("a", 1024)
		for i := 0; i < 32; i++ {
			require.NoError(t, rdbMaster.RPush(ctx, "test_client_obuf_limit", elem).Err())
		}
		time.Sleep(10 * time.Second)

		newDisconnections, err := getClientOutputBufferLimitDisconnections(rdbMaster, ctx)
		require.NoError(t, err)
		require.Equal(t, true, newDisconnections > oldDisconnections)
	})
}

func TestClientOutputBufferLimitsPubsub(t *testing.T) {
	server := util.StartServer(t, map[string]string{
		"client-output-buffer-limit": "normal 0 0 0 replica 0 0 0 pubsub 3k 0 0",
	})
	defer server.Close()

	t.Run("check if client-output-buffer-limit pubsub limit works", func(t *testing.T) {
		ctx := context.Background()

		rdbPub := server.NewClient()
		defer rdbPub.Close()
		rdbSub := server.NewClient()
		defer rdbSub.Close()
		oldDisconnections, err := getClientOutputBufferLimitDisconnections(rdbPub, ctx)
		require.NoError(t, err)

		rdbSub.Subscribe(ctx, "test_client_obuf_limit")
		elem := strings.Repeat("a", 1024*4)
		require.NoError(t, rdbPub.Publish(ctx, "test_client_obuf_limit", elem).Err())
		time.Sleep(10 * time.Second)

		newDisconnections, err := getClientOutputBufferLimitDisconnections(rdbPub, ctx)
		require.NoError(t, err)
		require.Equal(t, oldDisconnections+1, newDisconnections)
	})
}

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
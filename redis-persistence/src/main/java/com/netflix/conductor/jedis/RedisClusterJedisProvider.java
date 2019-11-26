/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.jedis;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.commands.JedisCommands;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Collections;

public class RedisClusterJedisProvider implements Provider<JedisCommands> {

    private final HostSupplier hostSupplier;

    @Inject
    public RedisClusterJedisProvider(HostSupplier hostSupplier){
        this.hostSupplier = hostSupplier;
    }

    @Override
    public JedisCommands get() {
        Host host = hostSupplier.getHosts().get(0);
        return new ShardedJedis(Collections.singletonList(new JedisShardInfo(host.getHostAddress())));
    }
}

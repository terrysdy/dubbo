/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.dubbo.rpc.cluster.loadbalance;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.AtomicPositiveInteger;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Round robin load balance.
 * 加权轮询
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "roundrobin";

    // Map<provider.method,调用编号>
    private final ConcurrentMap<String, AtomicPositiveInteger> sequences =
            new ConcurrentHashMap<String, AtomicPositiveInteger>();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // provider.method (com.xxx.sayHello)
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        int length = invokers.size(); // Number of invokers
        // 最大权重
        int maxWeight = 0;
        // 最小权重
        int minWeight = Integer.MAX_VALUE;
        // Map<invoker,权重>
        final LinkedHashMap<Invoker<T>, IntegerWrapper> invokerToWeightMap =
                new LinkedHashMap<Invoker<T>, IntegerWrapper>();
        // 总权重
        int weightSum = 0;

        // 循环获取最大、最小权重，计算权重和
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i), invocation);
            maxWeight = Math.max(maxWeight, weight); // Choose the maximum weight
            minWeight = Math.min(minWeight, weight); // Choose the minimum weight
            if (weight > 0) {
                invokerToWeightMap.put(invokers.get(i), new IntegerWrapper(weight));
                weightSum += weight;
            }
        }

        // AtomicPositiveInteger 为服务调用编号
        AtomicPositiveInteger sequence = sequences.get(key);
        if (sequence == null) {
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }
        // 当前调用编号
        int currentSequence = sequence.getAndIncrement();

        // 负载读为 O(mod),如果权重很大，会有效率问题
        if (maxWeight > 0 && minWeight < maxWeight) {
            int mod = currentSequence % weightSum;
            // mod 最多循环 maxWeight 次
            for (int i = 0; i < maxWeight; i++) {
                for (Map.Entry<Invoker<T>, IntegerWrapper> each : invokerToWeightMap.entrySet()) {
                    final Invoker<T> k = each.getKey();
                    final IntegerWrapper v = each.getValue();
                    // mod ==0 且权重大于0，响应对应 Invoker
                    if (mod == 0 && v.getValue() > 0) {
                        return k;
                    }
                    // mod != 0 且权重大于0，权重、mod 自减
                    if (v.getValue() > 0) {
                        v.decrement();
                        mod--;
                    }
                }
            }
        }

        // 权重相等直接取模
        return invokers.get(currentSequence % length);
    }

    private static final class IntegerWrapper {

        private int value;

        public IntegerWrapper(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void decrement() {
            this.value--;
        }
    }
}

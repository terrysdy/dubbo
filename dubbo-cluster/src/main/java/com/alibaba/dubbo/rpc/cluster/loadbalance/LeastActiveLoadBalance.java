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

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.Random;

/**
 * LeastActiveLoadBalance
 * 最小活跃数，provider 当前正在处理的请求数
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    private final Random random = new Random();

    /**
     * 遍历 invokers 列表，寻找活跃数最小的 Invoker
     * 如果有多个 Invoker 具有相同的最小活跃数，此时记录下这些 Invoker 在 invokers 集合中的下标，并累加它们的权重，比较它们的权重值是否相等
     * 如果只有一个 Invoker 具有最小的活跃数，此时直接返回该 Invoker 即可
     * 如果有多个 Invoker 具有最小活跃数，且它们的权重不相等，此时处理方式和 RandomLoadBalance 一致
     * 如果有多个 Invoker 具有最小活跃数，但它们的权重相等，此时随机返回一个即可
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int length = invokers.size(); // Number of invokers
        int leastActive = -1; // The least active value of all invokers
        int leastCount = 0; // The number of invokers having the same least active value
        // (leastActive)
        int[] leastIndexs = new int[length]; // The index of invokers having the same least
        // active value (leastActive)
        int totalWeight = 0; // The sum of weights
        int firstWeight = 0; // Initial value, used for comparision
        boolean sameWeight = true; // Every invoker has the same weight value?

        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName())
                    .getActive(); // Active number
            // 权重未经过预热降权，下一个版本已修复
            // int weight = getWeight(invoker,invocation);
            int weight = invoker.getUrl()
                    .getMethodParameter(invocation.getMethodName(), Constants.WEIGHT_KEY,
                            Constants.DEFAULT_WEIGHT); // Weight
            if (leastActive == -1
                    || active
                    < leastActive) { // Restart, when find a invoker having smaller least active
                // value.
                leastActive = active; // Record the current least active value
                leastCount = 1; // Reset leastCount, count again based on current leastCount
                leastIndexs[0] = i; // Reset
                totalWeight = weight; // Reset
                firstWeight = weight; // Record the weight the first invoker
                sameWeight = true; // Reset, every invoker has the same weight value?
            } else if (active
                    == leastActive) { // If current invoker's active value equals with
                // leaseActive, then accumulating.
                leastIndexs[leastCount++] = i; // Record index number of this invoker
                totalWeight += weight; // Add this invoker's weight to totalWeight.
                // If every invoker has the same weight?
                if (sameWeight && i > 0
                        && weight != firstWeight) {
                    sameWeight = false;
                }
            }
        }

        // assert(leastCount > 0)
        if (leastCount == 1) {
            // If we got exactly one invoker having the least active value, return this invoker
            // directly.
            return invokers.get(leastIndexs[0]);
        }
        if (!sameWeight && totalWeight > 0) {
            // If (not every invoker has the same weight & at least one invoker's weight>0),
            // select randomly based on totalWeight.
            int offsetWeight = random.nextInt(totalWeight);
            // Return a invoker based on the random value.
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexs[i];
                offsetWeight -= getWeight(invokers.get(leastIndex), invocation);
                if (offsetWeight <= 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(leastIndexs[random.nextInt(leastCount)]);
    }
}

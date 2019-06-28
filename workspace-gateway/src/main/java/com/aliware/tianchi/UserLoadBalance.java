package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class UserLoadBalance implements LoadBalance {
    private static final AtomicBoolean inited = new AtomicBoolean(false);

    static final ConcurrentMap<URL, HiveInvokerInfo> infoMap = new ConcurrentHashMap<>();
    private static final Semaphore semaphore = new Semaphore(100);

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {

        init(invokers);

        for (Invoker<T> invoker : invokers) {
            HiveInvokerInfo hiveInvokerInfo = infoMap.get(invoker.getUrl());
            if (hiveInvokerInfo.exhausted) {
                try {
                    semaphore.acquire(100);
                    hiveInvokerInfo.exhausted = false;
                    if (hiveInvokerInfo.weight - 3 > 0) {
                        hiveInvokerInfo.weight -= 3;
                        distributeWeight(3, invokers.stream().filter(x -> x != invoker).collect(Collectors.toList()), true);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    semaphore.release(100);
                }
            }
        }


        try {
            semaphore.acquire();
            int[] weightArray = new int[invokers.size()];
            long min = Long.MAX_VALUE;
            int minIndex = 0;
            for (int i = 0; i < invokers.size(); i++) {
                HiveInvokerInfo hiveInvokerInfo = infoMap.get(invokers.get(i).getUrl());
                weightArray[i] = hiveInvokerInfo.weight;

                long rtt = hiveInvokerInfo.rtt.get();
                if (rtt < min) {
                    min = rtt;
                    minIndex = i;
                }
            }
            int change = 35;
            weightArray[minIndex] += change;

            while (change > 0) {
                for (int i = 0; i < invokers.size(); i++) {
                    if (change > 0) {
                        if (i != minIndex) {
                            weightArray[i] -= 1;
                            change -= 1;
                        }
                    }
                }
            }
            int[] section = new int[invokers.size()];
            int totalWeight = 0;
            for (int i = 0; i < invokers.size(); i++) {

                int weight = weightArray[i];
                totalWeight += weight;
                section[i] = totalWeight;
            }

            int random = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < section.length; i++) {
                if (random < section[i]) {
                    return invokers.get(i);
                }
            }
        } catch (
                Exception e) {
            e.printStackTrace();
        } finally {
            semaphore.release();
        }

        throw new

                Error("should not happen");

    }

    private <T> void init(List<Invoker<T>> invokers) {
        if (!inited.get()) {
            if (inited.compareAndSet(false, true)) {
                for (Invoker<T> invoker : invokers) {
                    infoMap.put(invoker.getUrl(), new HiveInvokerInfo());
                }
            }
        }
    }

    private void distributeWeight(int weight, List<Invoker> targets, boolean add) {
        while (weight > 0) {
            for (Invoker invoker : targets) {
                if (weight > 0) {
                    HiveInvokerInfo hiveInvokerInfo = infoMap.get(invoker.getUrl());
                    if (add) {
                        hiveInvokerInfo.weight += 1;
                    } else {
                        hiveInvokerInfo.weight -= 1;
                    }
                    weight -= 1;
                }
            }
        }
    }
}
package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class UserLoadBalance implements LoadBalance {
    private static final AtomicBoolean inited = new AtomicBoolean(false);

    public static final ConcurrentMap<URL, HiveInvokerInfo> infoMap = new ConcurrentHashMap<>();
    Semaphore semaphore = new Semaphore(100);

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        if (!inited.get()) {
            if (inited.compareAndSet(false, true)) {
                for (Invoker<T> invoker : invokers) {
                    infoMap.put(invoker.getUrl(), new HiveInvokerInfo());
                }
            }
        }
        System.out.println("--------------");

        for (Invoker<T> invoker : invokers) {
            HiveInvokerInfo hiveInvokerInfo = infoMap.get(invoker.getUrl());
            System.out.println(hiveInvokerInfo.weight.get());

            if (hiveInvokerInfo.exhausted) {
                try {
                    semaphore.acquire(100);
                    hiveInvokerInfo.exhausted = false;
                    hiveInvokerInfo.weight.updateAndGet(x -> {
                        if (x - 5 > 0) {
                            return x - 5;
                        } else {
                            return x;
                        }
                    });
                    distributeWeight(5, invokers.stream().filter(x -> x != invoker).collect(Collectors.toList()), true);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    semaphore.release(100);
                }

            }
        }

        try {
            semaphore.acquire();
            int[] section = new int[invokers.size()];
            int totalWeight = 0;
            for (int i = 0; i < invokers.size(); i++) {
                int weight = infoMap.get(invokers.get(i).getUrl()).weight.get();
                totalWeight += weight;
                section[i] = totalWeight;
            }
            int random = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < section.length; i++) {
                if (random < section[i]) {
                    return invokers.get(i);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            semaphore.release();
        }

        throw new Error("should not happen");
    }

    private void distributeWeight(int weight, List<Invoker> targets, boolean add) {
        while (weight > 0) {
            for (Invoker invoker : targets) {
                if (weight > 0) {
                    HiveInvokerInfo hiveInvokerInfo = infoMap.get(invoker.getUrl());
                    if (add) {
                        hiveInvokerInfo.weight.incrementAndGet();
                    } else {
                        hiveInvokerInfo.weight.decrementAndGet();
                    }
                    weight -= 1;
                }
            }
        }
    }
}
package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class UserLoadBalance implements LoadBalance {
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static final AtomicBoolean inited = new AtomicBoolean(false);
    static final ConcurrentMap<URL, HiveInvokerInfo> infoMap = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(UserLoadBalance.class);

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        init(invokers);
        List<HiveInvokerInfo> sortedInfo = infoMap.values().stream()
                .sorted(Comparator.comparingLong(x -> x.averageRtt))
                .collect(Collectors.toList());

        int[] weightArray = new int[sortedInfo.size()];
        int subWeight = sortedInfo.size();
//        System.out.println();

        for (int i = 0; i < sortedInfo.size(); i++) {
            long max = sortedInfo.get(i).maxRequest;
            if (max == -1) {
                return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
            } else {
                weightArray[i] = subWeight - i;
//                System.out.println(weightArray[i]);
            }
        }

        int[] section = new int[sortedInfo.size()];
        int totalWeight = 0;
        for (int i = 0; i < sortedInfo.size(); i++) {
            int weight = weightArray[i];
            totalWeight += weight;
            section[i] = totalWeight;
        }

        HiveInvokerInfo targetInfo = null;
        int random = ThreadLocalRandom.current().nextInt(totalWeight);
        for (int i = 0; i < section.length; i++) {
            if (random < section[i]) {
                targetInfo = sortedInfo.get(i);
//                return sortedInfo.get(i).invoker;
            }
        }
        long averaverageRttCache = averageRttCache(targetInfo);
        if (targetInfo.currentRequest.get() < (long) (targetInfo.maxRequest)) {
            if (targetInfo.averageRttCache != -1) {
                if (averaverageRttCache < targetInfo.averageRttCache * 1.3) {
                    return targetInfo.invoker;
                }
            }
        } else {
            for (int i = 0; i < invokers.size(); i++) {
                HiveInvokerInfo hiveInvokerInfo = sortedInfo.get(i);
                if (hiveInvokerInfo.currentRequest.get() < (long) (hiveInvokerInfo.maxRequest)) {
                    return hiveInvokerInfo.invoker;
                }
            }
        }
        targetInfo.averageRttCache=averaverageRttCache;
        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }

    private <T> void init(List<Invoker<T>> invokers) {
        if (!inited.get()) {
            if (inited.compareAndSet(false, true)) {
                for (Invoker<T> invoker : invokers) {
                    infoMap.put(invoker.getUrl(), new HiveInvokerInfo(invoker));
                }
                HiveTask hiveTask = new HiveTask();
                executorService.execute(hiveTask);
            }
        }
    }

    private long averageRttCache(HiveInvokerInfo hiveInvokerInfo) {
        long total = 0;
        for (int i = 0; i < hiveInvokerInfo.rttCache.length; i++) {
            total += hiveInvokerInfo.rttCache[i];
        }
        return total / hiveInvokerInfo.rttCache.length;
    }
}
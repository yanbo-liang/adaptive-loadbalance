package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class UserLoadBalance implements LoadBalance {
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static final AtomicBoolean inited = new AtomicBoolean(false);
    static final ConcurrentMap<URL, HiveInvokerInfo> infoMap = new ConcurrentHashMap<>();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        init(invokers);
        List<HiveInvokerInfo> sortedInfo = infoMap.values().stream()
                .sorted(Comparator.comparingLong(x -> x.averageRtt))
                .collect(Collectors.toList());

        int[] weightArray = new int[sortedInfo.size()];
        int subWeight = sortedInfo.size();
        for (int i = 0; i < sortedInfo.size(); i++) {
            long max = sortedInfo.get(i).maxRequest;
            if (max == -1) {
                return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
            } else {
                weightArray[i] = (int) max * (subWeight - i);
//                weightArray[i] = subWeight - i;

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
        if (targetInfo.currentRequest.get() < (long) (targetInfo.maxRequest * 0.95)) {
            return targetInfo.invoker;
        } else {
            for (int i = 0; i < invokers.size(); i++) {
                HiveInvokerInfo hiveInvokerInfo = sortedInfo.get(i);
                if (hiveInvokerInfo.currentRequest.get() < (long) (hiveInvokerInfo.maxRequest * 0.95)) {
                    return hiveInvokerInfo.invoker;
                }
            }
            return invokers.get(0);
        }
//        return invokers.get(0);

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
}
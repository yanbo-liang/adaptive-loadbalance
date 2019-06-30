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
//        for (HiveInvokerInfo info : HiveTask.sortedInfo) {
//            if (info.currentRequest.get() < info.maxRequest) {
//                long l = averageRttCache(info);
//                if (l <= info.averageRttCache ) {
//                        info.averageRttCache = l;
//                        return info.invoker;
//                    } else {
////                        System.out.println(l + "  " + info.averageRttCache * 1.1);
//
//                    }
//                info.averageRttCache = l;
//
//            }
//        }
//
        List<HiveInvokerInfo> sortedInfo = HiveTask.sortedInfo;
        if (sortedInfo.size() == 0) {
            return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        }


        int[] weightArray = new int[sortedInfo.size()];
        int subWeight = sortedInfo.size();
        for (int i = 0; i < sortedInfo.size(); i++) {
            if (sortedInfo.get(i).maxRequest==-1){
                return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
            }
            weightArray[i] = (int)sortedInfo.get(i).maxRequest * (subWeight - i);
//            weightArray[i] = subWeight - i;
        }

        int[] section = new int[sortedInfo.size()];
        int totalWeight = 0;
        for (int i = 0; i < sortedInfo.size(); i++) {
            totalWeight += weightArray[i];
            section[i] = totalWeight;
        }

        HiveInvokerInfo targetInfo = null;
        int random = ThreadLocalRandom.current().nextInt(totalWeight);
        for (int i = 0; i < section.length; i++) {
            if (random < section[i]) {
                targetInfo = sortedInfo.get(i);
                break;
            }
        }

        if (targetInfo.currentRequest.get() < (long) (targetInfo.maxRequest)) {
            long l = averageRttCache(targetInfo);

            if (targetInfo.averageRttCache != -1) {
                if (l < targetInfo.averageRttCache) {
                    targetInfo.averageRttCache = l;
                    return targetInfo.invoker;
                }
            }
            targetInfo.averageRttCache = l;

        }
        for (int i = 0; i < invokers.size(); i++) {
            HiveInvokerInfo hiveInvokerInfo = sortedInfo.get(i);
            if (hiveInvokerInfo == targetInfo) {
                continue;
            }
            if (hiveInvokerInfo.currentRequest.get() < (long) (hiveInvokerInfo.maxRequest)) {
                return hiveInvokerInfo.invoker;
            }
        }


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
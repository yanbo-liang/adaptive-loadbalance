package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class UserLoadBalance implements LoadBalance {
    private static final AtomicBoolean inited = new AtomicBoolean(false);
    static final ConcurrentMap<URL, HiveInvokerInfo> infoMap = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(UserLoadBalance.class);

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        Invoker<T> randomInvoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));

        init(invokers);

        Collection<HiveInvokerInfo> infos = infoMap.values();
        for (HiveInvokerInfo info : infos) {
            if (info.maxRequest == 0) {
                return randomInvoker;
            }
        }

        List<HiveInvokerInfo> sortedInfo = infos.stream().sorted(Comparator.comparingLong(x ->
                (long) Arrays.stream(x.rttCache).average().orElse(0)))
                .collect(Collectors.toList());

        int[] weightArray = new int[sortedInfo.size()];
        int subWeight = sortedInfo.size();
        for (int i = 0; i < sortedInfo.size(); i++) {
            weightArray[i] = (int) sortedInfo.get(i).maxRequest / 15 * (15 + subWeight - i - 1);
//            weightArray[i] = sortedInfo.size()-i;
        }

        HiveInvokerInfo pickedInvoker = sortedInfo.get(pickByWeight(weightArray));
//        HiveInvokerInfo pickedInvoker = sortedInfo.get(0);

        if (pickedInvoker.currentRequest.get() < pickedInvoker.maxRequest) {
            return pickedInvoker.invoker;
        }
        for (int i = 0; i < invokers.size(); i++) {
            HiveInvokerInfo hiveInvokerInfo = sortedInfo.get(i);
            if (hiveInvokerInfo.invoker.getUrl().equals(pickedInvoker.invoker.getUrl())) {
                continue;
            }
            if (hiveInvokerInfo.currentRequest.get() < hiveInvokerInfo.maxRequest) {
                return hiveInvokerInfo.invoker;
            }
        }
        return randomInvoker;

//
//        List<HiveInvokerInfo> sortedInfo = HiveTask.sortedInfo;
//        if (sortedInfo.size() == 0) {
//            return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//        }
//
//
//        int[] weightArray = new int[sortedInfo.size()];
//        int subWeight = sortedInfo.size();
//        for (int i = 0; i < sortedInfo.size(); i++) {
//            if (sortedInfo.get(i).maxRequest == -1) {
//                return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//            }
//
//            weightArray[i] = (int) sortedInfo.get(i).maxRequest * (subWeight - i);
//
////            weightArray[i] = (subWeight - i);
//            HiveInvokerInfo targetInfo = sortedInfo.get(i);
//            long l = averageRttCache(targetInfo);
//            if (targetInfo.averageRttCache != -1) {
//                if (l < targetInfo.averageRttCache * 1.1) {
//                    weightArray[i] /=3;
//                }
//            }
//            targetInfo.averageRttCache = l;
//
//        }
//
//        int index = pickByWeight(weightArray);
//        HiveInvokerInfo a = sortedInfo.get(index);
//
//
//        if (a.currentRequest.get() < (long) (a.maxRequest)) {
//            return a.invoker;
//        }
//        for (int i = 0; i < invokers.size(); i++) {
//            HiveInvokerInfo hiveInvokerInfo = sortedInfo.get(i);
//            if (hiveInvokerInfo == a) {
//                continue;
//            }
//            if (hiveInvokerInfo.currentRequest.get() < (long) (hiveInvokerInfo.maxRequest)) {
//
//                return hiveInvokerInfo.invoker;
//
//
//            }
//        }
//

    }

    private <T> void init(List<Invoker<T>> invokers) {
        if (!inited.get()) {
            if (inited.compareAndSet(false, true)) {
                for (Invoker<T> invoker : invokers) {
                    infoMap.put(invoker.getUrl(), new HiveInvokerInfo(invoker));
                }
            }
        }
    }


//    private long averageRttCache(HiveInvokerInfo hiveInvokerInfo) {
//        long total = 0;
//        for (int i = 0; i < hiveInvokerInfo.rttCache.length; i++) {
//            total += hiveInvokerInfo.rttCache[i];
//        }
//        return total / hiveInvokerInfo.rttCache.length;
//    }

    private int pickByWeight(int[] weightArray) {
        int[] section = new int[weightArray.length];
        int totalWeight = 0;
        for (int i = 0; i < weightArray.length; i++) {
            totalWeight += weightArray[i];
            section[i] = totalWeight;
        }

        int random = ThreadLocalRandom.current().nextInt(totalWeight);
        for (int i = 0; i < section.length; i++) {
            if (random < section[i]) {
                return i;
            }
        }
        return 0;
    }
}
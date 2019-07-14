package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UserLoadBalance implements LoadBalance {
    private static final AtomicBoolean inited = new AtomicBoolean(false);
    private static final Logger logger = LoggerFactory.getLogger(UserLoadBalance.class);

    static volatile HiveInvokerInfo stressInvokerInfo = null;
    static volatile boolean stress = false;
    static ReadWriteLock selectLock = new ReentrantReadWriteLock();


    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        init(invokers);
        Invoker<T> randomInvoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        List<HiveInvokerInfo> infoList = HiveCommon.infoList;
        if (infoList == null) {
            return randomInvoker;
        }
//        selectLock.writeLock().lock();
        double maxCurrentWeight = -10D;
        HiveInvokerInfo maxInfo = null;
        for (HiveInvokerInfo info : infoList) {
            if (maxCurrentWeight < info.currentWeight) {
                maxCurrentWeight = info.currentWeight;
                maxInfo = info;
            }
        }
        maxInfo.currentWeight = maxInfo.currentWeight - 1;
        for (HiveInvokerInfo info : infoList) {
            info.currentWeight = info.weight + info.currentWeight;
        }
//        selectLock.writeLock().unlock();

        return maxInfo.invoker;

//        double[] weightArray = new double[infoList.size()];
//        for (int i = 0; i < weightArray.length; i++) {
//            weightArray[i] = infoList.get(i).weight;
//        }
//
//        HiveInvokerInfo pickedInfo = infoList.get(pickByWeight(weightArray));
//
//        return pickedInfo.invoker;
    }



//        if (pickedInfo.pendingRequest.get() < pickedInfo.maxPendingRequest) {
//            return pickedInfo.invoker;
//        }
//        for (int i = 0; i < invokers.size(); i++) {
//            HiveInvokerInfo hiveInvokerInfo = infoList.get(i);
//            if (hiveInvokerInfo == pickedInfo) {
//                continue;
//            }
//            if (hiveInvokerInfo.pendingRequest.get() < hiveInvokerInfo.maxPendingRequest) {
//
//                return hiveInvokerInfo.invoker;
//
//
//            }
//        }


//        return randomInvoker;

//        Collection<HiveInvokerInfo> infos = infoMap.values();
//        for (HiveInvokerInfo info : infos) {
//            if (info.maxPendingRequest == 0) {
//                return randomInvoker;
//            }
//        }
//
//        if (stress && stressInvokerInfo != null) {
//            if (stressInvokerInfo.currentRequest.get() < stressInvokerInfo.maxPendingRequest * stressInvokerInfo.stressCoefficient) {
//                return stressInvokerInfo.invoker;
//            }
//            else {
//                System.out.println("aa");
//                for (int i = 0; i < invokers.size(); i++) {
//                    HiveInvokerInfo info = infoMap.get(invokers.get(i).getUrl());
//                    if (stressInvokerInfo.invoker.getUrl().equals(info.invoker.getUrl())) {
//                        continue;
//                    }
//                    if (info.currentRequest.get() < info.maxPendingRequest) {
//                        return info.invoker;
//                    }
//                }
//            }
//        }


//        List<HiveInvokerInfo> sortedInfo = infos.stream().sorted(Comparator.comparingLong(x -> {
////            x.lock.readLock().lock();
//            long tmp = (long) Arrays.stream(x.rttCache).average().orElse(0);
////            x.lock.readLock().unlock();
//            return tmp;
//        })).collect(Collectors.toList());


//        int[] weightArray = new int[sortedInfo.size()];
//        int subWeight = sortedInfo.size();
//        for (
//                int i = 0; i < sortedInfo.size(); i++) {
////            weightArray[i] = (int) sortedInfo.get(i).maxPendingRequest / 10 * (10 + subWeight - i - 1);
//            weightArray[i] = (int) sortedInfo.get(i).maxPendingRequest * (subWeight - i);
//
////            weightArray[i] = sortedInfo.size()-i;
//        }
//
//        HiveInvokerInfo pickedInvoker = sortedInfo.get(pickByWeight(weightArray));

//        for (HiveInvokerInfo pickedInvoker : sortedInfo) {
//        if (pickedInvoker.currentRequest.get() < pickedInvoker.maxPendingRequest * pickedInvoker.stressCoefficient) {
//            return pickedInvoker.invoker;
//        }
//        for (
//                int i = 0; i < invokers.size(); i++) {
//            HiveInvokerInfo hiveInvokerInfo = sortedInfo.get(i);
//            if (hiveInvokerInfo.invoker.getUrl().equals(pickedInvoker.invoker.getUrl())) {
//                continue;
//            }
//            if (hiveInvokerInfo.currentRequest.get() < hiveInvokerInfo.maxPendingRequest * hiveInvokerInfo.stressCoefficient) {
//                return hiveInvokerInfo.invoker;
//            }
//        }
//        }
//        return randomInvoker;

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
//            if (sortedInfo.get(i).maxPendingRequest == -1) {
//                return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//            }
//
//            weightArray[i] = (int) sortedInfo.get(i).maxPendingRequest * (subWeight - i);
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
//        if (a.currentRequest.get() < (long) (a.maxPendingRequest)) {
//            return a.invoker;
//        }
//        for (int i = 0; i < invokers.size(); i++) {
//            HiveInvokerInfo hiveInvokerInfo = sortedInfo.get(i);
//            if (hiveInvokerInfo == a) {
//                continue;
//            }
//            if (hiveInvokerInfo.currentRequest.get() < (long) (hiveInvokerInfo.maxPendingRequest)) {
//
//                return hiveInvokerInfo.invoker;
//
//
//            }
//        }
//


    private <T> void init(List<Invoker<T>> invokers) {
        if (!inited.get()) {
            if (inited.compareAndSet(false, true)) {
                for (Invoker<T> invoker : invokers) {
                    HiveCommon.infoMap.put(invoker.getUrl(), new HiveInvokerInfo(invoker));
                }
                HiveTask task = new HiveTask();
                HiveCommon.executorService.execute(task);
            }
        }
    }

    private int pickByWeight(double[] weightArray) {
        double[] section = new double[weightArray.length];
        double totalWeight = 0;
        for (int i = 0; i < weightArray.length; i++) {
            totalWeight += weightArray[i];
            section[i] = totalWeight;
        }

        double random = ThreadLocalRandom.current().nextDouble(totalWeight);

        for (int i = 0; i < section.length; i++) {
            if (random < section[i]) {
//                System.out.println(random);
//                System.out.println(Arrays.toString(weightArray));
//                System.out.println(Arrays.toString(section));
//                System.out.println(i);
                return i;
            }
        }
        return 0;
    }

//    private long averageRttCache(HiveInvokerInfo hiveInvokerInfo) {
//        long total = 0;
//        for (int i = 0; i < hiveInvokerInfo.rttCache.length; i++) {
//            total += hiveInvokerInfo.rttCache[i];
//        }
//        return total / hiveInvokerInfo.rttCache.length;
//    }

//    private int pickByWeight(int[] weightArray) {
//        int[] section = new int[weightArray.length];
//        int totalWeight = 0;
//        for (int i = 0; i < weightArray.length; i++) {
//            totalWeight += weightArray[i];
//            section[i] = totalWeight;
//        }
//
//        int random = ThreadLocalRandom.current().nextInt(totalWeight);
//        for (int i = 0; i < section.length; i++) {
//            if (random < section[i]) {
//                return i;
//            }
//        }
//        return 0;
//    }
}
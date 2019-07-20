package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UserLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(UserLoadBalance.class);

    static volatile boolean stress = false;
    static ReadWriteLock selectLock = new ReentrantReadWriteLock();

    static ThreadLocal<HiveSelectInfo> localInfo = new ThreadLocal<>();

    volatile static boolean send = false;

    volatile static HiveInvokerInfo stressInfo;

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        HiveCommon.initLoadBalance(invokers);
        if (HiveCommon.inited) {
            if (send) {
                for (HiveInvokerInfo info : HiveCommon.infoList) {
                    if (info.pendingRequest.get() <= info.maxConcurrency) {
                        return info.invoker;
                    }
                }
            }
            if (stressInfo != null) {
                if (stressInfo.pendingRequest.get() < stressInfo.maxPendingRequest*0.95) {
                    return stressInfo.invoker;
                } else {
                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        if (info != stressInfo) {
                            if (info.pendingRequest.get() < info.maxPendingRequest*0.95) {
                                return info.invoker;
                            }
                        }
                    }
                }
            }
//
//            HiveSelectInfo sinfo = localInfo.get();
//            if (sinfo == null) {
//                HiveSelectInfo hiveSelectInfo = new HiveSelectInfo();
//                hiveSelectInfo.weights = HiveCommon.infoList.stream().mapToDouble(x -> x.weight).toArray();
//                hiveSelectInfo.cWeights = Arrays.copyOf(hiveSelectInfo.weights, hiveSelectInfo.weights.length);
//                localInfo.set(hiveSelectInfo);
//                sinfo = hiveSelectInfo;
//            } else {
//                double[] newWeights = HiveCommon.infoList.stream().mapToDouble(x -> x.weight).toArray();
//                if (!Arrays.equals(sinfo.weights, newWeights)) {
//                    sinfo.weights = newWeights;
//                    sinfo.cWeights = Arrays.copyOf(sinfo.weights, sinfo.weights.length);
//                }
//            }
//            double maxCurrentWeight = -1000D;
//            int index = 0;
//            for (int i = 0; i < sinfo.cWeights.length; i++) {
//                if (maxCurrentWeight < sinfo.cWeights[i]) {
//                    maxCurrentWeight = sinfo.cWeights[i];
//                    index = i;
//                }
//            }
//            sinfo.cWeights[index] -= 1;
//            for (int i = 0; i < sinfo.cWeights.length; i++) {
//                sinfo.cWeights[i] = sinfo.cWeights[i] + sinfo.weights[i];
//            }
//            return HiveCommon.infoList.get(index).invoker;

//            List<HiveInvokerInfo> infoList = HiveCommon.infoList;
//            double[] weights = new double[infoList.size()];
//            for (int i = 0; i < weights.length; i++) {
//                weights[i] = infoList.get(i).weight;
//            }
//
//            HiveInvokerInfo pickedInfo = infoList.get(HiveCommon.pickByWeight(weights));
//
//            return pickedInfo.invoker;

        }

        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
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


//        int[] weights = new int[sortedInfo.size()];
//        int subWeight = sortedInfo.size();
//        for (
//                int i = 0; i < sortedInfo.size(); i++) {
////            weights[i] = (int) sortedInfo.get(i).maxPendingRequest / 10 * (10 + subWeight - i - 1);
//            weights[i] = (int) sortedInfo.get(i).maxPendingRequest * (subWeight - i);
//
////            weights[i] = sortedInfo.size()-i;
//        }
//
//        HiveInvokerInfo pickedInvoker = sortedInfo.get(pickByWeight(weights));

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
//        int[] weights = new int[sortedInfo.size()];
//        int subWeight = sortedInfo.size();
//        for (int i = 0; i < sortedInfo.size(); i++) {
//            if (sortedInfo.get(i).maxPendingRequest == -1) {
//                return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//            }
//
//            weights[i] = (int) sortedInfo.get(i).maxPendingRequest * (subWeight - i);
//
////            weights[i] = (subWeight - i);
//            HiveInvokerInfo targetInfo = sortedInfo.get(i);
//            long l = averageRttCache(targetInfo);
//            if (targetInfo.averageRttCache != -1) {
//                if (l < targetInfo.averageRttCache * 1.1) {
//                    weights[i] /=3;
//                }
//            }
//            targetInfo.averageRttCache = l;
//
//        }
//
//        int index = pickByWeight(weights);
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
}
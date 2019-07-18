package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class HiveCommon {
    static final Logger logger = LoggerFactory.getLogger(HiveCommon.class);

    static final ConcurrentMap<URL, HiveInvokerInfo> infoMap = new ConcurrentHashMap<>();
    static volatile List<HiveInvokerInfo> infoList;

    static final AtomicInteger pendingRequestTotal = new AtomicInteger(0);

    static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    static final SimpleDateFormat format = new SimpleDateFormat("mm:ss:SSS");

    static volatile boolean inited = false;
    private static final AtomicBoolean initedByCallback = new AtomicBoolean(false);
    private static final AtomicBoolean initedByLoadBalance = new AtomicBoolean(false);

    static <T> void initLoadBalance(List<Invoker<T>> invokers) {
        if (!initedByLoadBalance.get()) {
            if (initedByLoadBalance.compareAndSet(false, true)) {
                for (Invoker<T> invoker : invokers) {
                    infoMap.put(invoker.getUrl(), new HiveInvokerInfo(invoker));
                }
                HiveTask task = new HiveTask();
                executorService.execute(task);
            }
        }
    }

    static void initCallBack() {
        if (initedByLoadBalance.get() && !initedByCallback.get()) {
            for (HiveInvokerInfo info : infoMap.values()) {
                if (info.maxPendingRequest == 0) {
                    return;
                }
            }
            if (initedByCallback.compareAndSet(false, true)) {
                int totalPendingRequest = infoMap.values().stream().mapToInt(x -> x.maxPendingRequest).sum();
                for (HiveInvokerInfo info : infoMap.values()) {
                    info.weightInitial = ((double) info.maxPendingRequest) / totalPendingRequest;
                    info.weight = info.weightInitial;
                    info.currentWeight = info.weight;
                    info.weightTop = ((double) info.maxPendingRequest) / 1024;
                }
                infoList = new ArrayList<>(HiveCommon.infoMap.values());
                inited = true;
            }
        }
    }


    static double distributeWeightUp(List<HiveInvokerInfo> infoList, double distributedWeight, double weightSum) {
        List<HiveInvokerInfo> sortedList = infoList.stream().sorted(Comparator.comparing(x -> x.rtt, Comparator.reverseOrder())).collect(Collectors.toList());
        double remain = 0;
        for (HiveInvokerInfo info : sortedList) {
            double weightChange = (info.weight / weightSum) * distributedWeight + remain;
            if (info.weight >= info.weightTop) {
                remain += weightChange;
                continue;
            }
            if (info.weight + weightChange < info.weightTop) {
                info.weight = info.weight + weightChange;
            } else {
                remain += (weightChange - (info.weightTop - info.weight));
                info.weight = info.weightTop;
            }
        }
        return remain;
    }

    static void distributeWeightDown(List<HiveInvokerInfo> infoList, double distributedWeight, double weightSum) {
        for (HiveInvokerInfo info : infoList) {
            info.weight = info.weight - distributedWeight * (info.weight / weightSum);
        }
    }

    static void weightNormalize() {
        double total = 0;
        for (HiveInvokerInfo info : infoList) {
            total += info.weight;
        }
        for (HiveInvokerInfo info : infoList) {
            info.weight = info.weight / total;
        }
    }

    static void setCurrentWeight() {
        List<HiveInvokerInfo> infoList = HiveCommon.infoList;
        for (HiveInvokerInfo info : infoList) {
            info.currentWeight = info.weight;
        }
    }

    static void clearWeight() {
        List<HiveInvokerInfo> infoList = HiveCommon.infoList;
        for (HiveInvokerInfo info : infoList) {
            info.weight = info.weightInitial;
        }
    }

    static void clearWeightAndAverage() {
        for (HiveInvokerInfo info : HiveCommon.infoList) {
            info.weight = info.weightInitial;
            info.rtt = 0;
        }
    }

    static void log(String msg) {
        System.out.println(msg);
        for (HiveInvokerInfo info : HiveCommon.infoList) {
            System.out.println(HiveCommon.format.format(new Date()) + '-' + info);
        }
        System.out.println();
    }

    static int pickByWeight(double[] weightArray) {
        double[] section = new double[weightArray.length];
        double totalWeight = 0;
        for (int i = 0; i < weightArray.length; i++) {
            totalWeight += weightArray[i];
            section[i] = totalWeight;
        }

        double random = ThreadLocalRandom.current().nextDouble(totalWeight);
        for (int i = 0; i < section.length; i++) {
            if (random < section[i]) {
                return i;
            }
        }
        return 0;
    }

//    private void mainCalculation() {
//        List<HiveInvokerInfo> good = new ArrayList<>();
//        List<HiveInvokerInfo> bad = new ArrayList<>();
//
//        for (HiveInvokerInfo info : infoList) {
//            if (info.rttAverageUpper < info.rtt * 1.15) {
//                good.add(info);
//            } else if (info.rttAverageDowner < info.rtt / 1.1) {
//                bad.add(info);
//            }
//        }
//        if (good.size() != 0 && bad.size() != 0) {
//            int goodsum = good.stream().mapToInt(x -> x.maxPendingRequest).sum();
//            int badsum = bad.stream().mapToInt(x -> x.maxPendingRequest).sum();
//            double change = 0;
//            if (goodsum < badsum) {
//                for (HiveInvokerInfo info : good) {
//                    double newweight = info.weight * 1.1;
//                    change += (newweight - info.weight);
//                    info.weight = newweight;
//                }
//                for (HiveInvokerInfo info : bad) {
//                    info.weight = info.weight - change / bad.size();
//                }
//            } else {
//                for (HiveInvokerInfo info : bad) {
//                    double newweight = info.weight / 1.1;
//                    change += info.weight - newweight;
//                    info.weight = newweight;
//                }
//                for (HiveInvokerInfo info : good) {
//                    info.weight = info.weight + change / bad.size();
//                }
//            }
//            System.out.println(good);
//            System.out.println(bad);
//
//        }
//    }
//
//    private double weightChangeSum(boolean odd, boolean up) {
//        List<HiveInvokerInfo> infoList = HiveCommon.infoList;
//        double totalChange = 0D;
//        for (int i = odd ? 0 : 1; i < infoList.size(); i += 2) {
//            HiveInvokerInfo info = infoList.get(i);
//            double newWeight = up ? info.weight * 1.15 : info.weight / 1.15;
//            totalChange += up ? newWeight - info.weight : info.weight - newWeight;
//            info.weight = newWeight;
//        }
//        return totalChange;
//    }
//
//    private void weightChangeDistribute(boolean odd, boolean up, double totalChange) {
//        List<HiveInvokerInfo> infoList = HiveCommon.infoList;
//        for (int i = odd ? 0 : 1; i < infoList.size(); i += 2) {
//            HiveInvokerInfo info = infoList.get(i);
//            double change = totalChange / (double) (infoList.size() / 2);
//            info.weight = up ? info.weight + change : info.weight - change;
//        }
//        weightNormalize();
//    }
//
//
//
//    private void weightDistributeToFastest() {
//        boolean done = false;
//        int remain = 1024;
//        for (HiveInvokerInfo info : HiveCommon.infoList) {
//            if (!done) {
//                if (remain > info.maxPendingRequest) {
//                    info.weight = info.maxPendingRequest / ((double) 1024);
//                    remain -= info.maxPendingRequest;
//                } else if (remain <= info.maxPendingRequest) {
//                    info.weight = remain / ((double) 1024);
//                    done = true;
//                }
//            } else {
//                info.weight = 0;
//            }
//        }
//    }

    static void weightCalculation() {
        int rttAverage=0;
        for (HiveInvokerInfo info : infoList) {
           rttAverage+= info.rtt*info.weight;
        }
        List<HiveInvokerInfo> goodList = new ArrayList<>();
        List<HiveInvokerInfo> badList = new ArrayList<>();
        Date date = new Date();
        for (HiveInvokerInfo info : infoList) {
            if (info.rtt > rttAverage) {
                badList.add(info);
            } else if (info.rtt < rttAverage) {
                goodList.add(info);
            }
        }

        double goodListWeight = goodList.stream().mapToDouble(x -> x.weight).sum();
        double badListWeight = badList.stream().mapToDouble(x -> x.weight).sum();
        double weightChange;
        if (goodListWeight > badListWeight) {
            weightChange = badListWeight * 0.1;
        } else {
            weightChange = goodListWeight * 0.1;
        }
        logger.info("{}-{}--{}", format.format(date), rttAverage, weightChange);

        HiveCommon.distributeWeightDown(badList, weightChange, badListWeight);
        double remianWeight = HiveCommon.distributeWeightUp(goodList, weightChange, goodListWeight);
        HiveCommon.distributeWeightUp(badList, remianWeight, badListWeight);

        weightNormalize();
        setCurrentWeight();
    }
}

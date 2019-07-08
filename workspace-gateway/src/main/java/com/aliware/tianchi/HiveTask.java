package com.aliware.tianchi;

import java.util.*;
import java.util.stream.Collectors;

public class HiveTask implements Runnable {
    static volatile List<HiveInvokerInfo> infoList;

    static boolean inited = false;


    private void init() {
        if (inited) {
            return;
        }
        Collection<HiveInvokerInfo> hiveInvokerInfos = UserLoadBalance.infoMap.values();
        for (HiveInvokerInfo info : hiveInvokerInfos) {
            if (info.maxRequest == 0) {
                return;
            }
        }
        int totalMaxRequest = hiveInvokerInfos.stream().mapToInt(x -> x.maxRequest).sum();

        for (HiveInvokerInfo info : hiveInvokerInfos) {
            info.weight = ((double) info.maxRequest) / (double) totalMaxRequest;
            info.weightBound = info.weight;
            System.out.println(info.toString());
        }

        infoList = new ArrayList<>(UserLoadBalance.infoMap.values());

        inited = true;
    }

    static int upCount = 0;
    static int downCount = 0;

    @Override
    public void run() {
        try {
            Thread.sleep(100);
            while (true) {
                init();

                if (inited) {
                    for (HiveInvokerInfo info : infoList) {
                        double rttAverageNew;
                        double rttAverageOld = info.rttAverage;
                        long rttTotalCount = info.rttTotalCount.get();
                        long rttTotalTime = info.rttTotalTime.get();
                        if (rttTotalCount != 0) {
                            rttAverageNew = (double) (rttTotalTime) / (double) (rttTotalCount);
                            if (rttAverageOld == 0D) {
                                info.rttAverage = rttAverageNew;
                            } else if (rttAverageOld * 0.9 < rttAverageNew && rttAverageNew < rttAverageOld * 1.1) {
                                info.maxRequestCoefficient += 0.05;
                                upCount=0;
                                downCount=0;
                            } else if (rttAverageNew < rttAverageOld * 0.9) {
                                upCount+=1;
                                downCount=0;
                            } else if (rttAverageNew > rttAverageOld * 1.1) {
                                upCount=0;
                                downCount+=1;
                            }
                            if (upCount==2){
                                upCount=0;
                                info.maxRequestCoefficient += 0.1;
                            }
                            if (downCount==2){
                                downCount=0;
                                info.maxRequestCoefficient -= 0.1;
                            }
                        }
                    }

                    for (HiveInvokerInfo invokerInfo:infoList){
                        System.out.println(invokerInfo);
                    }
                }


                Thread.sleep(100);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

//        try {
//            while (true) {
//                long start = System.currentTimeMillis();
//                List<HiveInvokerInfo> values = new ArrayList<>(UserLoadBalance.infoMap.values());
//                for (int i = 0; i < values.size(); i++) {
//                    HiveInvokerInfo hiveInvokerInfo = values.get(i);
//                    int totalAverage = 0;
//                    int totalCount = 0;
//                    hiveInvokerInfo.stressCoefficient = 0.6;
//                    UserLoadBalance.stressInvokerInfo = hiveInvokerInfo;
//                    for (int j = 0; j < 4; j++) {
//                        UserLoadBalance.stress = true;
//                        HiveFilter.stress = true;
//                        Thread.sleep(15);
//                        UserLoadBalance.stress = false;
//                        HiveFilter.stress = false;
//                        int rttTotalCount = hiveInvokerInfo.rttTotalCount.get();
//                        if (rttTotalCount == 0) {
//                            continue;
//                        }
//                        int average = hiveInvokerInfo.rttTotalTime.get() / hiveInvokerInfo.rttTotalCount.get();
//                        hiveInvokerInfo.rttTotalTime.updateAndGet(x -> 0);
//                        hiveInvokerInfo.rttTotalCount.updateAndGet(x -> 0);
//
//                        if (j != 0) {
//
//                            System.out.println(average + " " + totalAverage / (j));
//                            if (average > totalAverage / (j) * 1.1) {
//                                break;
//                            }
//                        }
//                        totalAverage += average;
//                        totalCount += 1;
//                        hiveInvokerInfo.averageRtt = totalAverage / totalCount;
//                        hiveInvokerInfo.stressCoefficient += 0.10;
//                    }
//                }
//                sortedInfo = values.stream().sorted(Comparator.comparingInt(x -> x.averageRtt)).collect(Collectors.toList());
//
//                long end = System.currentTimeMillis();
//                System.out.println("time" + (end - start));
//                for (HiveInvokerInfo info : values) {
//                    System.out.println(info.stressCoefficient);
//                }
//                Thread.sleep(6000);
//            }
//        } catch (
//                Exception e) {
//            e.printStackTrace();
//        }
    }
}

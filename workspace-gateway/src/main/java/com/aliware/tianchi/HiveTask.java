package com.aliware.tianchi;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.stream.Collectors;

public class HiveTask implements Runnable {

    static boolean init = false;
    static int count = 0;

    private boolean init() {
        if (init) {
            return true;
        } else {
            Collection<HiveInvokerInfo> hiveInvokerInfos = HiveCommon.infoMap.values();
            for (HiveInvokerInfo info : hiveInvokerInfos) {
                if (info.maxPendingRequest == 0) {
                    return false;
                }
            }
            int totalMaxRequest = hiveInvokerInfos.stream().mapToInt(x -> x.maxPendingRequest).sum();

            for (HiveInvokerInfo info : hiveInvokerInfos) {
                info.weight = ((double) info.maxPendingRequest) / (double) totalMaxRequest;
                info.weightBound = info.weight;
            }

            HiveCommon.infoList = new ArrayList<>(HiveCommon.infoMap.values());

            init = true;
            return true;
        }
    }

    @Override
    public void run() {
        SimpleDateFormat ft = new SimpleDateFormat("hh:mm:ss SSS");
        System.out.println("!!!!!!!start!!!!!1" + ft.format(new Date()));
        try {
            while (true) {
                if (init()) {
                    System.out.println("reset");
                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        info.weight = info.weightBound;
                        info.totalTime.updateAndGet(x -> 0);
                        info.totalRequest.updateAndGet(x -> 0);
                        info.rttAverage=0;
                        System.out.println(ft.format(new Date()) + '-' + info);
                    }

                    Thread.sleep(1000);
                    System.out.println("reset result");

                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        long totalTime = info.totalTime.get();
                        long completedRequest = info.totalRequest.get();

                        if (completedRequest != 0) {
                            info.rttAverage = ((double) totalTime) / completedRequest;
                        }
                        System.out.println(ft.format(new Date()) + '-' + info);
                        info.totalTime.updateAndGet(x -> 0);
                        info.totalRequest.updateAndGet(x -> 0);
                    }
                    HiveCommon.infoList = HiveCommon.infoList.stream()
                            .sorted(Comparator.comparingDouble(x -> x.rttAverage)).collect(Collectors.toList());

                    boolean done = false;
                    int remain = 1024;
                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        if (!done) {
                            if (remain > info.maxPendingRequest) {
                                info.weight = info.maxPendingRequest / ((double) 1024);
                                remain -= info.maxPendingRequest;
                            } else if (remain <= info.maxPendingRequest) {
                                info.weight = remain / ((double) 1024);
                                done = true;
                            }
                        } else {
                            info.weight = 0;
                        }

                    }
                    Thread.sleep(5000);
                    System.out.println("send result");
                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        long totalTime = info.totalTime.get();
                        long completedRequest = info.totalRequest.get();

                        if (completedRequest != 0) {
                            info.rttAverage = ((double) totalTime) / completedRequest;
                        }
                        System.out.println(ft.format(new Date()) + '-' + info);
                    }
                } else {
                    Thread.sleep(10);
                }
            }
        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }
}
//}
//    @Override
//    public void run() {
//        try {
//            while (true) {
//                System.out.println(HiveCommon.pendingRequestTotal.get());
////                init();
////
////                if (inited) {
////                    for (HiveInvokerInfo info : infoList) {
////                        double rttAverageNew = 0;
////                        double rttAverageOld = info.rttAverage;
////                        long totalRequest = info.totalRequest.get();
////                        long totalTime = info.totalTime.get();
////                        if (totalRequest != 0) {
////                            rttAverageNew = (double) (totalTime) / (double) (totalRequest);
////
////                            info.rttAverage = rttAverageNew;
////                            info.maxRequestCoefficient = totalRequest / ((500 / info.rttAverage) * info.maxPendingRequest);
////
////
////                        }
////                        info.totalRequest.updateAndGet(x -> 0);
////                        info.totalTime.updateAndGet(x -> 0);
////                        System.out.println(info);
////                    }
////                    boolean check = true;
////                    for (HiveInvokerInfo info : infoList) {
////                        if (info.rttAverage == 0D) {
////                            check = false;
////                            break;
////                        }
////                    }
////                    if (check) {
////                        infoList = infoList.stream().sorted(Comparator.comparingDouble(x -> x.rttAverage)).collect(Collectors.toList());
////                    }
////
////                }
//                Thread.sleep(10);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}


//    @Override
//    public void run() {
//        try {
//            Thread.sleep(100);
//            while (true) {
//                init();
//
//                if (inited) {
//                    for (HiveInvokerInfo info : infoList) {
//                        double rttAverageNew = 0;
//                        double rttAverageOld = info.rttAverage;
//                        long totalRequest = info.totalRequest.get();
//                        long totalTime = info.totalTime.get();
//                        if (totalRequest != 0) {
//                            rttAverageNew = (double) (totalTime) / (double) (totalRequest);
//                            if (rttAverageOld == 0D) {
//                                info.rttAverage = rttAverageNew;
//                            } else if (rttAverageOld * 0.95 < rttAverageNew & rttAverageNew < rttAverageOld * 1.05) {
//                                if (info.maxRequestCoefficient + 0.05 > 1) {
//                                    info.maxRequestCoefficient = 1;
//                                } else {
//                                    info.maxRequestCoefficient += 0.05;
//                                }
//                                info.rttAverage = rttAverageNew;
//
//                                info.upCount = 0;
//                                info.downCount = 0;
//                            } else if (rttAverageNew < rttAverageOld * 0.95) {
//                                info.upCount += 1;
//                                info.downCount = 0;
//
//                            } else if (rttAverageNew > rttAverageOld * 1.05) {
//                                info.upCount = 0;
//                                info.downCount += 1;
//                            }
//                            if (info.upCount == 1) {
//                                info.upCount = 0;
//                                if (info.maxRequestCoefficient + 0.08 > 1) {
//                                    info.maxRequestCoefficient = 1;
//                                } else {
//                                    info.maxRequestCoefficient += 0.08;
//                                }
//                                info.rttAverage = rttAverageNew;
////                                info.weight *= 1.1;
//                            }
//                            if (info.downCount == 1) {
//                                info.downCount = 0;
//                                if (info.maxRequestCoefficient - 0.1 < 0.5) {
//                                    info.maxRequestCoefficient = 0.5;
//                                } else {
//                                    info.maxRequestCoefficient -= 0.1;
//                                }
//                                info.rttAverage = rttAverageNew;
////                                info.weight /= 1.1;
//
//                            }
//                        }

//                        info.totalRequest.updateAndGet(x -> 0);
//                        info.totalTime.updateAndGet(x -> 0);
//
//                        info.weight = info.weightBound;
//
//                    }
//                    infoList = infoList.stream().sorted(Comparator.comparingDouble(x -> x.rttAverage)).collect(Collectors.toList());
//
//                }
//                    HiveInvokerInfo first = infoList.get(0);
//                    double expectRtt = first.rttAverage * (1 + (1 - first.maxRequestCoefficient) / first.maxRequestCoefficient);
//                    if (expectRtt < infoList.get(2).rttAverage) {
//                        first.weight *= 2;
//                    }
//                    HiveInvokerInfo second = infoList.get(1);
//                    double expectRttsecond = second.rttAverage * (1 + (1 - second.maxRequestCoefficient) / second.maxRequestCoefficient);
//                    if (expectRttsecond < infoList.get(2).rttAverage) {
//                        second.weight *= 2;
//                    }
//
//
//
//
//                    double total = 0;
//                    for (HiveInvokerInfo info : infoList) {
//                        total += info.weight;
//                    }
//                    for (HiveInvokerInfo info : infoList) {
//                        info.weight += info.weight / total;
//                    }


//                Thread.sleep(500);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

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
//                        int totalRequest = hiveInvokerInfo.totalRequest.get();
//                        if (totalRequest == 0) {
//                            continue;
//                        }
//                        int average = hiveInvokerInfo.totalTime.get() / hiveInvokerInfo.totalRequest.get();
//                        hiveInvokerInfo.totalTime.updateAndGet(x -> 0);
//                        hiveInvokerInfo.totalRequest.updateAndGet(x -> 0);
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
//            }
//        }

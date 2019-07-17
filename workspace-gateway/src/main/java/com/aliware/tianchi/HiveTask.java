package com.aliware.tianchi;

import java.util.Date;

public class HiveTask implements Runnable {

    @Override
    public void run() {
        System.out.println("!!!!!!!!!!!!!!!!!!!!task start at " + HiveCommon.format.format(new Date()));
        long start = System.currentTimeMillis();
        ;
        long lastSum = 0;
        try {
            while (true) {
                if (HiveCommon.inited && System.currentTimeMillis() > (start + (30 * 1000) + 10)) {
//                if (HiveCommon.inited) {


                    long sampleStartTime = System.currentTimeMillis();
                    long sampleEndTime = sampleStartTime + 200;
                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        info.sampleStartTime = sampleStartTime;
                        info.sampleEndTime = sampleEndTime;
                    }
                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        info.totalTime.updateAndGet(x -> 0);
                        info.totalRequest.updateAndGet(x -> 0);
                    }
                    Thread.sleep(200);
                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        info.lock.writeLock().lock();
                    }
                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        info.rttAverage = info.totalTime.get() / (double) info.totalRequest.get();
                    }
                    HiveCommon.log("test");

                    long sum = HiveCommon.infoList.stream().mapToLong(x -> x.totalRequest.get()).sum();
                    if (lastSum == 0) {
                        lastSum = sum;
                    } else if (sum < lastSum * 0.5) {
                        for (HiveInvokerInfo info : HiveCommon.infoList) {
                            info.lock.writeLock().unlock();
                        }
                        continue;
                    }
                    UserLoadBalance.selectLock.writeLock().lock();

                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        if (info.totalRequest.get() != 0) {

                            double newWeight = info.totalRequest.get() / (double) sum;

                            if (newWeight>info.weight){
                                info.weight*=1.02;
                            }else{
                                info.weight/=1.02;
                            }
                        }
                    }
                    lastSum = sum;
                    HiveCommon.weightNormalize();
                    HiveCommon.setCurrentWeight();
                    UserLoadBalance.selectLock.writeLock().unlock();

                    for (HiveInvokerInfo info : HiveCommon.infoList) {
                        info.lock.writeLock().unlock();
                    }
                } else {
                    Thread.sleep(1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


//                if (init() && System.currentTimeMillis() > start) {
//                    UserLoadBalance.selectLock.writeLock().lock();
//                    clearWeightAndAverage();
//                    clearTotal();
//                    setCurrentWeight();
//
//                    UserLoadBalance.selectLock.writeLock().unlock();
//                    Thread.sleep(300);
//                    calculateAverage();
//                    log("test");

//                        UserLoadBalance.selectLock.writeLock().lock();
//                    clearWeightAndAverage();
//                    clearTotal();
//                    setCurrentWeight();
//                    UserLoadBalance.selectLock.writeLock().unlock();
//                    Thread.sleep(300);
//                    calculateAverage();
//                    log("normal weight");
//
//                        UserLoadBalance.selectLock.writeLock().lock();
//                    clearWeight();
//                    weightChangeDistribute(false, false, weightChangeSum(true, true));
//                    clearTotal();
//                    setCurrentWeight();
//                    UserLoadBalance.selectLock.writeLock().unlock();
//                    Thread.sleep(300);
//                    calculateProbingAverage(true, true);
//                    log("odd up");
//
//                    UserLoadBalance.selectLock.writeLock().lock();
//                    clearWeight();
//                    weightChangeDistribute(true, false, weightChangeSum(false, true));
//                    clearTotal();
//                    setCurrentWeight();
//                    UserLoadBalance.selectLock.writeLock().unlock();
//                    Thread.sleep(300);
//                    calculateProbingAverage(false, true);
//                    log("even up");
//
//                    UserLoadBalance.selectLock.writeLock().lock();
//                    clearWeight();
//                    weightChangeDistribute(false, true, weightChangeSum(true, false));
//                    clearTotal();
//                    UserLoadBalance.selectLock.writeLock().unlock();
//                    Thread.sleep(300);
//                    setCurrentWeight();
//                    calculateProbingAverage(true, false);
//                    log("odd down");
//
//                    UserLoadBalance.selectLock.writeLock().lock();
//                    clearWeight();
//                    weightChangeDistribute(true, true, weightChangeSum(false, false));
//                    clearTotal();
//                    setCurrentWeight();
//                    UserLoadBalance.selectLock.writeLock().unlock();
//                    Thread.sleep(300);
//                    calculateProbingAverage(false, false);
//                    log("even down");
//
//                    UserLoadBalance.selectLock.writeLock().lock();
//                    clearWeight();
//                    mainCalculation();
//                    clearTotal();
//                    setCurrentWeight();
//                    UserLoadBalance.selectLock.writeLock().unlock();
//                    Thread.sleep(4500);
//                    calculateAverage();
//                    log("result");


//
//    private void calculateAverage() {
//        List<HiveInvokerInfo> infoList = HiveCommon.infoList;
//        for (HiveInvokerInfo info : infoList) {
//            info.lock.writeLock().lock();
//            long totalTime = info.totalTime.get();
//            long completedRequest = info.totalRequest.get();
//            info.lock.writeLock().unlock();
//            if (completedRequest != 0) {
//                info.rttAverage = ((double) totalTime) / completedRequest;
//            }
//        }
//    }
//
//    private void calculateProbingAverage(boolean odd, boolean up) {
//        List<HiveInvokerInfo> infoList = HiveCommon.infoList;
//        for (int i = odd ? 0 : 1; i < infoList.size(); i += 2) {
//            HiveInvokerInfo info = infoList.get(i);
//            info.lock.writeLock().lock();
//            long totalTime = info.totalTime.get();
//            long completedRequest = info.totalRequest.get();
//            info.lock.writeLock().unlock();
//            if (completedRequest != 0) {
//                if (up) {
//                    info.rttAverageUpper = ((double) totalTime) / completedRequest;
//                } else {
//                    info.rttAverageDowner = ((double) totalTime) / completedRequest;
//                }
//            }
//        }
//    }
//
//    private void clearTotal() {
//        List<HiveInvokerInfo> infoList = HiveCommon.infoList;
//        for (HiveInvokerInfo info : infoList) {
//            info.totalTime.updateAndGet(x -> 0);
//            info.totalRequest.updateAndGet(x -> 0);
//        }
//    }


}

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
//                        info.weight = info.weightInitial;
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

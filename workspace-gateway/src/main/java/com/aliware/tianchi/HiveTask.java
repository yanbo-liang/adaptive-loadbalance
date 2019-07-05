package com.aliware.tianchi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class HiveTask implements Runnable {
    static volatile List<HiveInvokerInfo> sortedInfo;

    @Override
    public void run() {
        try {
            while (true) {
                long start = System.currentTimeMillis();
                List<HiveInvokerInfo> values = new ArrayList<>(UserLoadBalance.infoMap.values());
                for (int i = 0; i < values.size(); i++) {
                    HiveInvokerInfo hiveInvokerInfo = values.get(i);
                    int totalAverage = 0;
                    int totalCount = 0;
                    hiveInvokerInfo.stressCoefficient = 0.6;
                    UserLoadBalance.stressInvokerInfo = hiveInvokerInfo;
                    for (int j = 0; j < 4; j++) {
                        UserLoadBalance.stress = true;
                        HiveFilter.stress = true;
                        Thread.sleep(100);
                        UserLoadBalance.stress = false;
                        HiveFilter.stress = false;
                        int rttTotalCount = hiveInvokerInfo.rttTotalCount.get();
                        if (rttTotalCount == 0) {
                            continue;
                        }
                        int average = hiveInvokerInfo.rttTotalTime.get() / hiveInvokerInfo.rttTotalCount.get();
                        hiveInvokerInfo.rttTotalTime.updateAndGet(x -> 0);
                        hiveInvokerInfo.rttTotalCount.updateAndGet(x -> 0);

                        if (j != 0) {

                            System.out.println(average + " " + totalAverage / (j));
                            if (average > totalAverage / (j) * 1.1) {
                                break;
                            }
                        }
                        totalAverage += average;
                        totalCount += 1;
                        hiveInvokerInfo.averageRtt = totalAverage / totalCount;
                        hiveInvokerInfo.stressCoefficient += 0.10;
                    }
                }
                sortedInfo = values.stream().sorted(Comparator.comparingInt(x -> x.averageRtt)).collect(Collectors.toList());

                long end = System.currentTimeMillis();
                System.out.println("time" + (end - start));
                for (HiveInvokerInfo info : values) {
                    System.out.println(info.stressCoefficient);
                }
                Thread.sleep(6000);
            }
        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }
}

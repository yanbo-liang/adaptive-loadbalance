package com.aliware.tianchi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class HiveTask implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(HiveTask.class);

    @Override
    public void run() {
        Semaphore rttSemaphore = HiveFilter.rttSemaphore;
        while (true) {
            try {
                rttSemaphore.acquire(500);
                UserLoadBalance.infoMap.forEach((k, v) -> {
                    v.rttWeight = 0;
                    v.averageRtt = Long.MAX_VALUE;
                    if (v.totalRequest.get() != 0) {
                        v.averageRtt = v.totalRtt.get() / v.totalRequest.get();
                    }
                    v.totalRtt.updateAndGet(x -> 0);
                    v.totalRequest.updateAndGet(x -> 0);
                });

                List<HiveInvokerInfo> sortedInfo = UserLoadBalance.infoMap.values().stream()
                        .sorted(Comparator.comparingLong(x -> x.averageRtt)).collect(Collectors.toList());
                if (sortedInfo.size() > 1) {
                    int change = 20;
                    sortedInfo.get(0).rttWeight += change;
                    while (change > 0) {
                        for (int i = 1; i < sortedInfo.size(); i++) {
                            if (change > 0) {
                                change -= 1;
                                sortedInfo.get(i).rttWeight -= 1;
                            }
                        }
                    }
                }
                System.out.println(sortedInfo);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                rttSemaphore.release(500);
            }

            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

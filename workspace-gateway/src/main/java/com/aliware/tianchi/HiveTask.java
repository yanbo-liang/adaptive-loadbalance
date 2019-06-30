package com.aliware.tianchi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class HiveTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(HiveTask.class);
    static List<HiveInvokerInfo> sortedInfo = Collections.EMPTY_LIST;

    @Override
    public void run() {
        Semaphore rttSemaphore = HiveFilter.rttSemaphore;
        while (true) {
            try {
                rttSemaphore.acquire(100);
                UserLoadBalance.infoMap.forEach((k, v) -> {
//                    v.averageRtt = Long.MAX_VALUE;
                    if (v.totalRequest.get() != 0) {
                        v.averageRtt = v.totalRtt.get() / v.totalRequest.get();
                    }
                    v.totalRtt.updateAndGet(x -> 0);
                    v.totalRequest.updateAndGet(x -> 0);
                });
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                rttSemaphore.release(100);
            }
//            System.out.println();
//
//            UserLoadBalance.infoMap.forEach((k, v) -> {
//                System.out.println(v);
//
//            });

            sortedInfo = UserLoadBalance.infoMap.values().stream()
                    .sorted(Comparator.comparingLong(x -> x.averageRtt))
                    .collect(Collectors.toList());
            try {
                Thread.sleep(555);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

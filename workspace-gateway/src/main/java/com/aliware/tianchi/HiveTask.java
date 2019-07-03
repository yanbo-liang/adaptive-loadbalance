package com.aliware.tianchi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
        Semaphore rttSemaphore = HiveFilter.semaphore;
        while (true) {
            try {
                rttSemaphore.acquire(100);
                sortedInfo = UserLoadBalance.infoMap.values().stream()
                        .sorted(Comparator.comparingLong(x -> x.maxRtt.get()))
                        .collect(Collectors.toList());
                UserLoadBalance.infoMap.forEach((k, v) -> {
                    v.maxRtt.updateAndGet(x -> 0);
                });
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                rttSemaphore.release(100);
            }
//
//            StringBuilder stringBuilder = new StringBuilder();
//            stringBuilder.append(a++);
//            stringBuilder.append('-');
//            for (HiveInvokerInfo info : sortedInfo) {
//                stringBuilder.append(info.invoker.getUrl().getHost());
//                stringBuilder.append(':');
//                stringBuilder.append(info.averageRtt);
//            }
//
//            logger.info(stringBuilder.toString());
            try {
                Thread.sleep(50);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

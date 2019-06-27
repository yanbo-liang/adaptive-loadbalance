package com.aliware.tianchi;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class Task implements Runnable {

    @Override
    public void run() {
        while (true) {
            try {
                TestClientFilter.semaphore.acquire(1000);
                ConcurrentMap<String, Integer> weightMap = UserLoadBalance.weightMap;
                ConcurrentMap<String, Boolean> exhaustedMap = TestClientFilter.exhaustedMap;
                if (exhaustedMap.size() > 0 && exhaustedMap.size() < weightMap.size()) {
                    Set<String> changeKeys = new HashSet<>();
                    Set<String> exhaustedKeys = exhaustedMap.keySet();
                    Set<String> weightKeys = weightMap.keySet();
                    for (String key : weightKeys) {
                        if (!exhaustedKeys.contains(key)) {
                            changeKeys.add(key);
                        }
                    }

                    int total = 0;
                    Set<Map.Entry<String, Boolean>> entries = exhaustedMap.entrySet();
                    for (Map.Entry<String, Boolean> entry : entries) {
                        if (entry.getValue()) {
                            int weight = weightMap.get(entry.getKey());
                            if (weight - 5 > 0) {
                                weightMap.put(entry.getKey(), weight - 5);
                                total += 5;
                            }
                        }
                    }
                    while (total > 0) {
                        for (String key : changeKeys) {
                            if (total > 0) {
                                int weight = weightMap.get(key);
                                weightMap.put(key, weight + 1);
                                total -= 1;
                            }
                        }
                    }
                }
                long a = Long.MAX_VALUE;
                String key = null;
                Set<Map.Entry<String, AtomicLong>> entries = TestClientFilter.totalRequestMap.entrySet();

                for (Map.Entry<String, AtomicLong> entry : entries) {
                    long totalTime = TestClientFilter.totalTimeMap.get(entry.getKey()).get();
                    long average = totalTime / entry.getValue().get();
                    if (average < a) {
                        a = average;
                        key = entry.getKey();
                    }
                }
                if (key != null&&(exhaustedMap.getOrDefault(key,false)==false)) {
                    weightMap.compute(key, (k, v) -> v + 10);

                    Set<String> changeKeys = new HashSet<>();
                    Set<String> weightKeys = weightMap.keySet();
                    for (String tmp : weightKeys) {
                        if (!key.equals(tmp)) {
                            changeKeys.add(tmp);
                        }
                    }
                    int total = 10;
                    while (total > 0) {
                        for (String tmp : changeKeys) {
                            if (total > 0) {
                                int weight = weightMap.get(tmp);
                                weightMap.put(tmp, weight - 1);
                                total -= 1;
                            }
                        }
                    }

                }

                TestClientFilter.exhaustedMap.clear();
                TestClientFilter.totalRequestMap.clear();
                TestClientFilter.totalTimeMap.clear();


            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                TestClientFilter.semaphore.release(1000);
            }

            try {
                Thread.sleep(300);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

package com.aliware.tianchi;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class Task implements Runnable {
    Map<String, Integer> map = new HashMap<>();
    private static boolean mess = true;

    @Override
    public void run() {
        while (true) {
            ConcurrentMap<String, Integer> weightMap = UserLoadBalance.weightMap;
            ConcurrentMap<String, Boolean> exhaustedMap = TestClientFilter.exhaustedMap;
//            if (TestClientFilter.startCheck) {
//                TestClientFilter.startCheck = false;
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
                        if (weight - 5 > 20) {
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
            } else if (exhaustedMap.size() == 0) {
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
                if (key != null) {
                    weightMap.compute(key, (k, v) -> v + 5);

                    Set<String> changeKeys = new HashSet<>();
                    Set<String> weightKeys = weightMap.keySet();
                    for (String tmp : weightKeys) {
                        if (!key.equals(tmp)) {
                            changeKeys.add(tmp);
                        }
                    }
                    int total = 5;
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


//                    int size = weightMap.size();
//                    List<String> keyList = new ArrayList<>(weightMap.keySet());
//                        int random = ThreadLocalRandom.current().nextInt(size);
//                        weightMap.compute(keyList.get(random), (k, v) -> v +2);
//
//                        int random1 = ThreadLocalRandom.current().nextInt(size);
//                        weightMap.compute(keyList.get(random1), (k, v) -> v - 2);
//

            }
            TestClientFilter.exhaustedMap.clear();
            TestClientFilter.totalRequestMap.clear();
            TestClientFilter.totalTimeMap.clear();
            System.out.println(weightMap.values());
//            } else {
//                TestClientFilter.startCheck = true;
//                TestClientFilter.exhaustedMap = new ConcurrentHashMap<>();
//            }


            try {
                Thread.sleep(300);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

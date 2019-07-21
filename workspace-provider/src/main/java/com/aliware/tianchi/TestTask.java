package com.aliware.tianchi;

public class TestTask implements Runnable {


    @Override
    public void run() {
        try {
            while (true) {
                Thread.sleep(150);
                TestServerFilter.totalTime.updateAndGet(x -> 0);
                TestServerFilter.totalRequest.updateAndGet(x -> 0);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

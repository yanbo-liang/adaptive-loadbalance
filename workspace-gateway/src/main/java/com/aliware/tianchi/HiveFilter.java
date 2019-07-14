package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.ArrayList;
import java.util.List;

@Activate(group = Constants.CONSUMER)
public class HiveFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            HiveInvokerInfo hiveInvokerInfo = HiveCommon.infoMap.get(invoker.getUrl());
            if (hiveInvokerInfo != null) {

                HiveCommon.pendingRequestTotal.incrementAndGet();

                hiveInvokerInfo.pendingRequest.incrementAndGet();

                HiveCommon.rttMap.put(invocation, System.currentTimeMillis());

            }
            return invoker.invoke(invocation);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        try {
            HiveInvokerInfo hiveInvokerInfo = HiveCommon.infoMap.get(invoker.getUrl());
            if (hiveInvokerInfo != null) {
                Long start = HiveCommon.rttMap.get(invocation);
                if (start != null) {
                    long rtt = System.currentTimeMillis() - start;

//                    hiveInvokerInfo.lock.readLock().lock();
//                    hiveInvokerInfo.totalTime.updateAndGet(x -> x + rtt);
//                    hiveInvokerInfo.totalRequest.incrementAndGet();
//                    hiveInvokerInfo.lock.readLock().unlock();
                    boolean full = false;
                    hiveInvokerInfo.lock.writeLock().lock();

                    hiveInvokerInfo.totalTime += rtt;
                    hiveInvokerInfo.totalRequest += 1;

                    if (hiveInvokerInfo.totalRequest == 500) {
                        hiveInvokerInfo.rttAverage = ((double) hiveInvokerInfo.totalTime) / hiveInvokerInfo.totalRequest;
                        hiveInvokerInfo.totalTime = 0;
                        hiveInvokerInfo.totalRequest = 0;
                        System.out.println(hiveInvokerInfo);
                        full = true;
                    }
                    hiveInvokerInfo.lock.writeLock().unlock();

                    if (full) {
                        UserLoadBalance.selectLock.writeLock().lock();
                        a();
                        setCurrentWeight();
                        UserLoadBalance.selectLock.writeLock().unlock();
                    }

                    HiveCommon.pendingRequestTotal.decrementAndGet();

                    hiveInvokerInfo.pendingRequest.decrementAndGet();

                } else {
                    System.out.println("fuck");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
    private void setCurrentWeight() {
        List<HiveInvokerInfo> infoList = HiveCommon.infoList;
        for (HiveInvokerInfo info : infoList) {
            info.currentWeight = info.weight;
        }
    }
    private void distributeWeightUp(List<HiveInvokerInfo> infoList, double distributedWeight) {
        double weightSum = infoList.stream().mapToDouble(x -> x.weight).sum();
        for (HiveInvokerInfo info : infoList) {
            info.weight = info.weight + (info.weight / weightSum) * distributedWeight;
        }
    }

    private void distributeWeightDown(List<HiveInvokerInfo> infoList, double distributedWeight) {
        double weightSum = infoList.stream().mapToDouble(x -> x.weight).sum();
        for (HiveInvokerInfo info : infoList) {
            info.weight = info.weight - (info.weight / weightSum) * distributedWeight;
        }
    }

    private void a() {
        List<HiveInvokerInfo> infoList = HiveCommon.infoList;
        if (infoList == null) {
            return ;
        }
        double invokerAverage = 0;
        for (HiveInvokerInfo info : infoList) {
            if (info.rttAverage==0){
                return;
            }
            invokerAverage += info.rttAverage * info.weight;
        }
        List<HiveInvokerInfo> belowList = new ArrayList<>();
        List<HiveInvokerInfo> aboveList = new ArrayList<>();

        for (HiveInvokerInfo info : infoList) {
            if (info.rttAverage < invokerAverage) {
                belowList.add(info);
            } else {
                aboveList.add(info);
            }
        }
        double belowWeight = belowList.stream().mapToDouble(x -> x.weight).sum();
        double aboveWeight = aboveList.stream().mapToDouble(x -> x.weight).sum();
        if (belowWeight < aboveWeight) {
            distributeWeightDown(belowList, belowWeight * 0.05);
            distributeWeightUp(aboveList, belowWeight * 0.05);
        } else {
            distributeWeightDown(aboveList, aboveWeight * 0.05);
            distributeWeightUp(belowList, aboveWeight * 0.05);
        }
    }

    private void weightDistributeToFastest() {
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
    }
}

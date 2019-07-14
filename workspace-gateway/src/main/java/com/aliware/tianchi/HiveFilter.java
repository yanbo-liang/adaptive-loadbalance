package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

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
//                    hiveInvokerInfo.lock.writeLock().lock();

                    hiveInvokerInfo.totalTime += rtt;
                    hiveInvokerInfo.totalRequest += 1;

                    if (hiveInvokerInfo.totalRequest == 500) {
                        hiveInvokerInfo.rttAverage = ((double) hiveInvokerInfo.totalTime) / hiveInvokerInfo.totalRequest;
                        hiveInvokerInfo.totalTime = 0;
                        hiveInvokerInfo.totalRequest = 0;
                        System.out.println(hiveInvokerInfo);
                    }

//                    hiveInvokerInfo.lock.writeLock().unlock();

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
}

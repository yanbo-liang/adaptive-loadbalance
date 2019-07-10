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
                Invocation a = invocation;
                a.getArguments();
                Long start = HiveCommon.rttMap.get(invocation);
                if (start != null) {
                    long rtt = System.currentTimeMillis() - start;

                    hiveInvokerInfo.totalTime.updateAndGet(x -> x + rtt);

                    HiveCommon.pendingRequestTotal.decrementAndGet();

                    hiveInvokerInfo.pendingRequest.decrementAndGet();

                    hiveInvokerInfo.totalRequest.incrementAndGet();
                }else{
                    System.out.println("fuck");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}

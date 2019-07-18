package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

@Activate(group = Constants.CONSUMER)
public class HiveFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            HiveInvokerInfo info = HiveCommon.infoMap.get(invoker.getUrl());
            if (info != null) {

                HiveCommon.pendingRequestTotal.incrementAndGet();

                info.pendingRequest.incrementAndGet();

            }
            return invoker.invoke(invocation);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        long end = System.currentTimeMillis();
        try {
            HiveInvokerInfo info = HiveCommon.infoMap.get(invoker.getUrl());
            if (info != null) {
                if (!result.hasException()) {

                }
                HiveCommon.pendingRequestTotal.decrementAndGet();

                info.pendingRequest.decrementAndGet();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}

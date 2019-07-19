package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.concurrent.ConcurrentMap;

@Activate(group = Constants.CONSUMER)
public class HiveFilter implements Filter {
    static final ConcurrentMap<Invocation,Long> rttMap = new ConcurrentReferenceHashMap<>(2000,ConcurrentReferenceHashMap.ReferenceType.WEAK);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            HiveInvokerInfo info = HiveCommon.infoMap.get(invoker.getUrl());
            if (info != null) {

                HiveCommon.pendingRequestTotal.incrementAndGet();

                info.pendingRequest.incrementAndGet();

                rttMap.put(invocation,System.currentTimeMillis());
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
                    Long start = rttMap.get(invocation);
                    if (start!=null){
                        int rtt = (int)(end-start);
                        info.tTime.updateAndGet(x->x+rtt);
                        info.tRequest.incrementAndGet();
                    }
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

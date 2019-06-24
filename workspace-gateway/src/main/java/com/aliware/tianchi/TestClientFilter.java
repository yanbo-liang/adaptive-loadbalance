package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端过滤器
 * 可选接口
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.CONSUMER)
public class TestClientFilter implements Filter {


    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        try {
            String key = invoker.getUrl().toString();

            long start = System.nanoTime();
            Result result = invoker.invoke(invocation);
            long end = System.nanoTime();
            double rtt = end - start;

//            Double value = UserLoadBalance.rttMap.get(key);
//            if (value != null) {
//                if (rtt > value * 2) {
////                Test.block.put(key, 3);
//                    Test.block.compute(key, (k, v) -> {
//                        int tmp;
//                        if (v == null) {
//                            tmp = 1;
//                        } else {
//                            tmp = v + 3;
//                        }
//                        return tmp;
//                    });
//                }
//            }
            UserLoadBalance.rttMap.merge(key, rtt, (oldRtt, newRtt) -> 0.5 * oldRtt + 0.5 * newRtt);
            return result;

        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        return result;
    }
}

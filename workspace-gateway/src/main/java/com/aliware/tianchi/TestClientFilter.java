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

    private ConcurrentHashMap<String, Double> map = new ConcurrentHashMap<>();

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        try {
            String key = invoker.getUrl().toString();
            long start = System.currentTimeMillis();
            Result result = invoker.invoke(invocation);
            long end = System.currentTimeMillis();
            double rtt = end - start;
            Double value = map.get(key);
            if (value != null && rtt > value * 1.8) {
                return null;
//                Test.block.put(key, 3);
//                Test.block.merge(invoker.getUrl().toString(),3,(a,b)->a+1);
            }
            map.merge(key, rtt, (a, b) -> 0.7 * a + 0.3 * b);
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

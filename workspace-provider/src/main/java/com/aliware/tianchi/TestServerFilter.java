package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端过滤器
 * 可选接口
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.PROVIDER)
public class TestServerFilter implements Filter {

    static final Map<Invocation, Long> rttMap = Collections.synchronizedMap(new WeakHashMap<>());
    static final AtomicLong totalTime = new AtomicLong(0);
    static final AtomicLong totalRequest = new AtomicLong(0);

    static final Map<Long, Thread> threadMap = new ConcurrentHashMap<>();
    static final AtomicBoolean inited = new AtomicBoolean(false);
    static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    void init() {
        if (!inited.get()) {
            if (inited.compareAndSet(false, true)) {
                TestTask task = new TestTask();
                executorService.execute(task);
            }
        }

    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        init();
        try {
            threadMap.put(Thread.currentThread().getId(), Thread.currentThread());
            rttMap.put(invocation, System.currentTimeMillis());
            Result result = invoker.invoke(invocation);
            return result;
        } catch (Exception e) {
            throw e;
        }

    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        Long start = rttMap.get(invocation);
        if (start == null) {
            System.out.println("bug! should not happen");
        } else if (!result.hasException()){
            long rtt = System.currentTimeMillis() - start;
            totalTime.updateAndGet(x -> x + rtt);
            totalRequest.incrementAndGet();
        }
        return result;
    }

}

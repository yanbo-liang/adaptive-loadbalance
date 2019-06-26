package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端过滤器
 * 可选接口
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.CONSUMER)
public class TestClientFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(TestClientFilter.class);

    public static final ConcurrentMap<Invocation, Long> rttMap = new ConcurrentReferenceHashMap<>(1024, ConcurrentReferenceHashMap.ReferenceType.WEAK);
    public static ConcurrentMap<String, AtomicLong> totalRequestMap = new ConcurrentHashMap<>();
    public static ConcurrentMap<String, AtomicLong> totalTimeMap = new ConcurrentHashMap<>();


//    public static final ConcurrentMap<String, AtomicLong> invokerRttMap = new ConcurrentHashMap<>();
////
//    public static final ConcurrentMap<String, AtomicInteger> pendingMap = new ConcurrentHashMap<>();

    public static ConcurrentMap<String, Boolean> exhaustedMap = new ConcurrentHashMap<>();

    public volatile static boolean startCheck = true;
//    public static final ConcurrentMap<String, AtomicInteger> totalMap = new ConcurrentHashMap<>();
//
//    @Override
//    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
//        try {
//            String key = invoker.getUrl().toString();
//
//            AtomicInteger totalCount = totalMap.get(key);
//            if (totalCount == null) {
//                synchronized (TestClientFilter.class) {
//                    if (totalMap.get(key) == null) {
//                        totalMap.put(key, new AtomicInteger(0));
//                    }
//                }
//            }
//            Result result = invoker.invoke(invocation);
//            return result;
//        } catch (Exception e) {
//            throw e;
//        }
//    }
//
//    @Override
//    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
//        String key = invoker.getUrl().toString();
//        AtomicInteger totalCount = totalMap.get(key);
//        if (totalCount != null) {
//            totalCount.incrementAndGet();
//        }
//
//        return result;
//    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            String key = invoker.getUrl().toString();


//            AtomicInteger pendingCount = pendingMap.get(key);
//            if (pendingCount == null) {
//                synchronized (TestClientFilter.class) {
//                    if (pendingMap.get(key) == null) {
//                        pendingMap.put(key, new AtomicInteger(1));
//                    }
//                }
//            } else {
//                pendingCount.incrementAndGet();
//            }


            rttMap.put(invocation, System.currentTimeMillis());

            Result result = invoker.invoke(invocation);
            return result;
        } catch (Exception e) {
            throw e;
        }
    }


    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        String key = invoker.getUrl().toString();
        if (startCheck) {
            if (result.hasException()) {
                if (result.getException() instanceof RuntimeException) {
                    boolean exhausted = exhaustedMap.getOrDefault(key, false);
                    if (!exhausted) {
                        exhaustedMap.put(key, true);
                    }
                }
            }
        }
        long rtt = System.currentTimeMillis() - rttMap.get(invocation);

        AtomicLong totalRequest = totalRequestMap.get(key);
        if (totalRequest == null) {
            synchronized (totalRequestMap) {
                AtomicLong atomicLong = totalRequestMap.get(key);
                if (atomicLong == null) {
                    totalRequestMap.put(key, new AtomicLong(1));
                } else {
                    atomicLong.incrementAndGet();
                }
            }
        } else {
            totalRequest.incrementAndGet();
        }

        AtomicLong totalTime = totalTimeMap.get(key);
        if (totalTime == null) {
            synchronized (totalTimeMap) {
                AtomicLong atomicLong = totalTimeMap.get(key);
                if (atomicLong == null) {
                    totalTimeMap.put(key, new AtomicLong(rtt));
                } else {
                    atomicLong.updateAndGet(x -> x + rtt);
                }
            }
        } else {
            totalTime.updateAndGet(x -> x + rtt);
        }


//        AtomicInteger pendingCount = pendingMap.get(key);
//        if (pendingCount != null) {
//            pendingCount.decrementAndGet();
//            if (result.hasException()) {
//                logger.info(pendingCount.get() + "");
//            }
//        }


//
//            AtomicLong invokerRtt = invokerRttMap.get(key);
//            if (invokerRtt == null) {
//                synchronized (TestClientFilter.class) {
//                    if (invokerRttMap.get(key) == null) {
//                        invokerRttMap.put(key, new AtomicLong(tmp));
//                    }
//                }
//            } else {
//                long a = invokerRtt.get();
//                if (tmp > a * 1.5) {
//                    blockMap.get(key).updateAndGet(x -> x + 1);
//                } else {
//
//                }
//                invokerRtt.accumulateAndGet(tmp, (old, param) -> (long) (0.8 * old + 0.2 * param));
//            }
//


        return result;
    }
}

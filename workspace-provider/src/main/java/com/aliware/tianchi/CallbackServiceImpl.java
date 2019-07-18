package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端回调服务
 * 可选接口
 * 用户可以基于此服务，实现服务端向客户端动态推送的功能
 */
public class CallbackServiceImpl implements CallbackService {

    private final Map<String, CallbackListener> listeners = new ConcurrentHashMap<>();

    public CallbackServiceImpl() {
        DataStore defaultExtension = ExtensionLoader.getExtensionLoader(DataStore.class).getDefaultExtension();
        Map<String, Object> map = defaultExtension.get(Constants.EXECUTOR_SERVICE_COMPONENT_KEY);
        Object port = new ArrayList<>(map.keySet()).get(0);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) map.get(port);
        int maximumPoolSize = threadPoolExecutor.getMaximumPoolSize();
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                if (!listeners.isEmpty()) {
                    for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                        try {
                            Collection<Thread> values = TestServerFilter.threadMap.values();
                            int count = 0;
                            for (Thread t : values) {
                                if (t.getState().equals(Thread.State.TIMED_WAITING)) {
                                    count++;
                                }
                            }
                            long totalTime = TestServerFilter.totalTime.get();
                            long totalRequest = TestServerFilter.totalRequest.get();
                            TestServerFilter.totalTime.updateAndGet(x -> 0);
                            TestServerFilter.totalRequest.updateAndGet(x -> 0);
                            entry.getValue().receiveServerMsg(System.getProperty("quota") + "-" + maximumPoolSize + "-" + count + "-" + totalTime + "-" + totalRequest);
                        } catch (Throwable t1) {
                            t1.printStackTrace();
//                            listeners.remove(entry.getKey());
                        }
                    }
                }
            }
        }, 0, 10);
    }


    @Override
    public void addListener(String key, CallbackListener listener) {
        listeners.put(key, listener);
    }
}

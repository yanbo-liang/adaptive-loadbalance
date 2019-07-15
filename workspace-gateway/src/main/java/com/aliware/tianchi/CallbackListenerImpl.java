package com.aliware.tianchi;


import org.apache.dubbo.rpc.listener.CallbackListener;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端监听器
 * 可选接口
 * 用户可以基于获取获取服务端的推送信息，与 CallbackService 搭配使用
 */
public class CallbackListenerImpl implements CallbackListener {

    @Override
    public void receiveServerMsg(String msg) {
        int index = msg.indexOf('-');
        if (index != -1) {
            String name = msg.substring(0, index);
            int maxRequest = Integer.valueOf(msg.substring(index + 1));
            for (HiveInvokerInfo info : HiveCommon.infoMap.values()) {
                if (info.name.equals(name)) {
                    info.maxPendingRequest = maxRequest;
                    break;
                }
            }
        }
        HiveCommon.initCallBack();
//        System.out.println("receive msg from server :" + msg);
    }
}
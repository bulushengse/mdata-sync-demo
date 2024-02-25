//package com.zhoubc.mdata.sync.queue;
//
//import com.zhoubc.mdata.sync.utils.Action;
//import com.zhoubc.mdata.sync.utils.MapUtil;
//import org.apache.commons.lang3.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentSkipListSet;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
///**
// * 工作队列池
// *
// * 2个队列池，，
// *
// *
// * @author zhoubc
// * @description: TODO
// * @date 2023/7/9 18:33
// */
//public class RoutableBatchWorker {
//    private static final Logger logger = LoggerFactory.getLogger(RoutableBatchWorker.class);
//
//    //队列池
//    private BatchQueue<RoutableData>[] queues;
//
//    //队列池大小
//    private int workerCount;
//
//    //队列池里的每个队列信息  线程名caller，线程状态
//    private Map<String, RoutableBatchWorker.CallerState> signals = new HashMap<>();
//
//    //队列池每个队列里的工作任务
//    private Action<List<RoutableData>> batchHandler;
//
//
//    public RoutableBatchWorker(int workerCount, int batchSize, int pollTimeoutMs, Action<List<RoutableData>> bacthHandler){
//        if ( workerCount < 1 || workerCount > 32 ){
//            throw new IllegalArgumentException("workerCount不合法，应该在1-32之间");
//        }
//        if ( batchSize > 0 ){
//            throw new IllegalArgumentException("batchSize不合法，不能小于等于0");
//        }
//        if ( pollTimeoutMs < 10 ){
//            throw new IllegalArgumentException("pollTimeoutMs不合法，不能小于10");
//        }
//        if ( bacthHandler == null ){
//            throw new IllegalArgumentException("bacthHandler不合法，不能为空");
//        }
//
//        //工作任务，数据写入写ES
//        this.batchHandler = bacthHandler;
//
//        //创建队列池
//        this.workerCount = workerCount;
//        this.queues = new BatchQueue[workerCount];
//        for (int i = 0; i <this.queues.length; i++) {
//            //创建一个生产者
//            BatchQueue<RoutableData> q = new BatchQueue<>();
//            q.setName("w"+i);
//            q.setQueueSize(batchSize);
//            q.setPollTimeout(pollTimeoutMs); //100* 2048
//
//            q.setHandlerTaskAction(this::batchHandler);
//            q.setStopTaskAction(this::stopTaskHandler);
//
//            //这里会起一个线程，自行管理BlockingQueue队列里的任务，，
//            q.doInitialize();
//            this.queues[i] = q;
//        }
//    }
//
//    private void batchHandler(List<RoutableData> list) {
//        this.batchHandler.execute(list);
//    }
//
//    private void stopTaskHandler(int type, RoutableData data) {
//        if (type < 0 && data != null) {
//            CountDownLatch pc = (CountDownLatch) data.getUserData();
//            if (pc != null) {
//                pc.countDown();
//            }
//        }
//    }
//
//    public void acquire(String caller) {
//        RoutableBatchWorker.CallerState c = this.getCallerState(caller);
//        c.acquire();
//    }
//
//    public void queue(String caller, String endpoint, Object data){
//        int key = mod(endpoint, this.workerCount);
//
//        RoutableBatchWorker.CallerState c = this.getCallerState(caller);
//        c.registerSignalKey(key);
//
//        BatchQueue<RoutableData> q = this.queues[key];
//        q.insert(0, RoutableData.of(caller, endpoint, data));
//    }
//
//    public boolean release(String caller, int timeoutMs) {
//        RoutableBatchWorker.CallerState c = this.getCallerState(caller);
//        c.freeze();
//        return c.release(timeoutMs);
//    }
//
//
//
//    private static int mod(String data, int all){
//        int hc = Math.abs(Objects.hashCode(data));
//        return hc % all;
//    }
//
//    private RoutableBatchWorker.CallerState getCallerState(String caller) {
//        return MapUtil.getValue(this.signals, caller, (k) -> {
//            return new RoutableBatchWorker.CallerState(caller, this.queues);
//        });
//    }
//
//    private static class CallerState {
//        public static int READY = 0;
//        public static int WORKING = 1;
//        public static int FOROZEN = 2;
//        private AtomicInteger state;
//        private String caller;
//        private BatchQueue<RoutableData>[] queues ;
//
//        private Set<Integer> signalKeys;
//        private Map<Integer, CountDownLatch> singnals;
//        private SingleAccessorSupport singleAccessorSupport;
//
//        public CallerState(String caller, BatchQueue<RoutableData>[] queues) {
//            this.state = new AtomicInteger(READY);
//            this.singnals = MapUtil.concurrentHashMap();
//            if (StringUtils.isBlank(caller)){
//                throw new IllegalArgumentException("caller不能为空");
//            }
//            if ( queues == null ){
//                throw new IllegalArgumentException("queues不能为null");
//            }
//            this.caller = caller;
//            this.queues = queues;
//            this.singleAccessorSupport = new SingleAccessorSupport(this.getClass().getName());
//        }
//
//        public boolean isReady() {
//            return this.state.get() == READY;
//        }
//
//        public boolean isWorking() {
//            return this.state.get() == WORKING;
//        }
//
//        public void registerSignalKey(int index) {
//            this.checkState(WORKING, "working");
//            if (this.signalKeys == null) {
//                this.signalKeys = new ConcurrentSkipListSet<>();
//            }
//            this.signalKeys.add(index);
//        }
//
//        public void acquire() {
//            this.singleAccessorSupport.acquire();
//            if(!this.isWorking()){
//                boolean ok = this.state.compareAndSet(READY, WORKING);
//                if (!ok) {
//                   throw new ErrorStatusException(this.caller + "调用acquire失败，无法设置为working状态，因为当前状态为" + this.state.get());
//                }
//            }
//        }
//
//        public Set<String> freeze() {
//            this.singleAccessorSupport.validate();
//            if (this.state.compareAndSet(WORKING, FOROZEN)) {
//                if (this.signalKeys == null) {
//                      return null;
//                }
//
//                Iterator it = this.signalKeys.iterator();
//                while(it.hasNext()) {
//                    int k = (Integer)it.next();
//                    CountDownLatch cd = new CountDownLatch(1);
//                    this.singnals.put(k, cd);
//                    this.queues[k].insert(-1, RoutableData.of(this.caller, String.valueOf(k), cd));
//                }
//            }
//
//            return null;
//        }
//
//        public boolean release(int timeoutMs) {
//            this.singleAccessorSupport.validate();
//            if (this.isReady()) {
//                return true;
//            } else if (this.state.compareAndSet(FOROZEN, READY)) {
//                boolean f = true;
//                for (Map.Entry<Integer, CountDownLatch> m : this.singnals.entrySet()) {
//                    boolean finished = this.await(m.getKey(), m.getValue(), timeoutMs);
//                    if (!finished) {
//                        f = false;
//                    }
//                }
//                this.singnals.clear();
//                this.singleAccessorSupport.release();
//                return f;
//            } else {
//                throw new RuntimeException("请先调用freeze方法，["+this.caller+"]当前状态："+this.state.get());
//            }
//        }
//
//        private boolean await(int index, CountDownLatch cd, int timeoutMs) {
//            if (cd == null) {
//                return true;
//            } else {
//                try {
//                    boolean f = cd.await(timeoutMs, TimeUnit.MILLISECONDS);
//                    if (!f) {
//                        //log.warn
//                    }
//                    return f;
//                } catch (InterruptedException e) {
//                    //log.error
//                    return false;
//                }
//            }
//        }
//
//        private void checkState(int expected, String statusName) {
//            int st = this.state.get();
//            if (st != expected) {
//                throw new RuntimeException(this.caller + "状态不对，期待状态是" + statusName + "，当前状态是：" + st);
//            }
//        }
//
//
//    }
//
//}

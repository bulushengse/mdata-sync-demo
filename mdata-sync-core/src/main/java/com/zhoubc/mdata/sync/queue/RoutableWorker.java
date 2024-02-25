package com.zhoubc.mdata.sync.queue;

import com.zhoubc.mdata.sync.utils.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 工作人
 * @author zhoubc
 * @description: TODO
 * @date 2023/7/9 18:33
 */
public class RoutableWorker {
    private static final Logger logger = LoggerFactory.getLogger(RoutableWorker.class);

    //工作队列
    private BatchQueue<RoutableData> queue;

    private String caller;
    private String endpoint;

    private static int READY = 0;
    private static int WORKING = 1;
    private static int FOROZEN = 2;
    private AtomicInteger state;

    private CountDownLatch countDownLatch;
    private SingleAccessorSupport singleAccessorSupport;

    //工作队列的任务
    private Action<List<RoutableData>> batchHandler;

    public RoutableWorker(String caller, int batchSize, int pollTimeoutMs, Action<List<RoutableData>> bacthHandler){
        if ( batchSize <= 0 ){
            throw new IllegalArgumentException("batchSize不合法，不能小于等于0");
        }
        if ( pollTimeoutMs < 10 ){
            throw new IllegalArgumentException("pollTimeoutMs不合法，不能小于10");
        }
        if ( bacthHandler == null ){
            throw new IllegalArgumentException("bacthHandler不合法，不能为空");
        }

        //线程名称
        this.caller = caller;
        //工作任务，数据写入ES 任务
        this.batchHandler = bacthHandler;

        this.state = new AtomicInteger(READY);
        this.singleAccessorSupport = new SingleAccessorSupport(this.getClass().getName());

        //创建工作队列
        BatchQueue<RoutableData> q = new BatchQueue<>();
        q.setName(caller);
        q.setBatchSize(batchSize);
        q.setPollTimeout(pollTimeoutMs); //100* 2048
        q.setHandlerTaskAction(this::batchHandler);
        q.setStopTaskAction(this::stopTaskHandler);

        //这里会起一个线程，自行管理BlockingQueue队列里的任务
        q.doInitialize();
        this.queue = q;
    }

    private void batchHandler(List<RoutableData> list) {
        this.batchHandler.execute(list);
    }

    /**
     *  一批同步数据队列结尾处放入一个计数器，当计数器为0时，这批数据同步完成，工作状态初始化
     */
    private void stopTaskHandler(int type, RoutableData data) {
        if (type < 0 && data != null) {
            CountDownLatch pc = (CountDownLatch) data.getUserData();
            if (pc != null) {
                //计数器-1
                pc.countDown();
            }
        }
    }

    private Boolean isReady() {
        return this.state.get() == READY;
    }

    private Boolean isWorking() {
        return this.state.get() == WORKING;
    }

    /**
     * 队列状态改为工作状态
     */
    public void acquire() {
        this.singleAccessorSupport.acquire();
        if(!this.isWorking()){
            boolean ok = this.state.compareAndSet(READY, WORKING);
            if (!ok) {
                throw new ErrorStatusException(this.caller + "调用acquire失败，无法设置为working状态，因为当前状态为" + this.state.get());
            }
        }
    }

    /**
     * 更新数据放入队列
     */
    public void queue(String endpoint, Object data){
        int st = this.state.get();
        if (st != WORKING) {
            throw new RuntimeException(this.caller + "状态不对，期待状态是" + WORKING + "，当前状态是：" + st);
        }

        this.endpoint = endpoint;
        this.queue.insert(0, RoutableData.of(this.caller, endpoint, data));
    }

    /**
     * 队列状态改为准备状态，等待下一次同步轮询
     */
    public Boolean release(int awaitTimeoutMs) {
        this.singleAccessorSupport.validate();
        if (this.state.compareAndSet(WORKING, FOROZEN)) {
            // 向队列里放入一个=1计数器countDownLatch，表示在这批数据结尾放入一个标识。
            this.countDownLatch = new CountDownLatch(1);
            this.queue.insert(-1, RoutableData.of(this.caller, this.endpoint, this.countDownLatch));
        }

        if (this.isReady()) {
            return true;
        }

        if (this.state.compareAndSet(FOROZEN, READY)) {
            this.singleAccessorSupport.release();
            // 等待结尾标识的计数器为0，说明这批数据同步ES完毕
            Boolean finished = this.await(this.countDownLatch, awaitTimeoutMs);
            return finished;
        }

        throw new RuntimeException("请先调用freeze方法，["+this.caller+"]当前状态："+this.state.get());
    }

    private Boolean await(CountDownLatch cd, int timeoutMs) {
        if (cd == null) {
            return true;
        } else {
            try {
                boolean f = cd.await(timeoutMs, TimeUnit.MILLISECONDS);
                if (!f) {
                    //log.warn
                }
                return f;
            } catch (InterruptedException e) {
                //log.error
                return false;
            }
        }
    }

}

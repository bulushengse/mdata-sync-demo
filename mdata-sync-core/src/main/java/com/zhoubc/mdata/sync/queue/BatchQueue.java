package com.zhoubc.mdata.sync.queue;

import com.zhoubc.mdata.sync.utils.Action;
import com.zhoubc.mdata.sync.utils.Action2;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 阻塞队列 (BlockingQueue)是Java util.concurrent包下重要的数据结构，BlockingQueue提供了线程安全的队列访问方式：
 * 当阻塞队列进行插入数据时，如果队列已满，线程将会阻塞等待直到队列非满；从阻塞队列取数据时，如果队列已空，线程将会阻塞等待直到队列非空。
 * 阻塞队列常用于生产者和消费者的场景，生产者是往队列里添加元素的线程，消费者是从队列里拿元素的线程。
 * 并发包下很多高级同步类的实现都是基于BlockingQueue实现的。
 * @author zhoubc
 * @description: TODO
 * @date 2023/7/9 18:45
 */
@Data
public class BatchQueue<T>{
    private static final Logger logger = LoggerFactory.getLogger(BatchQueue.class);

    //阻塞队列
    private BlockingQueue<QueueData<T>> blockingQueue = new LinkedBlockingQueue();
    private boolean working = true;

    private int batchSize = 100;
    private int pollTimeout = 10;
    private String name = null;

    private volatile int threadCounter = 0;

    private Thread workingThread = null;

    private Action<List<T>> handlerTaskAction;
    private Action2<Integer, T> stopTaskAction;

    private int maxSize = 100;


    public void doInitialize(){
        if (this.batchSize <= 0) {
            throw new IllegalArgumentException("batchSize不能小于等于0");
        }
        if (this.pollTimeout <= 0) {
            throw new IllegalArgumentException("timeout不能小于等于0");
        }
        if (this.name == null) {
            throw new IllegalArgumentException("name不能为null");
        }
        if (this.handlerTaskAction == null) {
            throw new IllegalArgumentException("callback不能为null");
        }

         this.workingThread = new Thread(() -> {
             ArrayList handlerTasks;
             ArrayList stopTask;
             for (; this.working; this.doCallback(handlerTasks,stopTask)) {
                 handlerTasks = new ArrayList();
                 stopTask = new ArrayList();

                 for (int i = 0; i < this.batchSize; i++) {
                     try {
                         //获取并移除此队列的头元素，如果在元素可用前超过了指定的等待时间，则返回null，当等待时可以被中断
                         QueueData<T> entry = this.blockingQueue.poll(this.pollTimeout, TimeUnit.MILLISECONDS);
                         if (entry == null) {
                             break;
                         }
                         if (entry.getType() < 0) {
                             stopTask.add(entry);
                             break;
                         }
                         handlerTasks.add(entry.getData());
                     } catch (InterruptedException e) {
                         this.working = false;
                         break;
                     }
                 }
             }
         });
        this.workingThread.setName(this.name + "_batch_worker_" + threadCounter++);
        this.workingThread.setDaemon(true);
        this.workingThread.start();
    }

    private void doCallback(List<T> handlerTasks, List<QueueData<T>> stopTask) {
        if (handlerTasks != null && handlerTasks.size() > 0 && this.handlerTaskAction != null) {
            this.handlerTaskAction.execute(handlerTasks);
        }

        if (stopTask != null && stopTask.size() > 0 && this.stopTaskAction != null) {
            Iterator iterator = stopTask.iterator();

            while(iterator.hasNext()) {
                QueueData item = (QueueData) iterator.next();

                this.stopTaskAction.execute(item.getType(), (T) item.getData());
            }
        }
    }

//    public void insert(T entry) {
//        this.insert(0, entry);
//    }

    public void insert(int type, T entry) {
        this.checkStatus();
        int size = this.blockingQueue.size();
        if (size >= this.maxSize) {
            throw new RuntimeException("队列大小太大了，达到 " + size + ",请稍后再试");
        } else {
            if (entry != null) {
                try {
                    this.blockingQueue.put(QueueData.of(type, entry));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

//    public void insert(List<T> list) {
//        this.checkStatus();
//        if (list != null) {
//            Iterator it = list.iterator();
//            while(it.hasNext()) {
//                T item = (T) it.next();
//                this.insert(item);
//            }
//        }
//    }


    public void stop() {
        this.checkStatus();
        this.working = false;
        if (this.workingThread != null && !this.workingThread.isInterrupted()) {
            this.workingThread.interrupt();
        }
    }

    private void checkStatus(){
        if (!this.working) {
            throw new RuntimeException("队列已经停止了");
        }
    }

}

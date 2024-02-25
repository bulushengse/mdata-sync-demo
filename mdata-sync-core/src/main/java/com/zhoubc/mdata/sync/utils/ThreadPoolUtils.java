package com.zhoubc.mdata.sync.utils;

import org.apache.lucene.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/12 22:05
 */
public class ThreadPoolUtils {
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolUtils.class);

    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    public static ExecutorService buildConsumeThreadPool(final String threadName) {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(20,256,1L, TimeUnit.MINUTES, new ArrayBlockingQueue<>(2048));
        pool.setThreadFactory(new NamedThreadFactory("thread-factory-task-group-"+threadName));
        pool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                if (!executor.isShutdown()) {
                    r.run();
                    logger.warn("{}线程池满了，直接执行任务，activeCount={}", threadName, executor.getActiveCount());
                }
            }
        });

        return pool;
    }


    public static ExecutorService buildESWriteThreadPool(){
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(1024);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(CPU_COUNT*4, CPU_COUNT*16, 30, TimeUnit.SECONDS, workQueue);
        pool.allowCoreThreadTimeOut(true);
        pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        return pool;
    }

}

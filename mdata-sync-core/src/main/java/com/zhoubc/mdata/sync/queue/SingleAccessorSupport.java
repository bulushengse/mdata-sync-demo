package com.zhoubc.mdata.sync.queue;

import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/7/16 18:32
 */
public class SingleAccessorSupport {

    private final AtomicLong currentThreadId = new AtomicLong(-1L);
    private final AtomicInteger refCount = new AtomicInteger(0);
    private String errorMessage;

    public SingleAccessorSupport(String className) {
        this.errorMessage = className + "is not safe for multi-threaded access";
    }

    public final void validate() {
        long threadId = Thread.currentThread().getId();
        if (this.currentThreadId.get() != threadId) {
            this.throwError();
        }
    }

    public final void acquire() {
        long threadId = Thread.currentThread().getId();
        if (threadId != this.currentThreadId.get() && !this.currentThreadId.compareAndSet(-1L, threadId)) {
            this.throwError();
        }
    }

    public final void release() {
        if (this.refCount.decrementAndGet() == 0) {
            this.currentThreadId.set(-1L);
        }
    }

    private void throwError() {
        throw new ConcurrentModificationException(this.errorMessage);
    }


}

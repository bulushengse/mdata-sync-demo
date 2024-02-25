package com.zhoubc.mdata.sync.utils;

import java.util.concurrent.Callable;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/4/2 18:13
 */
public class ExecutorUtil {

    public static<T> T callWithRetry(Callable<T> callable, int retryTimes, Func2<Integer, Exception, Boolean> exceptionHandler){
        return callWithRetry(callable, retryTimes, exceptionHandler, 0, false);
    }

    public static<T> T callWithRetry(Callable<T> callable, int retryTimes, Func2<Integer, Exception, Boolean> exceptionHandler, int sleepMsBeforeRetry, boolean sleepExponentially){
        if (null == callable){
            throw new IllegalArgumentException("入参callable不能为空");
        } else if (retryTimes < 1) {
            throw new IllegalArgumentException(String.format("入参retryTimes[%d]不能小于1", retryTimes));
        } else {
            int i = 0;

            while(i < retryTimes){
                try {
                    return callable.call();
                } catch (Exception e) {
                    if (exceptionHandler != null) {
                        boolean canContinue = exceptionHandler.execute(i, e);
                        if (!canContinue) {
                            return null;
                        }
                    }

                    if (sleepMsBeforeRetry > 0 && i + 1 < retryTimes) {
                        long timeToSleep = (long) sleepMsBeforeRetry * (sleepExponentially ? (long)Math.pow(2.0D, (double)i) : 1L );

                        try {
                            Thread.sleep(Math.min(timeToSleep, 4096L));
                        } catch (InterruptedException ie) {
                        }
                    }

                    ++i;
                }
            }


        }
        return null;
    }

}

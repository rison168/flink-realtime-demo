package com.rison.bigdata.utils;

import com.sun.corba.se.spi.orbutil.threadpool.NoSuchThreadPoolException;
import org.apache.hadoop.util.ThreadUtil;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @PACKAGE_NAME: com.rison.bigdata.utils
 * @NAME: ThreadPoolUtil
 * @USER: Rison
 * @DATE: 2022/9/4 21:45
 * @PROJECT_NAME: flink-realtime-demo
 **/
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil(){}

    public static ThreadPoolExecutor getThreadPoolExecutor(){
        if (threadPoolExecutor == null){
            synchronized (ThreadUtil.class){
                if (threadPoolExecutor == null){
                    threadPoolExecutor = new ThreadPoolExecutor(
                            20,
                            50,
                            1L,
                            TimeUnit.MINUTES,
                            new LinkedBlockingDeque<>(128)
                    );
                }
            }
        }
        return threadPoolExecutor;
    }
}

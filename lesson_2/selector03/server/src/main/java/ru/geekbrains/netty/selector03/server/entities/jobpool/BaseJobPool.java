package ru.geekbrains.netty.selector03.server.entities.jobpool;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;

public class BaseJobPool {

    private ReentrantLock watchdogLock = new ReentrantLock();
    private ThreadPoolExecutor threadPool;


    /**
     * Shutdown pool
     * <br>
     * Will wait all remaining threads to finish
     */
    public void close() {

        // waiting all pool threads to complete for 30 sec then terminate pool

        watchdogLock.lock();

        //watchdog
        int waitCount = 0;
        while (threadPool.getActiveCount() > 0) {
            try {
                waitCount++;
                watchdogLock.wait(1000);
            }
            catch (InterruptedException ignore) {}

            if (waitCount > 30)
                break;
        }

        watchdogLock.unlock();

        // terminate thread pool
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    public void stop() {

        threadPool.shutdownNow();
    }
}

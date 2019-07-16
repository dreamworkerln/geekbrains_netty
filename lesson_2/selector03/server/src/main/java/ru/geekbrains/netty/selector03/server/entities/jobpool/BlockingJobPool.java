package ru.geekbrains.netty.selector03.server.entities.jobpool;

import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;



// ----------------------------------------------------------------------




// =========================================================================================


/**
 * Job pool fixed size.
 * <br>
 * When busy threads exceeds pool size
 * Then thread that calls Blocking JobPool.ads(...) would be blocked until some jobs have been finished
 * @param <T>
 */
public class BlockingJobPool<T> extends BaseJobPool {

    private int queueSize;



    //private Semaphore semaphore;

    // ThreadPoolExecutor.getActiveCount() precious replacement
    private final AtomicInteger workingThreadCnt = new AtomicInteger(0);

    // On job done handler
    private Consumer<T> callback;

    //private final AtomicInteger threadCount = new AtomicInteger(0);

    /**
     * Pool of worker threads
     * @param poolSize pool size (count of threads)
     * @param callback handler onComplete event
     */
    public BlockingJobPool(int poolSize, Consumer<T> callback) {

        queueSize = poolSize*2;

        final CustomizableThreadFactory threadFactory = new CustomizableThreadFactory();
        threadFactory.setDaemon(true);
        threadFactory.setThreadNamePrefix("BlockingPool-");

//        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(4);
//        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 1000,
//                TimeUnit.MILLISECONDS, queue);


        threadPool = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(queueSize),
                threadFactory);

        //threadPool = (ThreadPoolExecutor)Executors.newFixedThreadPool(poolSize, threadFactory);

        this.callback = callback;

        //semaphore = new Semaphore(poolSize, true);
    }

    /**
     * Add job to execute
     * <br>
     * If pool have all it's threads busy then thread that called BlockingJobPool.add(...)
     * will wait until some thread in pool have finished it's job
     * @param job Supplier
     */
    public void add(Supplier<T> job) {

        //try {
            //semaphore.acquire();

            workingThreadCnt.getAndIncrement();

            CompletableFuture.supplyAsync(job, threadPool)
                    .handle(this::handle)
                    .thenAccept(this::callback);

        //}
        // stop job evaluation if interrupted
//        catch (InterruptedException e) {
//
//            // if thread was interrupted inside job
//            //semaphore.release();
//        }
    }


    // ----------------------------------------------------------------------------------------------------


    /**
     * WorkProcessable.work error handler
     * please handle you exeptions directly in job (lambda) - this.add(Supplier T  job)
     */
    private T handle(T result, Throwable e) {

        if (e != null) {
            e.printStackTrace();
        }


        return result;
    }


    /**
     * On job done
     * @param msg T
     */
    private void callback(T msg) {

        // notify caller about job done
        callback.accept(msg);

        workingThreadCnt.getAndDecrement();
        //semaphore.release();
    }


    public boolean isFull() {
        return workingThreadCnt.get() == threadPool.getMaximumPoolSize();
    }



    // ----------------------------------------------------------------------------------------------------

}




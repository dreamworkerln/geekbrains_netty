package ru.geekbrains.netty.selector02.server.entities.jobpool;

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



    private Semaphore semaphore;

    private ThreadPoolExecutor threadPool;

    // ThreadPoolExecutor.getActiveCount() precious replacement
    private final AtomicInteger workingThreadCnt = new AtomicInteger(0);

    // On job done handler
    private Consumer<T> callback;

    private final AtomicInteger threadCount = new AtomicInteger(0);

    /**
     * Pool of worker threads
     * @param poolSize pool size (count of threads)
     * @param callback handler onComplete event
     */
    public BlockingJobPool(int poolSize,
                           Consumer<T> callback) {

        final CustomizableThreadFactory threadFactory = new CustomizableThreadFactory();
        threadFactory.setDaemon(true);
        threadFactory.setThreadNamePrefix("BlockingPool-");

        threadPool = (ThreadPoolExecutor)Executors.newFixedThreadPool(poolSize, threadFactory);
        this.callback = callback;

        semaphore = new Semaphore(poolSize, true);
    }

    /**
     * Add job to execute
     * <br>
     * If pool have all it's threads busy then thread that called BlockingJobPool.add(...)
     * will wait until some thread in pool have finished it's job
     * @param job Supplier
     */
    public void add(Supplier<T> job) {

        try {
            semaphore.acquire();

            threadCount.getAndIncrement();

            CompletableFuture.supplyAsync(job, threadPool)
                    .handle(this::handle)
                    .thenAccept(this::callback);

        }
        // stop job evaluation if interrupted
        catch (InterruptedException e) {

            // if thread was interrupted inside job
            semaphore.release();
        }
    }


    // ----------------------------------------------------------------------------------------------------


    /**
     * WorkProcessable.work error handler - please handle you exeptions directly in job - this.add(Supplier T  job)
     */
    private T handle(T result, Throwable e) {

//        //System.out.println("handle");
//
//        // not forget to check if (e == null)
//        while (e != null && !(e instanceof PayloadedException)) {
//            e = e.getCause();
//        }
//
//        if (e != null) {
//            //System.out.println("error");
//
//            // Создаем экземпляр класса T (путем вызова Function<Object,T> creator)
//            // и передаем в конструктор PayloadedException.getPayload() - id задачи
//            // Таким образом вызывающая сторона узнает, что задача с указанным id не была выполнена
//            // Т.к. у T есть поле T.isOk = false, и оно становится == true только при успешном выполнении job
//            result = creator.apply(((PayloadedException)e).getPayload());
//        }

        return result;
    }


    /**
     * On job done
     * @param msg T
     */
    private void callback(T msg) {

        // notify caller about job done
        callback.accept(msg);

        threadCount.getAndDecrement();
        semaphore.release();
    }


    public boolean isFull() {
        return threadCount.get() == threadPool.getMaximumPoolSize();
    }



    // ----------------------------------------------------------------------------------------------------

}


/*
class ExampleService {

    // -----------------------------------------------------------



    JobResult work(int id, String data) {

        JobResult result = null;

        try {
            String name = Thread.currentThread().getName();
            long timeout = 1000;
            //System.out.printf("%s sleeping %d %s...%n", name, timeout, "ms");
            Thread.sleep(1000);

            char[] str = new char[6];
            ThreadLocalRandom current = ThreadLocalRandom.current();

            int i = 100000 + current.nextInt(900000);


            if (i > 900000)
                throw new IllegalArgumentException(id + ": Надоело");

            result = new JobResult(id, Integer.toString(i));

        }
        catch (IllegalArgumentException e) {
            throw e;
        }
        catch (Exception e) {

            System.out.println("Error: " + e);
        }
        return result;
    }

}
*/





/*


        // Handle exception ---------------------------------------------
        f.handle((result, throwable) -> {

                    System.out.println("Error: " + throwable.toString());

                    // may be null
                    return result;
                });
        // --------------------------------------------------------------
 */




/*


    //private ReentrantLock lock = new ReentrantLock();
    //private Condition lockCondition = lock.newCondition();

    // ThreadPoolExecutor.getActiveCount() precious replacement
    // private final AtomicInteger threadCount = new AtomicInteger(0);





    public void add(Supplier<T> job) {



            // wait if ThreadPoolExecutor threads is busy
        lock.lock();

        // threadPool.getActiveCount() is laggy and inaccurate
        while (threadCount.get() == threadPool.getMaximumPoolSize()) {
            try {

                lockCondition.await(1000, TimeUnit.MILLISECONDS); // Suspend BlockingJobPool.add caller thread
            }
            catch (InterruptedException ignore) {}
        }

        lock.unlock();

        // increasing busy thread counter
        threadCount.getAndIncrement();



        ...
    }







    private void callback(T msg) {

    // decreasing busy thread counter
    threadCount.getAndDecrement();

    // notify about job done
    callback.accept(msg);

    // resume BlockingJobPool.add waiting thread if exists one
    lock.lock();
    lockCondition.signal();
    lock.unlock();


}




*/


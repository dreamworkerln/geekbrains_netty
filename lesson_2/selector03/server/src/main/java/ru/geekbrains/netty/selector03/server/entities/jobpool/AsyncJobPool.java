package ru.geekbrains.netty.selector03.server.entities.jobpool;

import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Supplier;


// =========================================================================================


/**
 * Async job pool dynamic size.
 * <br>
 * @param <T>
 */
public class AsyncJobPool<T> extends BaseJobPool {

    // On job done handler
    private Consumer<T> callback;



    /**
     * Pool of worker threads
     * @param callback handler onComplete event
     */
    public AsyncJobPool(Consumer<T> callback) {

        final CustomizableThreadFactory threadFactory = new CustomizableThreadFactory();

        threadFactory.setDaemon(true);

        threadFactory.setThreadNamePrefix("AsyncPool-");

        threadPool = (ThreadPoolExecutor)Executors.newCachedThreadPool(threadFactory);
        this.callback = callback;
    }

    /**
     * Add job to execute
     * <br>
     * @param job Supplier
     */
    public void add(Supplier<T> job) {

        CompletableFuture.supplyAsync(job, threadPool)
                .handle(this::handle)
                .thenAccept(this::callback);
    }


    // ----------------------------------------------------------------------------------------------------


    /**
     * WorkProcessable.work error handler
     */
    // Never should be called
    private T handle(T result, Throwable e) {

        //System.out.println(e.getMessage());
        return result;
    }


    /**
     * On job done
     * @param msg callback message
     */
    private void callback(T msg) {

        //System.out.println("callback");

        callback.accept(msg);
    }



    // ----------------------------------------------------------------------------------------------------
}




//    // Link to T.Constructor
//    private Function<Object,T> creator;
//


//    /**
//     * Pool of worker threads
//     * @param callback handler onComplete event
//     * @param creator functional interface - pointer to T constructor that accept one parameter - Object id
//     *                and return T
//     */
//    public AsyncJobPool(Consumer<T> callback,
//                        Function<Object,T> creator) {
//
//        threadPool = (ThreadPoolExecutor)Executors.newCachedThreadPool();
//        this.callback = callback;
//        this.creator = creator;
//    }






//    /**
//     * WorkProcessable.work error handler
//     */
//    private T handle(T result, Throwable e) {
//
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
//
//        return result;
//    }







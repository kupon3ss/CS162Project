package kvstore;

import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class ThreadPool {

    /* Array of threads in the pool */
    private Thread threads[];

    /* the queue of jobs waiting to be executed by the worker threads in the pool */
    private LinkedList<Runnable> jobQueue;

    /* lock & condition for the jobQueue */
    private ReentrantLock jobsLock;
    private Condition jobsCondition;


    /**
     * Constructs a Threadpool with a starting number of threads.
     *
     * @param size number of threads in the thread pool
     */
    public ThreadPool(int size) {
        threads = new Thread[size];
        // implement me
        jobQueue = new LinkedList<Runnable>();
        jobsLock = new ReentrantLock();
        jobsCondition = jobsLock.newCondition();
        for (int i = 0; i < size; i++) {
            threads[i] = new WorkerThread(this);
            threads[i].start();
        }
    }

    /**
     * Add a job to the queue of jobs that have to be executed. As soon as a
     * thread is free, the thread will retrieve a job from this queue if
     * if one exists and start processing it.
     *
     * @param r job that has to be executed
     * (does not throw) InterruptedException if thread is interrupted while in blocked
     *         state. Your implementation may or may not actually throw this.
     */
    public void addJob(Runnable r) {//} throws InterruptedException {
        // implement me
        jobsLock.lock();
        jobQueue.addLast(r);
        jobsCondition.signal();
        jobsLock.unlock();
    }

    /**
     * Block until a job is present in the queue and retrieve the job
     *
     * @return A runnable task that has to be executed
     * (does not throw) InterruptedException if thread is interrupted while in blocked
     *         state. Your implementation may or may not actually throw this.
     */
    private Runnable getJob() {//throws InterruptedException {
        // implement me
        jobsLock.lock();
        while (jobQueue.isEmpty()) {
            jobsCondition.awaitUninterruptibly();
        }
        Runnable job = jobQueue.removeFirst();
        jobsLock.unlock();
        return job;
    }

    /**
     * A thread in the thread pool.
     */
    private class WorkerThread extends Thread {

        private ThreadPool threadPool;

        /**
         * Constructs a thread for a particular ThreadPool.
         *
         * @param pool the ThreadPool containing this thread
         */
        public WorkerThread(ThreadPool pool) {
            threadPool = pool;
        }

        /**
         * Scan for and execute tasks.
         */
        @Override
        public void run() {
            // implement me
            Thread job;
            while (true) {
                // get a job (unless blocked) and execute it
                job = new Thread(threadPool.getJob()); //change to below line to run ThreadPoolTest
                // job = (Thread) threadPool.getJob();
                job.start();
                // wait for job to finish before fetching a new one
                try {
                    job.join();
                } catch (InterruptedException e) {}
                // if the thread was interrupted, just get the next one
            }
        }

    }

    /*public void startThreads() {
        for (Thread worker : threads)
            worker.start();
    }
    public void interruptThreads() {
        for (Thread worker : threads)
            worker.interrupt();
    }*/

}

package kvstore;

import static org.junit.Assert.*;
import org.junit.*;

import java.util.concurrent.locks.ReentrantLock;

/**
 * The testing class for ThreadPool
 */
public class ThreadPoolTest {

    ThreadPool pool;

    Thread thread1, thread2, thread3, thread4;

    int thread1put;
    int thread2put;
    String thread3put;

    @Before
    public void setUp() throws Exception {
        pool = new ThreadPool(4);
        thread1 = new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 7000; i++);
                thread1put = 9999;
            }
        });
        thread2 = new Thread(new Runnable() {
            public void run() {
                thread2put = 78;
                for (int i = 0; i < 4000; i++);
            }
        });
        thread3 = new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 8000; i++);
                thread3put = "somestring";
            }
        });
        thread4 = new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 234; i++);
            }
        });
    }

    @Test
    public void firstTest() throws InterruptedException {
        ThreadPool pool = new ThreadPool(4);
        pool.addJob(thread1);
        pool.addJob(thread2);
        pool.addJob(thread3);
        pool.addJob(thread4);
        
        while(thread1.getState() != Thread.State.NEW);
        thread1.join();
        assertEquals(9999, thread1put);
        thread2.join();
        assertEquals(78, thread2put);
        thread3.join();
        assertEquals("somestring", thread3put);
        thread4.join();
    }

    @Test
    public void secondTest() {
        Thread thread5 = new Thread(thread1);
        Thread thread6 = new Thread(thread2);
        Thread thread7 = new Thread(thread3);
        Thread thread8 = new Thread(thread4);
        Thread thread9 = new Thread(thread5);
        Thread thread10 = new Thread(thread6);
        Thread thread11 = new Thread(thread7);
        Thread thread12 = new Thread(thread8);
        Thread thread13 = new Thread(thread9);
        Thread thread14 = new Thread(thread10);
        //do something with the threads
    }

    @Test
    public void thirdTest() throws InterruptedException {
        pool = new ThreadPool(20);
        int i;
        final ReentrantLock lock = new ReentrantLock();
        Thread thread0 = new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 5000; i++) {
                    lock.lock();
                    i++;
                    lock.unlock();
                }
            }
        });
        Thread thread;
        for (int j = 0; j < 200; j++) {
            thread = new Thread(thread0);
            pool.addJob(thread);
            thread.join();//not sure if this would work..., might need to start them all, but then the reference is lost
        }
    }

}

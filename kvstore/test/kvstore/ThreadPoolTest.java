package kvstore;

import static org.junit.Assert.*;
import org.junit.*;

import java.util.LinkedList;
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
        
        while(thread1.getState().equals(Thread.State.NEW));
        thread1.join();
        assertEquals(9999, thread1put);
        while(thread2.getState().equals(Thread.State.NEW));
        thread2.join();
        assertEquals(78, thread2put);
        while(thread3.getState().equals(Thread.State.NEW));
        thread3.join();
        assertEquals("somestring", thread3put);
        while(thread4.getState().equals(Thread.State.NEW));
        thread4.join();
    }

    @Test
    public void secondTest() throws InterruptedException {
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
        pool.addJob(thread1); pool.addJob(thread2);
        pool.addJob(thread3); pool.addJob(thread4);
        pool.addJob(thread5); pool.addJob(thread6);
        pool.addJob(thread7); pool.addJob(thread8);
        pool.addJob(thread9); pool.addJob(thread10);
        pool.addJob(thread11); pool.addJob(thread12);
        pool.addJob(thread13); pool.addJob(thread14);
        while (thread1.getState().equals(Thread.State.NEW));
        thread1.join();
        while (thread2.getState().equals(Thread.State.NEW));
        thread2.join();
        while (thread3.getState().equals(Thread.State.NEW));
        thread3.join();
        while (thread4.getState().equals(Thread.State.NEW));
        thread4.join();
        while (thread5.getState().equals(Thread.State.NEW));
        thread5.join();
        while (thread6.getState().equals(Thread.State.NEW));
        thread6.join();
        while (thread7.getState().equals(Thread.State.NEW));
        thread7.join();
        while (thread8.getState().equals(Thread.State.NEW));
        thread8.join();
        while (thread9.getState().equals(Thread.State.NEW));
        thread9.join();
        while (thread10.getState().equals(Thread.State.NEW));
        thread10.join();
        while (thread11.getState().equals(Thread.State.NEW));
        thread11.join();
        while (thread12.getState().equals(Thread.State.NEW));
        thread12.join();
        while (thread13.getState().equals(Thread.State.NEW));
        thread13.join();
        while (thread14.getState().equals(Thread.State.NEW));
        thread14.join();
        //do something with the threads
        assertTrue(true);
    }

    int count = 0;

    @Test
    public void thirdTest() throws InterruptedException {
        pool = new ThreadPool(20);
        final ReentrantLock lock = new ReentrantLock();
        Thread thread0 = new Thread(new Runnable() {
            public void run() {
                for (int k = 0; k < 5000; k++) {
                    lock.lock();
                    count++;
                    lock.unlock();
                }
            }
        });
        LinkedList<Thread> threads = new LinkedList<Thread>();
        Thread thread;
        for (int j = 0; j < 200; j++) {
            thread = new Thread(thread0);
            threads.add(thread);
            pool.addJob(thread);
        }
        for (Thread t : threads) {
            while (t.getState().equals(Thread.State.NEW)) ; //wait
            lock.lock();
            System.out.println("current count (to 1,000,000): " + count);
            lock.unlock();
            t.join();
        }
        lock.lock();
        assertEquals(1000000, count);
        lock.unlock();
    }

}

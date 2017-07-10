package com.emc.roger.curator.lock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by cher5 on 2017/7/10.
 */
public class Reader {

    public static void main(String[] args) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182,localhost:2183", retryPolicy);
        client.start();
        try {
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        reading(new InterProcessReadWriteLock(client, "/read_write_lock"));

    }

    public static void reading(InterProcessReadWriteLock readWriteLock) {
        try {
            readWriteLock.readLock().acquire();
            try {
                for(int i = 0; i < 10; i++) {
                    System.out.println("i am reading");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        System.out.println("i am interrupted from reading");
                        break;
                    }
                }
            } finally {
                readWriteLock.readLock().release();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

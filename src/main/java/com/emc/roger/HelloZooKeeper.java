package com.emc.roger;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by cher5 on 2017/3/10.
 */
public class HelloZooKeeper {
    final static private Logger logger = LoggerFactory.getLogger(HelloZooKeeper.class);

    public static void main(String[] args) {


        // znode path
        String path = "/MyFirstZnode"; // Assign path to znode

        // data in byte array
        byte[] data = "My first zookeeper app".getBytes(); // Declare data

        try {
            HelloZooKeeper helloZooKeeper = new HelloZooKeeper();
            ZooKeeper zk = helloZooKeeper.connect("localhost:2181,localhost:2182,localhost:2183");
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); // Create the data to the specified path
            zk.close();
        } catch (Exception e) {
            System.out.println(e.getMessage()); //Catch error message
        }

    }




    // Method to connect zookeeper ensemble.
    public ZooKeeper connect(String host) throws IOException,InterruptedException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        ZooKeeper zoo = new ZooKeeper(host,5000, we -> {
            if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });

        connectedSignal.await();
        return zoo;
    }




}

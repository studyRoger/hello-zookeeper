package com.emc.roger.barrier;

import com.emc.roger.HelloZooKeeper;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;


/**
 * Created by cher5 on 2017/7/6.
 */
public class Worker {
    public static final CountDownLatch latch = new CountDownLatch(1);
    public static void main(String[] args) throws IOException, InterruptedException {
        Worker worker = new Worker();
        worker.init("localhost:2181,localhost:2182,localhost:2183");
        latch.await();
    }


    private volatile ZooKeeper zk;
    private volatile boolean amIFirer = false;


    private final long workId;

    public Worker() {
        workId = System.currentTimeMillis();
        System.out.println("i am " + workId);
    }

    public void init(String hostport) throws IOException {
        zk = new ZooKeeper(hostport,3000, we -> {
            if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("i am connected to zookeeper");
            }
        });
        try {
            if(zk.exists("/barrier", false) != null) {
                if(zk.exists("/barrier/fire", Worker.this::onFire) != null) {
                    working();
                } else {
                    zk.create("/barrier/worker", String.valueOf(workId).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                }
            } else {
                System.out.println("no barrier");
                working();
            }
        } catch (KeeperException | InterruptedException  e) {
            e.printStackTrace();
            System.out.println("i should not do work because of exception from zookeeper when i am looking for /barrier");
            latch.countDown();
        }
    }

    public void working() {
        System.out.println("i am working");
        System.out.println("i am done");
        latch.countDown();
    }

    public void onFire(WatchedEvent watchedEvent) {
        if(watchedEvent.getType().equals(Watcher.Event.EventType.NodeCreated)) {
            working();
        }
    }
}

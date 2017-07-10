package com.emc.roger.barrier;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by cher5 on 2017/7/7.
 */
public class MyBarrier {
    public static final int N = 3;

    public static void main(String[] args) throws IOException {
        MyBarrier barrier = new MyBarrier();
        barrier.init("localhost:2181,localhost:2182,localhost:2183");

    }

    private ZooKeeper zk;

    public void init(String hostport) throws IOException {
        zk = new ZooKeeper(hostport,50000, we -> {
            if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("i am connected to zookeeper");
            }
        });
        try {

            if (zk.exists("/barrier", false) == null) {
                System.out.println("barrier is not set up");
                zk.create("/barrier", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("barrier is created");
            } else if(zk.exists("/barrier/fire", false) != null){
                zk.delete("/barrier/fire", -1);
                System.out.println("barrier is reset");
            }
            zk.getChildren("/barrier", MyBarrier.this::onChildNodeChanged);

        } catch (KeeperException | InterruptedException  e) {
            e.printStackTrace();
            System.out.println("i should not do work because of exception from zookeeper when i am looking for /barrier");

        }
        try {
            synchronized (this) {
                wait();

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } finally {
            try {
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void onChildNodeChanged(WatchedEvent we) {
        if(we.getType().equals(Watcher.Event.EventType.NodeChildrenChanged)) {
            List<String> children = null;
            try {
                children = zk.getChildren("/barrier", false);
                if(children.size() >= N) {

                    zk.create("/barrier/fire", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println("fire!!!");
                    zk.delete("/barrier/fire", -1);
                    System.out.println("barrier is reset");
                } else {
                    System.out.println("--------------");
                    children.forEach(System.out::println);
                }
                zk.getChildren("/barrier", MyBarrier.this::onChildNodeChanged);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}

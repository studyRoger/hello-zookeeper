package com.emc.roger.lock;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;

/**
 * Created by cher5 on 2017/7/7.
 */
public class Task {

    final private long id;
    private String me;
    private ZooKeeper zk;
    private static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Task task = new Task();
        task.init("localhost:2181,localhost:2182,localhost:2183");
        task.doWorkWithLock();

        latch.await();
    }

    public Task() {
        id = System.currentTimeMillis();
        System.out.println("i am " + id);
    }

    public void init(String hostport) throws IOException, KeeperException, InterruptedException {
        zk = new ZooKeeper(hostport, 3000, we -> {
            if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("i am connected to zookeeper");
            }
        });
        me = zk.create(
                "/_locknode_/lock-",
                String.valueOf(id).getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(me);
    }

    public boolean tryLock() throws KeeperException, InterruptedException {
        List<String> nodes = zk.getChildren("/_locknode_", false);
        nodes.sort(String::compareTo);
        if(me.endsWith(nodes.get(0))) {
            System.out.println("i get lock");
            return true;
        } else {
            System.out.println("i didn't get lock, waiting in queue...");
            zk.exists("/_locknode_/" + nodes.get(0), Task.this::onHeadRemoved);
            return false;
        }

    }

    public void onHeadRemoved(WatchedEvent we) {
        if(we.getType().equals(NodeDeleted)) {
            doWorkWithLock();
        }
    }

    /*public void unLock(String lock) throws KeeperException, InterruptedException {
        zk.delete(lock, -1);
    }*/

    public void doWorkWithLock() {
        try {
            if(tryLock()) {
                doWork();
                zk.close();
                latch.countDown();
            }

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void doWork() {
        System.out.println("i am working");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("i am done");
    }
}

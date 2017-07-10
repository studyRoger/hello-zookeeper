package com.emc.roger.election;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;

/**
 * Created by cher5 on 2017/7/7.
 */
public class Candidate {
    private static ExecutorService executorService = Executors.newFixedThreadPool(1);
    private ZooKeeper zk;
    private String me;
    private volatile CompletableFuture currentTask;
    private volatile boolean done = false;
    private static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        Candidate candidate = new Candidate();
        candidate.init("localhost:2181,localhost:2182,localhost:2183");
        candidate.elect();
        latch.await();
        executorService.shutdown();
    }

    public void init(String hostport) throws IOException, KeeperException, InterruptedException {
        zk = new ZooKeeper(hostport, 3000, we -> {
            if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("i am connected to zookeeper");
            }
        });
        me = zk.create(
                "/_election_/candidate-",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(me);

    }

    public CompletableFuture elect() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/_election_", false);
        children.sort(String::compareTo);
        if(me.endsWith(children.get(0))) {
            done = true;
            if(currentTask != null) {
                currentTask.cancel(true);
            }
            System.out.println("i am the leader");
            return CompletableFuture.runAsync(this::workAsLeader, executorService);
        } else {
            int myIdx = children.indexOf(me.replace("/_election_/", ""));
            String previous = "/_election_/" + children.get(myIdx - 1);
            zk.exists(previous, this::onLeaderRetired);
            System.out.println("i am a worker");
            return CompletableFuture.runAsync(this::workAsWorker, executorService);
        }
    }

    public void onLeaderRetired(WatchedEvent we) {
        if(we.getType().equals(NodeDeleted)) {
            try {
                elect();
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public void workAsLeader() {
        for(int i = 0; i < 5; i++) {
            System.out.println("I am leading...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
        System.out.println("I am retired");
        latch.countDown();
    }

    public void workAsWorker() {
        while(!done) {
            System.out.println("I am working...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("I am done as a worker");
    }
}

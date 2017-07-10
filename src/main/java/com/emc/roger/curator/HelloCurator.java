package com.emc.roger.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by cher5 on 2017/7/10.
 */
public class HelloCurator {
    public static void main(String[] args) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182,localhost:2183", retryPolicy);
        client.start();

        try {
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            e.printStackTrace();
            client.close();
            return;
        }

        System.out.println(client.getNamespace());
        try {
            client.getChildren().forPath("/").forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("cannot get children nodes of /");
        }

        client.close();
    }
}

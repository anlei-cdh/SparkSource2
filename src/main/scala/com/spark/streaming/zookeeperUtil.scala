package com.spark.streaming

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * Created by dell1 on 2017/6/22.
  */
class zookeeperUtil private {
    private def getclient(zookeeperlist: String): CuratorFramework = {
        val client = CuratorFrameworkFactory.newClient(zookeeperlist, new ExponentialBackoffRetry(1000, 3));
        return client
    }
}

object zookeeperUtil {
    val zookeeperutil = new zookeeperUtil

    def apply() = {
        println("-------apply--------")
    }

    def getclient(zookeeperlist: String): CuratorFramework = {
        zookeeperutil.getclient(zookeeperlist)
    }
}



package com.spark.util

import com.java.util.KafkaUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

import scala.reflect.ClassTag

/**
  * Created by dell1 on 2017/6/21.
  */
class kafkaManagerbycurator(val kafkaParams: Map[String, String]) extends Serializable {
    def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag](
                                                                                                                      ssc: StreamingContext, kafkaParams: Map[String, String], topics: Set[String]): InputDStream[(K, V)] = {
        val groupid = kafkaParams.get("group.id").get
        val brokerlist = kafkaParams.get("metadata.broker.list").get
        val zookeeperlist = kafkaParams.get("zookeeper.connect").get
        val client = zookeeperUtil.getclient(zookeeperlist)
        // 在zookeeper上读取offsets前先根据实际情况更新offsets
        setOrUpdateOffsets(zookeeperlist, brokerlist, topics, groupid)
        val message = {
            var offsets: Map[TopicAndPartition, Long] = Map()
            client.start()
            for (topic <- topics) {
                val child = client.getChildren.forPath("/sparkconsumer/" + groupid + "/" + topic)
                import scala.collection.JavaConversions._
                for (cpartition <- child) {
                    val value = new String(client.getData.forPath("/sparkconsumer/" + groupid + "/" + topic + "/" + cpartition), "utf-8").toLong
                    val tp: TopicAndPartition = TopicAndPartition(topic, cpartition.toInt)
                    offsets += (tp -> value)
                }
            }
            KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
                ssc, kafkaParams, offsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
        }
        message
    }

    def updateZKOffsets(rdd: RDD[(String, String)]): Unit = {
        val groupId = kafkaParams.get("group.id").get
        val zookeeperlist = kafkaParams.get("zookeeper.connect").get
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val client = zookeeperUtil.getclient(zookeeperlist)
        client.start()
        for (offsets <- offsetsList) {
            //val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
            val PATH = "/sparkconsumer/" + groupId + "/" + offsets.topic + "/" + offsets.partition;
            //if(client.checkExists().forPath(PATH)==null){
            //  client.create().creatingParentsIfNeeded().forPath(PATH,offsets.untilOffset.toString.getBytes)
            //}else {
            client.setData().forPath(PATH, offsets.untilOffset.toString.getBytes)
            //}
        }
        client.close()
    }

    private def setOrUpdateOffsets(zookeeperlist: String, brokerlist: String, topics: Set[String], groupId: String): Unit = {
        val client = zookeeperUtil.getclient(zookeeperlist)
        client.start()
        topics.foreach(topic => {
            val kafkatopic = KafkaUtil.getInstance().getEarlistOffsetByTopic(brokerlist, topic)
            val partition = kafkatopic.getOffsetList()
            import scala.collection.JavaConversions._
            for (k <- partition.keySet()) {
                val PATH = "/sparkconsumer/" + groupId + "/" + topic + "/" + k
                if (client.checkExists().forPath(PATH) == null) {
                    // client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(Ids.OPEN_ACL_UNSAFE).forPath(PATH,partition.get(k).toString.getBytes());
                    client.create().creatingParentsIfNeeded().forPath(PATH, partition.get(k).toString.getBytes())
                    //client.setData().forPath(PATH,partition.get(k).toString.getBytes())
                } else {
                    val offset = new String(client.getData.forPath(PATH), "utf-8")
                    if (offset.toLong < partition.get(k).toLong) {
                        client.setData().forPath(PATH, partition.get(k).toString.getBytes)
                    }
                }
            }
        })
        client.close()
    }
}

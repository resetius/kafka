/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.util.Random
import java.util.Properties
import kafka.api.{TopicMetadata, PartitionMetadata}
import kafka.cluster.Broker
import kafka.log.LogConfig
import kafka.utils.{Logging, ZkUtils, Json}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import scala.collection._
import mutable.ListBuffer
import scala.collection.mutable
import kafka.common._
import scala.Predef._
import collection.Map
import scala.Some
import collection.Set

object AdminUtils extends Logging {
  val rand = new Random
  val TopicConfigChangeZnodePrefix = "config_change_"

  /**
   * There are 2 goals of replica assignment:
   * 1. Spread the replicas evenly among brokers.
   * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
   *
   * To achieve this goal, we:
   * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
   * 2. Assign the remaining replicas of each partition with an increasing shift.
   *
   * Here is an example of assigning
   * broker-0  broker-1  broker-2  broker-3  broker-4
   * p0        p1        p2        p3        p4       (1st replica)
   * p5        p6        p7        p8        p9       (1st replica)
   * p4        p0        p1        p2        p3       (2nd replica)
   * p8        p9        p5        p6        p7       (2nd replica)
   * p3        p4        p0        p1        p2       (3nd replica)
   * p7        p8        p9        p5        p6       (3nd replica)
   *
   * To create rack aware assignment, this API will first create an rack interlaced broker list. For example,
   * from this brokerID -> rack mapping:
   *
   * 0 -> "rack1", 1 -> "rack3", 2 -> "rack3", 3 -> "rack2", 4 -> "rack2", 5 -> "rack1"
   *
   * The rack interlaced list will be
   *
   * 0, 3, 1, 5, 4, 2
   *
   * Then an easy round-robin assignment can be applied. Assume 6 partitions with replication factor of 3, the assignment
   * will be
   *
   * 0 -> 0,3,1
   * 1 -> 3,1,5
   * 2 -> 1,5,4
   * 3 -> 5,4,2
   * 4 -> 4,2,0
   * 5 -> 2,0,3
   *
   * Once it has completed the first round-robin, if there are more partitions to assign, the algorithm will start to
   * have a shift for the followers. This is to ensure we will not always get the same set of sequence.
   * In this case, if there is another partition to assign (partition #6), the assignment will be
   *
   * 6 -> 0,4,2 (instead of repeating 0,3,1 as partition 0)
   *
   * The rack aware assignment always chooses leader of the partition using round robin on the rack interlaced broker list.
   * When choosing followers, it will be biased towards brokers on racks that do not have
   * any replica assignment, until every rack has a replica. Then the follower assignment will go back to round-robin on
   * the broker list.
   *
   * As the result, if the number of replicas is equal to or greater than the number of racks, it will ensure that
   * each rack will get at least one replica. Otherwise, each rack will get
   * at most one replica. In the perfect situation where number of replica is the same as number of racks and each rack
   * has the same number of brokers, it guarantees that replica distribution is even across brokers and racks.
   *
   * @param rackInfo Map from broker ID to its rack (zone). If empty, no rack aware
   *                 assignment will be used.
   * @throws AdminOperationException If rack information is supplied but is incomplete, of if
   *                                 there is not possible to assign each replica to a unique rack
   *
   */
  def assignReplicasToBrokers(brokerList: Seq[Int],
                              nPartitions: Int,
                              replicationFactor: Int,
                              fixedStartIndex: Int = -1,
                              startPartitionId: Int = -1,
                              rackInfo: Map[Int, String] = Map())
  : Map[Int, Seq[Int]] = {
    val subRackInfo = rackInfo.filterKeys(brokerList.contains(_))
    if (rackInfo.size > 0 && subRackInfo.size != brokerList.size) {
      throw new AdminOperationException("Incomplete broker-rack mapping supplied for broker list " + brokerList)
    }
    val numRacks = subRackInfo.values.toSet.size
    val useRackAware = numRacks > 0
    val arrangedBrokerList = if (useRackAware) interlaceBrokersByRack(subRackInfo) else brokerList
    if (nPartitions <= 0)
      throw new AdminOperationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdminOperationException("replication factor must be larger than 0")
    if (replicationFactor > arrangedBrokerList.size)
      throw new AdminOperationException("replication factor: " + replicationFactor +
        " larger than available brokers: " + arrangedBrokerList.size)
    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(arrangedBrokerList.size)
    var currentPartitionId = if (startPartitionId >= 0) startPartitionId else 0

    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(arrangedBrokerList.size)
    for (i <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % arrangedBrokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % arrangedBrokerList.size
      val leader = arrangedBrokerList(firstReplicaIndex)
      val replicaList = if (!useRackAware)
        getReplicaList(arrangedBrokerList, leader, replicationFactor, firstReplicaIndex, nextReplicaShift)
      else
        getReplicaListWithRackAwareAssignment(arrangedBrokerList, leader, replicationFactor, numRacks,
          firstReplicaIndex, nextReplicaShift, rackInfo)
      ret.put(currentPartitionId, replicaList)
      currentPartitionId = currentPartitionId + 1
    }
    ret.toMap
  }

  private def getReplicaList(brokerList: Seq[Int],
                             leader: Int,
                             replicationFactor: Int,
                             firstReplicaIndex: Int,
                             nextReplicaShift: Int): List[Int] = {
    var replicaList = List(leader)
    for (j <- 0 until replicationFactor - 1)
      replicaList ::= brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
    replicaList.reverse
  }

  private def getReplicaListWithRackAwareAssignment(brokerList: Seq[Int],
                                                    leader: Int,
                                                    replicationFactor: Int,
                                                    numRacks: Int,
                                                    firstReplicaIndex: Int,
                                                    nextReplicaShift: Int,
                                                    rackInfo: Map[Int, String]): List[Int] = {
    var replicaList = List(leader)
    val racksWithReplicas: mutable.Set[String] = mutable.Set(rackInfo(leader))
    var k = 0
    for (j <- 0 until replicationFactor - 1) {
      var done = false
      while (!done) {
        val broker = brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift * numRacks, k, brokerList.size))
        val rack = rackInfo(broker)
        // unless every rack has a replica, try to find the broker on the rack without any replica assigned
        if (!racksWithReplicas.contains(rack) || racksWithReplicas.size == numRacks) {
          replicaList ::= broker
          racksWithReplicas += rack
          done = true
        }
        k = k + 1
      }
    }
    replicaList.reverse
  }

  /**
   * Given broker and rack information, returns a list of brokers interlaced by the rack. Assume
   * this is the rack and its brokers:
   *
   * rack1: 0, 1, 2
   * rack2: 3, 4, 5
   * rack3: 6, 7, 8
   *
   * This API would return the list of 0, 3, 6, 1, 4, 7, 2, 5, 8
   *
   * This is essential to make sure the assignReplicasToBrokers API can use such list
   * and assign replicas to brokers in a simple round-robin fashion, but still keep
   * the even distribution of leader count and replica count on each broker, while
   * making sure replicas are distributed to all racks.
   */
  private[admin] def interlaceBrokersByRack(brokerRackMap: Map[Int, String]): Seq[Int] = {
    val reverseMap = getInverseMap(brokerRackMap)
    val brokerListsByRack = reverseMap.map { case(rack, list) => (rack, reverseMap(rack).toIterator) }
    val racks = brokerListsByRack.keys.toArray.sorted
    var result: List[Int] = List()
    var rackIndex = 0
    while (result.size < brokerRackMap.size) {
      val rackIterator = brokerListsByRack(racks(rackIndex))
      if (rackIterator.hasNext) {
        result ::= rackIterator.next()
      }
      rackIndex = (rackIndex + 1) % racks.size
    }
    result.reverse
  }

  private[admin] def getInverseMap(brokerRackMap: Map[Int, String]): Map[String, List[Int]] = {
    brokerRackMap.toList.map { case(k, v) => (v, k) }
      .groupBy(_._1)
      .map { case(k, v) => (k, v.map(_._2))}
  }


  /**
  * Add partitions to existing topic with optional replica assignment
  *
  * @param zkClient Zookeeper client
  * @param topic Topic for adding partitions to
  * @param numPartitions Number of partitions to be set
  * @param replicaAssignmentStr Manual replica assignment
  * @param checkBrokerAvailable Ignore checking if assigned replica broker is available. Only used for testing
  * @param config Pre-existing properties that should be preserved
  * @param rackInfo Map from broker ID to its rack (zone). If empty, no rack aware
  *                 assignment will be used.
  */
  def addPartitions(zkClient: ZkClient,
                    topic: String,
                    numPartitions: Int = 1,
                    replicaAssignmentStr: String = "",
                    checkBrokerAvailable: Boolean = true,
                    config: Properties = new Properties,
                    rackInfo: Map[Int, String] = Map.empty) {
    val existingPartitionsReplicaList = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic))
    if (existingPartitionsReplicaList.size == 0)
      throw new AdminOperationException("The topic %s does not exist".format(topic))

    val existingReplicaList = existingPartitionsReplicaList.head._2
    val partitionsToAdd = numPartitions - existingPartitionsReplicaList.size
    if (partitionsToAdd <= 0)
      throw new AdminOperationException("The number of partitions for a topic can only be increased")

    // create the new partition replication list
    val brokerList = ZkUtils.getSortedBrokerList(zkClient)
    val newPartitionReplicaList = if (replicaAssignmentStr == null || replicaAssignmentStr == "")
      AdminUtils.assignReplicasToBrokers(brokerList, partitionsToAdd, existingReplicaList.size, existingReplicaList.head,
        existingPartitionsReplicaList.size, rackInfo = rackInfo)
    else
      getManualReplicaAssignment(replicaAssignmentStr, brokerList.toSet, existingPartitionsReplicaList.size, checkBrokerAvailable)

    // check if manual assignment has the right replication factor
    val unmatchedRepFactorList = newPartitionReplicaList.values.filter(p => (p.size != existingReplicaList.size))
    if (unmatchedRepFactorList.size != 0)
      throw new AdminOperationException("The replication factor in manual replication assignment " +
        " is not equal to the existing replication factor for the topic " + existingReplicaList.size)

    info("Add partition list for %s is %s".format(topic, newPartitionReplicaList))
    val partitionReplicaList = existingPartitionsReplicaList.map(p => p._1.partition -> p._2)
    // add the new list
    partitionReplicaList ++= newPartitionReplicaList
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaList, config, true)
  }

  def getManualReplicaAssignment(replicaAssignmentList: String, availableBrokerList: Set[Int], startPartitionId: Int, checkBrokerAvailable: Boolean = true): Map[Int, List[Int]] = {
    var partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    var partitionId = startPartitionId
    partitionList = partitionList.takeRight(partitionList.size - partitionId)
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      if (brokerList.size <= 0)
        throw new AdminOperationException("replication factor must be larger than 0")
      if (brokerList.size != brokerList.toSet.size)
        throw new AdminOperationException("duplicate brokers in replica assignment: " + brokerList)
      if (checkBrokerAvailable && !brokerList.toSet.subsetOf(availableBrokerList))
        throw new AdminOperationException("some specified brokers not available. specified brokers: " + brokerList.toString +
          "available broker:" + availableBrokerList.toString)
      ret.put(partitionId, brokerList.toList)
      if (ret(partitionId).size != ret(startPartitionId).size)
        throw new AdminOperationException("partition " + i + " has different replication factor: " + brokerList)
      partitionId = partitionId + 1
    }
    ret.toMap
  }
  
  def deleteTopic(zkClient: ZkClient, topic: String) {
    ZkUtils.createPersistentPath(zkClient, ZkUtils.getDeleteTopicPath(topic))
  }
  
  def topicExists(zkClient: ZkClient, topic: String): Boolean = 
    zkClient.exists(ZkUtils.getTopicPath(topic))
    
  def createTopic(zkClient: ZkClient,
                  topic: String,
                  partitions: Int, 
                  replicationFactor: Int, 
                  topicConfig: Properties = new Properties,
                  rackInfo: Map[Int, String] = Map.empty) {
    val brokerList = ZkUtils.getSortedBrokerList(zkClient)
    val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, partitions, replicationFactor, rackInfo = rackInfo)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, replicaAssignment, topicConfig)
  }
                  
  def createOrUpdateTopicPartitionAssignmentPathInZK(zkClient: ZkClient,
                                                     topic: String,
                                                     partitionReplicaAssignment: Map[Int, Seq[Int]],
                                                     config: Properties = new Properties,
                                                     update: Boolean = false) {
    // validate arguments
    Topic.validate(topic)
    LogConfig.validate(config)
    require(partitionReplicaAssignment.values.map(_.size).toSet.size == 1, "All partitions should have the same number of replicas.")

    val topicPath = ZkUtils.getTopicPath(topic)
    if(!update && zkClient.exists(topicPath))
      throw new TopicExistsException("Topic \"%s\" already exists.".format(topic))
    partitionReplicaAssignment.values.foreach(reps => require(reps.size == reps.toSet.size, "Duplicate replica assignment found: "  + partitionReplicaAssignment))
    
    // write out the config if there is any, this isn't transactional with the partition assignments
    writeTopicConfig(zkClient, topic, config)
    
    // create the partition assignment
    writeTopicPartitionAssignment(zkClient, topic, partitionReplicaAssignment, update)
  }
  
  private def writeTopicPartitionAssignment(zkClient: ZkClient, topic: String, replicaAssignment: Map[Int, Seq[Int]], update: Boolean) {
    try {
      val zkPath = ZkUtils.getTopicPath(topic)
      val jsonPartitionData = ZkUtils.replicaAssignmentZkData(replicaAssignment.map(e => (e._1.toString -> e._2)))

      if (!update) {
        info("Topic creation " + jsonPartitionData.toString)
        ZkUtils.createPersistentPath(zkClient, zkPath, jsonPartitionData)
      } else {
        info("Topic update " + jsonPartitionData.toString)
        ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionData)
      }
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
    } catch {
      case e: ZkNodeExistsException => throw new TopicExistsException("topic %s already exists".format(topic))
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }
  
  /**
   * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
   * @param zkClient: The ZkClient handle used to write the new config to zookeeper
   * @param topic: The topic for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeTopicConfig(zkClient: ZkClient, topic: String, configs: Properties) {
    if(!topicExists(zkClient, topic))
      throw new AdminOperationException("Topic \"%s\" does not exist.".format(topic))

    // remove the topic overrides
    LogConfig.validate(configs)

    // write the new config--may not exist if there were previously no overrides
    writeTopicConfig(zkClient, topic, configs)
    
    // create the change notification
    zkClient.createPersistentSequential(ZkUtils.TopicConfigChangesPath + "/" + TopicConfigChangeZnodePrefix, Json.encode(topic))
  }
  
  /**
   * Write out the topic config to zk, if there is any
   */
  private def writeTopicConfig(zkClient: ZkClient, topic: String, config: Properties) {
    val configMap: mutable.Map[String, String] = {
      import JavaConversions._
      config
    }
    val map = Map("version" -> 1, "config" -> configMap)
    ZkUtils.updatePersistentPath(zkClient, ZkUtils.getTopicConfigPath(topic), Json.encode(map))
  }
  
  /**
   * Read the topic config (if any) from zk
   */
  def fetchTopicConfig(zkClient: ZkClient, topic: String): Properties = {
    val str: String = zkClient.readData(ZkUtils.getTopicConfigPath(topic), true)
    val props = new Properties()
    if(str != null) {
      Json.parseFull(str) match {
        case None => // there are no config overrides
        case Some(map: Map[String, _]) => 
          require(map("version") == 1)
          map.get("config") match {
            case Some(config: Map[String, String]) =>
              for((k,v) <- config)
                props.setProperty(k, v)
            case _ => throw new IllegalArgumentException("Invalid topic config: " + str)
          }

        case o => throw new IllegalArgumentException("Unexpected value in config: "  + str)
      }
    }
    props
  }

  def fetchAllTopicConfigs(zkClient: ZkClient): Map[String, Properties] =
    ZkUtils.getAllTopics(zkClient).map(topic => (topic, fetchTopicConfig(zkClient, topic))).toMap

  def fetchTopicMetadataFromZk(topic: String, zkClient: ZkClient): TopicMetadata =
    fetchTopicMetadataFromZk(topic, zkClient, new mutable.HashMap[Int, Broker])

  def fetchTopicMetadataFromZk(topics: Set[String], zkClient: ZkClient): Set[TopicMetadata] = {
    val cachedBrokerInfo = new mutable.HashMap[Int, Broker]()
    topics.map(topic => fetchTopicMetadataFromZk(topic, zkClient, cachedBrokerInfo))
  }

  private def fetchTopicMetadataFromZk(topic: String, zkClient: ZkClient, cachedBrokerInfo: mutable.HashMap[Int, Broker]): TopicMetadata = {
    if(ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic))) {
      val topicPartitionAssignment = ZkUtils.getPartitionAssignmentForTopics(zkClient, List(topic)).get(topic).get
      val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)
      val partitionMetadata = sortedPartitions.map { partitionMap =>
        val partition = partitionMap._1
        val replicas = partitionMap._2
        val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partition)
        val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
        debug("replicas = " + replicas + ", in sync replicas = " + inSyncReplicas + ", leader = " + leader)

        var leaderInfo: Option[Broker] = None
        var replicaInfo: Seq[Broker] = Nil
        var isrInfo: Seq[Broker] = Nil
        try {
          leaderInfo = leader match {
            case Some(l) =>
              try {
                Some(getBrokerInfoFromCache(zkClient, cachedBrokerInfo, List(l)).head)
              } catch {
                case e: Throwable => throw new LeaderNotAvailableException("Leader not available for partition [%s,%d]".format(topic, partition), e)
              }
            case None => throw new LeaderNotAvailableException("No leader exists for partition " + partition)
          }
          try {
            replicaInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, replicas.map(id => id.toInt))
            isrInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, inSyncReplicas)
          } catch {
            case e: Throwable => throw new ReplicaNotAvailableException(e)
          }
          if(replicaInfo.size < replicas.size)
            throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
              replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
          if(isrInfo.size < inSyncReplicas.size)
            throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
              inSyncReplicas.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
          new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError)
        } catch {
          case e: Throwable =>
            debug("Error while fetching metadata for partition [%s,%d]".format(topic, partition), e)
            new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo,
              ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
        }
      }
      new TopicMetadata(topic, partitionMetadata)
    } else {
      // topic doesn't exist, send appropriate error code
      new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.UnknownTopicOrPartitionCode)
    }
  }

  private def getBrokerInfoFromCache(zkClient: ZkClient,
                                     cachedBrokerInfo: scala.collection.mutable.Map[Int, Broker],
                                     brokerIds: Seq[Int]): Seq[Broker] = {
    var failedBrokerIds: ListBuffer[Int] = new ListBuffer()
    val brokerMetadata = brokerIds.map { id =>
      val optionalBrokerInfo = cachedBrokerInfo.get(id)
      optionalBrokerInfo match {
        case Some(brokerInfo) => Some(brokerInfo) // return broker info from the cache
        case None => // fetch it from zookeeper
          ZkUtils.getBrokerInfo(zkClient, id) match {
            case Some(brokerInfo) =>
              cachedBrokerInfo += (id -> brokerInfo)
              Some(brokerInfo)
            case None =>
              failedBrokerIds += id
              None
          }
      }
    }
    brokerMetadata.filter(_.isDefined).map(_.get)
  }

  private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }
}

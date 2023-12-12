# Octopus - Distributed Tiered Storage for Cluster Computing

This repository contains the source code for the **OctopusFS** distributed tiered file system as well as the **Trident** scheduling and prefetching framework.

## OctopusFS
OctopusFS is a novel distributed file system that is aware of storage media (e.g., memory, SSDs, HDDs, NAS) with different capacities and performance characteristics, built by extending HDFS 2.7.0. The OctopusFS system offers a variety of pluggable policies for automating data management across both the storage tiers and cluster nodes. A new data placement policy employs multi-objective optimization techniques for making intelligent data management decisions based on the requirements of fault tolerance, data and load balancing, and throughput maximization. Moreover, machine learning is employed for tracking and predicting file access patterns, which are then used by data movement policies to decide when and which data to move up or down the storage tiers for increasing system performance. This approach uses XGBoost to dynamically refine the models with new file accesses and improve the prediction performance of the models. At the same time, the storage media are explicitly exposed to users and applications, allowing them to choose the distribution, placement, and movement of replicas in the cluster based on their own performance and fault tolerance requirements.

## Trident
Trident is a scheduling and prefetching framework that is designed to make task assignment, resource scheduling, and prefetching decisions based on both locality and storage tier information. Trident formulates task scheduling as a minimum cost maximum matching problem in a bipartite graph and utilizes two novel pruning algorithms for bounding the size of the graph, while still guaranteeing optimality. In addition, Trident extends YARN's resource request model and proposes a new storage-tier-aware resource scheduling algorithm. Finally, Trident includes a cost-based data prefetching approach that coordinates with the schedulers for optimizing prefetching operations. The Trident Task Scheduler is implemented in both Hadoop MapReduce 2.7.0 and Spark 2.4.6, while the Trident Prefetcher is implemented only in Hadoop MapReduce 2.7.0.

## Usage Instructions
### Prerequisites
- Java 1.8.0
- Apache Ant 1.10
- Maven 3.3
- Python 3.4
- ProtocolBuffer 2.5.0
- CMake 3.6.0

Detailed instructions on how to install the prerequisites and build the source code can be found under the folder [instructions](instructions).

### Configuration
You can configure and run OctopusFS and Trident by configuring and running the modified HDFS, Hadoop, and Spark found in this repository. Detailed instructions can be found under the folder [instructions](instructions). In addition, sample configurations for pseudo-distributed and real cluster deployments can be found under the folder [sample-confs](sample-confs).

Below, we highlight some of the key configurations to use in `hdfs-site.xml` for enabling the unique features offered by OctopusFS.
You can specify the storage device type of each local directory for storing data on the DataNodes with the following configuration:
```
<property>
  <name>dfs.datanode.data.dir</name>
  <value>[DISK]file:///path/to/datadisk1,[DISK]file:///path/to/datadisk2,[SSD]file:///path/to/datassd,[MEMORY]file:///path/to/folder/for/mem/metadata</value>
</property>
```
To use the new OctopusFS policies, you need to specify the following configurations in `hdfs-site.xml`:
```
<property>
  <name>dfs.default.storage.tier.policy</name>
  <value>DYNAMIC</value>
</property>

<property>
  <name>dfs.block.replicator.classname</name>
  <value>org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDynamic</value>
</property>

<property>
  <name>dfs.datanode.fsdataset.volume.choosing.policy</name>
  <value>org.apache.hadoop.hdfs.server.datanode.fsdataset.DynamicVolumeChoosingPolicy</value>
</property>

<property>
  <name>dfs.block.retrieval.policy.classname</name>
  <value>org.apache.hadoop.hdfs.server.blockmanagement.BlockRetrievalPolicyTiering</value>
</property>

<property>
  <name>dfs.replication.downgrade.policy.classname</name>
  <value>org.apache.hadoop.hdfs.server.namenode.replicationmanagement.ReplicationDowngradePolicyDefault</value>
</property>

<property>
  <name>dfs.replication.upgrade.policy.classname</name>
  <value>org.apache.hadoop.hdfs.server.namenode.replicationmanagement.ReplicationUpgradePolicyDefault</value>
</property>
```

Next, we highlight some of the key configurations to use in `yarn-site.xml` for enabling the unique features offered by Trident Resource and Task Schedulers in Hadoop.
```
  <property>
    <name>yarn.resourcemanager.scheduler.class</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.hungarian.HungarianScheduler</value>
    <description>The class to use as the resource scheduler.</description>
  </property>

  <property>
    <name>yarn.scheduler.hungarian.enable-tiers</name>
    <value>true</value>
  </property>

  <property>
    <name>yarn.app.mapreduce.am.task.assignment.solver.class</name>
    <value>org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator$AMAssignmentSolverHungarian</value>
  </property>

  <property>
    <name>yarn.app.mapreduce.am.task.assignment.solver.enable.tiers</name>
    <value>true</value>
  </property>
```
Next, we highlight some of the key configurations to use in `mapred-site.xml` for enabling the unique features offered by Trident Prefetcher in Hadoop.
```
<property>
  <name>mapreduce.octopus.prefetcher.strategy</name>
  <value>2</value>
  <description>0=no prefetching; 1=always prefetching; 2=selective prefetching</description>
</property>

<property>
  <name>mapreduce.octopus.prefetcher.max.wait.ms</name>
  <value>0</value>
</property>
```
Finally, we highlight some of the key configurations to use in `spark-defaults.conf` for enabling the unique features offered by Trident Scheduler in Spark.
```
spark.yarn.allocator.use.hungarian   true
spark.yarn.allocator.enable.tiers    true

spark.scheduler.use.hungarian        true
spark.scheduler.enable.tiers         true

spark.locality.wait     3000
spark.hungarian.delay   1000
```

## Publications
- H. Herodotou and E. Kakoulli. [Cost-based Data Prefetching and Scheduling in Big Data Platforms over Tiered Storage Systems](https://dl.acm.org/doi/10.1145/3625389). ACM Transactions on Database Systems (TODS), Vol. 48, No. 4, Article 11, pp. 1-40, November 2023.
- H. Herodotou and E. Kakoulli. [Trident: Task Scheduling over Tiered Storage Systems in Big Data Platforms](https://dicl.cut.ac.cy/images/dicl/pubs/2021-PVLDB-Trident.pdf). Proc. of VLDB Endowment (PVLDB), Vol. 14, No. 9, pp. 1570-1582, May 2021. 
- H. Herodotou and E. Kakoulli. [Automating Distributed Tiered Storage Management in Cluster Computing](https://dicl.cut.ac.cy/images/dicl/pubs/2019-VLDB-OctopusPlusPlus.pdf). Proc. of VLDB Endowment (PVLDB), Vol. 13, No. 1, pp. 43-56, September 2019.
- H. Herodotou. [AutoCache: Employing Machine Learning to Automate Caching in Distributed File Systems](https://dicl.cut.ac.cy/images/dicl/pubs/2019-SMDB-AutoCache.pdf). In Proc. of the 35th IEEE Intl. Conf. on Data Engineering Workshops (ICDEW '19), pp. 133-139, April 2019.
- E. Kakoulli, N. D. Karmiris, and H. Herodotou. [OctopusFS in Action: Tiered Storage Management for Data Intensive Computing](https://dicl.cut.ac.cy/images/dicl/pubs/2018-VLDB-OctopusFS-Demo.pdf). Demo, Proc. of VLDB Endowment (PVLDB), Vol. 11, No. 12, pp. 1914-1917, August 2018.
- E. Kakoulli and H. Herodotou. [OctopusFS: A Distributed File System with Tiered Storage Management](https://dicl.cut.ac.cy/images/dicl/pubs/2017-SIGMOD-OctopusFS.pdf). In Proc. of the ACM Intl. Conf. on Management of Data (SIGMOD '17), May 2017.

## Funding
- AWS Cloud Credits for Research Grant, Amazon Web Services, July 2018
- Starting Grant, Cyprus University of Technology, May 2015 - Apr 2017

## Contact
- Herodotos Herodotou, Cyprus University of Technology, [https://dicl.cut.ac.cy/](https://dicl.cut.ac.cy/)
- Elena Kakoulli, Neapolis University Pafos

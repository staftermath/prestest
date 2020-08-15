# prestest: A Test Framework for Presto connector on Hive

In many applications, when we develop libraries or ETL processes, we intend to deploy on an environment with Hive and 
presto connections. Usually presto is connect to hive. It could be difficult to develop a unit test framework to 
test the functionality such as creating table using hive and then access them through presto. This library 
provides a module to help build such test framework for your code. 

## System Requirement
- tested under linux.
- docker needs to be installed. You can download it from 
<a href="https://www.docker.com/products/docker-desktop" target="_blank">here</a>

## Restriction
This library depends heavily on <a href="https://hub.docker.com/r/bde2020/hive/" target="_blank">docker-hive</a>. 
It means the following container names should not be taken in your system:

- docker-hive_hive-metastore_1
- docker-hive_datanode_1
- docker-hive_namenode_1
- docker-hive_hive-server_1
- docker-hive_presto-coordinator_1
- docker-hive_hive-metastore-postgresql_1

The library contains some functions that may
 
- start all containers above.
- stop all containers above.
- reset all containers above to factory.

The test helpers are implemented under pytest. However, you may uses the modules and develop in any other test 
frameworks.

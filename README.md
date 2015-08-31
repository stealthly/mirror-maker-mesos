Mirror Maker on Mesos
======================

Mirror Maker is a Kafka utility for keeping a replica of an existing Kafka cluster.
Mirror Maker on Mesos (hereinafter MMM) is a Mesos framework wrapping MM tool.  

Build
-------------
Assuming $WORKING_DIR is your working directory.

To build the fatjar:
    
    # cd $WORKING_DIR && git clone https://github.com/stealthly/mirror-maker-mesos.git
    # (Checkout the needed branch or stay on master)
    # cd mirror-maker-mesos && ./gradlew jar

Environment Configuration
--------------------------

Before running MMM, set the location of libmesos:

    # export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so

If the host running scheduler has several IP addresses you may also need to

    # export LIBPROCESS_IP=<IP_ACCESSIBLE_FROM_MASTER>
    
You will also need java in you PATH.

Scheduler Configuration
----------------------

To start the scheduler you will need the following configuration files: 

1) scheduler configuration (default - `mmm.properties`)

2) kafka consumer configuration file (default - `kafka-consumer.properties`)
 
3) kafka producer configuration file (default - `kafka-producer.properties`)

4) log4j-configuration file - shared for scheduler and executors (default - `mmm-log4j.properties`) 

Default configuration files can be found in 'config' directory under this project.
 
With kafka consumer and producer configuration file you can control replication durability. The default settings are intended for
at least-once processing semantics.

To see all available options for scheduler use `help` command:

```
Command: scheduler [options] <config>
Starts the scheduler of the mirror-maker-mesos framework.
  <config>
        Path to scheduler configuration file.
  -c-c <value> | --consumer-config <value>
        Path to the Kafka consumer configuration file. Required.
  -p-c <value> | --producer-config <value>
        Path to Kafka producer configuration file. Required.
```

Run the scheduler
-----------------

Assuming you are trying to start `mmm.sh` script from the $RUNNING_DIR you need to ensure:
 
 1) You have `$RUNNING_DIR/config` directory in with log4j configuration file there
  
 2) You have `$RUNNING_DIR/dist` directory with the respective mmm jar archive and log4j configuration - schedulers make
 these files available for the executors to download while those are being started
 
 
 ```
 # cd $RUNNING_DIR 
 # cp -r $WORKING_DIR/config $RUNNING_DIR
 # mkdir dist 
 # cp $WORKING_DIR/config/mmm-log4j.properties dist/
 # cp $WORKING_DIR/mmm-0.1-SNAPSHOT.jar dist/
 ```
   
After these instructions you are ready start the scheduler:
   
   ```
   # ./mmm.sh scheduler config/mmm.properties \
    --consumer-config config/kafka-consumser.properties \
     --producer-config config/kafka-producer.properties
   ```
   
Quick start
-----------

Now you can add some Mirror Maker processes as mesos tasks.

In order not to specify REST API url each time you may set the environment variable:

```
# export MMM_API="http://192.168.3.5:7000/api"
```

You can check the host:port value from the `mmm.properties` file, setting `mmm.api`.

To start one MM task with default resources:

```
# ./mmm.sh add 1
Added 1 tasks successfully.
Cluster:
  server:
    id: 1
    state: Added
    task configuration:
      cpu: 0.2
      mem: 256.0

```

You now have a cluster with 1 server that is added with default params, it will start once mesos have resources to satisfy default constraints.

To check the status:

```
# ./mmm.sh status
Cluster:
  server:
    id: 1
    state: Added
    task configuration:
      cpu: 0.2
      mem: 256.0
```

To delete the task (will stop and remove the server):

```
# ./mmm.sh delete 1
Delete servers request (ids=1) was sent successfully.
Cluster:
No servers were added. Add servers with 'add' command.
```

Troubleshooting
==================

Goto Mesos UI (<mesos-master-host>:5050) check the framework (default mirror-maker-mesos) and its tasks.

Logs in task's sandbox: `stdout`, `stderr` and `logs/scheduler.log0`, `logs/executor.log` may be helpful.
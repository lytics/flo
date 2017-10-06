SPEC
====

### Server

The server registers the protobuf messages used in flo, as well
as registers the actors used in flo, these are the internal
messages and actors, not user defined. Then it calls the grid
server and waits.

### Leader Actor

The leader actor monitors worker actors. When they die or new
peers enter the namespace it starts worker actors. Worker actors
are started at some fixed ratio of peer:workers. The leader
also performs the coordination of deciding where sources of
data run, in other words which worker will own a particular
source of data.

### Worker Actor

The worker actor monitors etcd for graph instance entries. When
one is created it start a mapred process for that graph instance.
The worker does all its user defined work via the mapred process,
it does nothing itself in regards to user defined work.

### MapRed Process

The mapred process is the mapper and reducer of data. It uses the
user defined graph to know how to map and reduce data.

### What Happens to Start a Graph?

The flo client is used to write an entry into etcd. The entry
contains the wanted state of "running", along with the graph
type and graph name. Using these information each worker will
see the entry, and try to put a mapred process into the running
state parameterized with the graph type and graph name.

### What Happens to Stop a Graph?

The flo client is used to write an entry into etcd. The entry
contains the wanted state of "stopping", along with the graph
type and graph name. Using these information each worker will
try to stop the respective mapred process, if it is running.

### Event Time Tracking

Each mapred process keeps track of where in event-time it currently
resides. The worker-actor will aggregate all these watermarks from
the mapred processes running inside it, and send it to the leader.
The leader will then aggregate the aggregations, and send it back
to each worker-actor, which then can pass the watermarks back to
each mapred process so that it is aware of the global event-time
state. This makes tracking event-time telemetry O(n).

### Etcd Entry

What should the path be?

The path should be something like:

    flo.<namespace>.graph.<type>.<name>

What are some properties of the entry to make it work?

The actual entry would need to include the wanted state of the graph,
such as RUNNING, STOPPING, or TERMINATING. It should also include a
configuration blob that can be passed to user defined code.

```
{
    "type": <graphType>,
    "name": <graphName>,
    "wanted": <state>,
    "config": <config>,
}
```

### Coordinating Data Sources
What are the possible ways data can be consumed?

#### Example Sources
1.
The source is something like PubSub, for example:
    g := graph.New()
    g.From(pubsub.New("sub-1", "sub-2", "sub-3"))
If each MapRed process started a PubSub client, and read
each subscription the system would be reading a disjoint
set of messages, which is the intended behavior. Basically
with a message queue like PubSub, PubSub itself handles
the partitioning of data between clients.

2.
The source is somthing like a database table scan, for example:
    g := graph.New()
    g.From(sql.Scan("users"))
If each MapRed process started an SQL DB client, and scanned
the table there would be a problem, every entry would be read
multiple times because the system, in this case a DB, does not
have any internal notion of partitioning scanning clients, that
logic must be implemented by the scanners.

3.
The source is a file on each system, which has the same name
on each system, for example:
    g := graph.New()
    g.From(filedata.New("/tmp/data"))
If each MapRed process started to read the file on its respective
system, there might be a problem. If there is one MapRed process
per system then the behavior is correct, otherwise there is duplicate
reading of each entry in the file.

#### State, Worker and Process Layout
1.
How are workers started? Is there one worker per peer? Or are there
possible multiple workers on each peer?

    There should be one worker per peer.

2.
How many mapred processes are there per worker per graph? Is there
one mapred process per worker per graph? Or are there multiple
mapred processes per worker per graph?

    There should be one mapred process per worker per graph.

3.
Is autoscaling a requirement or not?

    Autoscaling should be supported if the datastore used
    for state is non-local, like BigTable or Cassandra, and
    not-supported if it is local, like BoltDB or BadgerDB.

4.
The binary will be deployed into Kubernetes, because it depends on
Kubernetes for recovery of crashed pods.

5.
The process could be deployed as a Deployment, StatefulSet or 
independently and then controlled via a ReplicationControler.
It could even be a hybrid, there the reducer runs in a StatefulSet
and the mappers run in a Deployment.

#### Consequences of Design Choices
The consequences of the design choices above are at least some
of the following:

1.
The MapRed process will need sub-processes to perform the reading
of sources and mapping their data. It does not make sense to have
the reading and mapping fixed to the number of pods. Since the
worker, and in turn the MapRed process, are fixed to the pod count.

2.
The MapRed process will need sub-processes to perform the reducing
since the reducing actually represents a "ring" or "disjoint key
space" that must be independent of the pod count.

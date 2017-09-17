flo
===

A dataflow like library for Go.

### Introduction

Graphs are generic processing pipelines, when they are
defined they are defined with only a "type" name, later
when they are started they receive further parameters.
So a graph is like a "template", a "class", or a "type"
depending on the perspective you are coming from.

	g := graph.New(<graphType>)
	g.From(data.Sources)
	g.Transform()
	g.GroupBy()
	g.Window()
	g.Trigger()
	g.Into(data.Sinks)

Graphs get their data from sources, which need to be setup.
The call to setup will get as arguments the type of graph,
the name of this part particular graph instance, and the config
that this instance used. Using these arguments the source
has full knowledge about who the data is for, and can make
appropriate adjustments if needed. The same goes for the sink.

	type Sources interface {
		Setup(<graphType>, <graphName>, <graphConfig>) []Source
	}

	type Sinks interface {
		Setup(<graphType>, <graphName>, <graphConfig>) []Sink
	}

The process is initialized by registering the messages used
for processing, which must be Protobuf messages. The process
must also define all the graph types that it supports. Since
flo is for Go, the process, which is just a running binary,
is how functionality is packaged and shipped. Consequently
ever process participating in running the flo graphs must
start with the same registrations of messages, and definitions
of graphs.

	flo.Register(<message>)
	flo.Define(<grapType>, <graph>)

The process starts doing work by creating on operator, and
calling the blocking method Serve. An instance of a defined
graph is started by calling StartGraph on an operator.
This will create entries in etcd. All processes that are
serving will monitor etcd, see the new entry, and start
that graph in-process. The parameters to StartGraph are all
strings. The graph configuration is opaque, and is written
to etcd as-is. The graph configuration exists for the sake
of the source and sink, which may or may-not need it.

	op, err := flo.New(<etcd>)
	op.Serve(<listener>)
	op.StartGraph(<graphType>, <graphName>, <graphConfig>)
	op.StopGraph(<graphType>, <graphName>)
	op.TerminateGraph(<graphType>, <graphName>)
	op.Stop()

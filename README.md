flo
===

A dataflow like library for Go.

### Introduction

Graphs are pipelines, you can think of them as a template
or class, a graph can be started multiple times with
a different name and configuration. Below each ...
represents a function that the user of flo supplies.

	g := graph.New()
	g.From(...)
	g.Transform(...)
	g.Group(...)
	g.Window(...)
	g.Trigger(...)
	g.Into(...)

Graphs get their data from sources, and write their outputs into
sinks.

	type Sources interface {
		Setup(<graphType>, <graphName>, <graphConfig>) []Source
	}

	type Sinks interface {
		Setup(<graphType>, <graphName>, <graphConfig>) []Sink
	}

The process is initialized by registering the messages used
for processing, which must be Protobuf messages. And registering
the graphs used for processing.

	flo.RegisterMsg(<message>)
	flo.RegisterGraph(<grapType>, <graph>)

Since flo is just a library, processing is done the Go way: by
building a static binary which just uses the flo server to
start and stop graphs.

	server, ... := flo.NewServer(...)
	lis, ... := net.Listen(...)
	go server.Serve(lis)

Once the server is running, a graph is started using the flo
client from anywhere. The client writes an entry to etcd for
each graph instance. The server, watching etcd events, responds
to the client.

	client, ... := flo.NewClient(...)
	client.StartGraph(graphType, graphName, conf)

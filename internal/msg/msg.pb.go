// Code generated by protoc-gen-go. DO NOT EDIT.
// source: msg.proto

/*
Package msg is a generated protocol buffer package.

It is generated from these files:
	msg.proto

It has these top-level messages:
	Event
	Progress
	Term
*/
package msg

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Event struct {
	Key       string `protobuf:"bytes,1,opt,name=Key" json:"Key,omitempty"`
	Data      []byte `protobuf:"bytes,2,opt,name=Data,proto3" json:"Data,omitempty"`
	DataType  string `protobuf:"bytes,3,opt,name=DataType" json:"DataType,omitempty"`
	SpanStart int64  `protobuf:"varint,4,opt,name=SpanStart" json:"SpanStart,omitempty"`
	SpanEnd   int64  `protobuf:"varint,5,opt,name=SpanEnd" json:"SpanEnd,omitempty"`
	Graph     string `protobuf:"bytes,6,opt,name=Graph" json:"Graph,omitempty"`
}

func (m *Event) Reset()                    { *m = Event{} }
func (m *Event) String() string            { return proto.CompactTextString(m) }
func (*Event) ProtoMessage()               {}
func (*Event) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Event) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *Event) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Event) GetDataType() string {
	if m != nil {
		return m.DataType
	}
	return ""
}

func (m *Event) GetSpanStart() int64 {
	if m != nil {
		return m.SpanStart
	}
	return 0
}

func (m *Event) GetSpanEnd() int64 {
	if m != nil {
		return m.SpanEnd
	}
	return 0
}

func (m *Event) GetGraph() string {
	if m != nil {
		return m.Graph
	}
	return ""
}

type Progress struct {
	Peer         string   `protobuf:"bytes,1,opt,name=Peer" json:"Peer,omitempty"`
	Graph        string   `protobuf:"bytes,2,opt,name=Graph" json:"Graph,omitempty"`
	Source       []string `protobuf:"bytes,3,rep,name=Source" json:"Source,omitempty"`
	Done         bool     `protobuf:"varint,4,opt,name=Done" json:"Done,omitempty"`
	MinEventTime int64    `protobuf:"varint,5,opt,name=MinEventTime" json:"MinEventTime,omitempty"`
}

func (m *Progress) Reset()                    { *m = Progress{} }
func (m *Progress) String() string            { return proto.CompactTextString(m) }
func (*Progress) ProtoMessage()               {}
func (*Progress) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *Progress) GetPeer() string {
	if m != nil {
		return m.Peer
	}
	return ""
}

func (m *Progress) GetGraph() string {
	if m != nil {
		return m.Graph
	}
	return ""
}

func (m *Progress) GetSource() []string {
	if m != nil {
		return m.Source
	}
	return nil
}

func (m *Progress) GetDone() bool {
	if m != nil {
		return m.Done
	}
	return false
}

func (m *Progress) GetMinEventTime() int64 {
	if m != nil {
		return m.MinEventTime
	}
	return 0
}

type Term struct {
	Peers []string `protobuf:"bytes,1,rep,name=Peers" json:"Peers,omitempty"`
}

func (m *Term) Reset()                    { *m = Term{} }
func (m *Term) String() string            { return proto.CompactTextString(m) }
func (*Term) ProtoMessage()               {}
func (*Term) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *Term) GetPeers() []string {
	if m != nil {
		return m.Peers
	}
	return nil
}

func init() {
	proto.RegisterType((*Event)(nil), "msg.Event")
	proto.RegisterType((*Progress)(nil), "msg.Progress")
	proto.RegisterType((*Term)(nil), "msg.Term")
}

func init() { proto.RegisterFile("msg.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 240 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x54, 0x90, 0xc1, 0x4a, 0xc4, 0x30,
	0x10, 0x86, 0xc9, 0xa6, 0xad, 0xed, 0xb0, 0x07, 0x19, 0x44, 0x82, 0xec, 0xa1, 0xf4, 0xd4, 0x93,
	0x17, 0x5f, 0xc1, 0xc5, 0x83, 0x08, 0x4b, 0xda, 0x17, 0x88, 0x3a, 0xd4, 0x3d, 0x34, 0x09, 0x49,
	0x14, 0xf6, 0xee, 0x3b, 0xf8, 0xba, 0x92, 0xd9, 0x6a, 0xf5, 0x94, 0xff, 0x9b, 0x64, 0x66, 0xfe,
	0x3f, 0xd0, 0xcc, 0x71, 0xba, 0xf5, 0xc1, 0x25, 0x87, 0x72, 0x8e, 0x53, 0xf7, 0x25, 0xa0, 0xdc,
	0x7f, 0x90, 0x4d, 0x78, 0x09, 0xf2, 0x91, 0x4e, 0x4a, 0xb4, 0xa2, 0x6f, 0x74, 0x96, 0x88, 0x50,
	0xdc, 0x9b, 0x64, 0xd4, 0xa6, 0x15, 0xfd, 0x56, 0xb3, 0xc6, 0x1b, 0xa8, 0xf3, 0x39, 0x9e, 0x3c,
	0x29, 0xc9, 0x4f, 0x7f, 0x19, 0x77, 0xd0, 0x0c, 0xde, 0xd8, 0x21, 0x99, 0x90, 0x54, 0xd1, 0x8a,
	0x5e, 0xea, 0xb5, 0x80, 0x0a, 0x2e, 0x32, 0xec, 0xed, 0xab, 0x2a, 0xf9, 0xee, 0x07, 0xf1, 0x0a,
	0xca, 0x87, 0x60, 0xfc, 0x9b, 0xaa, 0x78, 0xe0, 0x19, 0xba, 0x4f, 0x01, 0xf5, 0x21, 0xb8, 0x29,
	0x50, 0x8c, 0xd9, 0xca, 0x81, 0x28, 0x2c, 0xee, 0x58, 0xaf, 0x6d, 0x9b, 0x3f, 0x6d, 0x78, 0x0d,
	0xd5, 0xe0, 0xde, 0xc3, 0x4b, 0xb6, 0x27, 0xfb, 0x46, 0x2f, 0xc4, 0x61, 0x9c, 0x25, 0xf6, 0x55,
	0x6b, 0xd6, 0xd8, 0xc1, 0xf6, 0xe9, 0x68, 0x39, 0xfe, 0x78, 0x9c, 0x69, 0xf1, 0xf5, 0xaf, 0xd6,
	0xed, 0xa0, 0x18, 0x29, 0xcc, 0x79, 0x5b, 0xde, 0x1a, 0x95, 0xe0, 0xb1, 0x67, 0x78, 0xae, 0xf8,
	0x2b, 0xef, 0xbe, 0x03, 0x00, 0x00, 0xff, 0xff, 0x2c, 0xfc, 0x00, 0x26, 0x57, 0x01, 0x00, 0x00,
}

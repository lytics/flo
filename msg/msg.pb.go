// Code generated by protoc-gen-go.
// source: msg.proto
// DO NOT EDIT!

/*
Package msg is a generated protocol buffer package.

It is generated from these files:
	msg.proto

It has these top-level messages:
	Unit
	Keyed
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

type Unit struct {
	ID   []byte `protobuf:"bytes,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Data []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	From string `protobuf:"bytes,3,opt,name=from" json:"from,omitempty"`
}

func (m *Unit) Reset()                    { *m = Unit{} }
func (m *Unit) String() string            { return proto.CompactTextString(m) }
func (*Unit) ProtoMessage()               {}
func (*Unit) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Unit) GetID() []byte {
	if m != nil {
		return m.ID
	}
	return nil
}

func (m *Unit) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Unit) GetFrom() string {
	if m != nil {
		return m.From
	}
	return ""
}

type Keyed struct {
	Key      string `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Data     []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	DataType string `protobuf:"bytes,3,opt,name=dataType" json:"dataType,omitempty"`
	Unix     int64  `protobuf:"varint,4,opt,name=unix" json:"unix,omitempty"`
	Graph    string `protobuf:"bytes,6,opt,name=graph" json:"graph,omitempty"`
}

func (m *Keyed) Reset()                    { *m = Keyed{} }
func (m *Keyed) String() string            { return proto.CompactTextString(m) }
func (*Keyed) ProtoMessage()               {}
func (*Keyed) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *Keyed) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *Keyed) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Keyed) GetDataType() string {
	if m != nil {
		return m.DataType
	}
	return ""
}

func (m *Keyed) GetUnix() int64 {
	if m != nil {
		return m.Unix
	}
	return 0
}

func (m *Keyed) GetGraph() string {
	if m != nil {
		return m.Graph
	}
	return ""
}

func init() {
	proto.RegisterType((*Unit)(nil), "msg.Unit")
	proto.RegisterType((*Keyed)(nil), "msg.Keyed")
}

func init() { proto.RegisterFile("msg.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 164 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xe2, 0xe2, 0xcc, 0x2d, 0x4e, 0xd7,
	0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0xce, 0x2d, 0x4e, 0x57, 0xb2, 0xe3, 0x62, 0x09, 0xcd,
	0xcb, 0x2c, 0x11, 0xe2, 0xe3, 0x62, 0xf2, 0x74, 0x91, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x09, 0x62,
	0xf2, 0x74, 0x11, 0x12, 0xe2, 0x62, 0x49, 0x49, 0x2c, 0x49, 0x94, 0x60, 0x02, 0x8b, 0x80, 0xd9,
	0x20, 0xb1, 0xb4, 0xa2, 0xfc, 0x5c, 0x09, 0x66, 0x05, 0x46, 0x0d, 0xce, 0x20, 0x30, 0x5b, 0xa9,
	0x98, 0x8b, 0xd5, 0x3b, 0xb5, 0x32, 0x35, 0x45, 0x48, 0x80, 0x8b, 0x39, 0x3b, 0xb5, 0x12, 0x6c,
	0x02, 0x67, 0x10, 0x88, 0x89, 0xd5, 0x08, 0x29, 0x2e, 0x0e, 0x10, 0x1d, 0x52, 0x59, 0x90, 0x0a,
	0x35, 0x06, 0xce, 0x07, 0xa9, 0x2f, 0xcd, 0xcb, 0xac, 0x90, 0x60, 0x51, 0x60, 0xd4, 0x60, 0x0e,
	0x02, 0xb3, 0x85, 0x44, 0xb8, 0x58, 0xd3, 0x8b, 0x12, 0x0b, 0x32, 0x24, 0xd8, 0xc0, 0x8a, 0x21,
	0x9c, 0x24, 0x36, 0xb0, 0x07, 0x8c, 0x01, 0x01, 0x00, 0x00, 0xff, 0xff, 0xcd, 0xcb, 0x34, 0xee,
	0xcd, 0x00, 0x00, 0x00,
}
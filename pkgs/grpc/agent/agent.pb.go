// 使用的语法版本

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v4.22.3
// source: pkgs/grpc/agent/agent.proto

package agent

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type FileDataPacketType int32

const (
	FileDataPacketType_Data FileDataPacketType = 0
	FileDataPacketType_EOF  FileDataPacketType = 1
)

// Enum value maps for FileDataPacketType.
var (
	FileDataPacketType_name = map[int32]string{
		0: "Data",
		1: "EOF",
	}
	FileDataPacketType_value = map[string]int32{
		"Data": 0,
		"EOF":  1,
	}
)

func (x FileDataPacketType) Enum() *FileDataPacketType {
	p := new(FileDataPacketType)
	*p = x
	return p
}

func (x FileDataPacketType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (FileDataPacketType) Descriptor() protoreflect.EnumDescriptor {
	return file_pkgs_grpc_agent_agent_proto_enumTypes[0].Descriptor()
}

func (FileDataPacketType) Type() protoreflect.EnumType {
	return &file_pkgs_grpc_agent_agent_proto_enumTypes[0]
}

func (x FileDataPacketType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use FileDataPacketType.Descriptor instead.
func (FileDataPacketType) EnumDescriptor() ([]byte, []int) {
	return file_pkgs_grpc_agent_agent_proto_rawDescGZIP(), []int{0}
}

// 文件数据。注意：只在Type为Data的时候，Data字段才能有数据
type FileDataPacket struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Type FileDataPacketType `protobuf:"varint,1,opt,name=Type,proto3,enum=FileDataPacketType" json:"Type,omitempty"`
	Data []byte             `protobuf:"bytes,2,opt,name=Data,proto3" json:"Data,omitempty"`
}

func (x *FileDataPacket) Reset() {
	*x = FileDataPacket{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkgs_grpc_agent_agent_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FileDataPacket) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FileDataPacket) ProtoMessage() {}

func (x *FileDataPacket) ProtoReflect() protoreflect.Message {
	mi := &file_pkgs_grpc_agent_agent_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FileDataPacket.ProtoReflect.Descriptor instead.
func (*FileDataPacket) Descriptor() ([]byte, []int) {
	return file_pkgs_grpc_agent_agent_proto_rawDescGZIP(), []int{0}
}

func (x *FileDataPacket) GetType() FileDataPacketType {
	if x != nil {
		return x.Type
	}
	return FileDataPacketType_Data
}

func (x *FileDataPacket) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type SendIPFSFileResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FileHash string `protobuf:"bytes,1,opt,name=FileHash,proto3" json:"FileHash,omitempty"`
}

func (x *SendIPFSFileResp) Reset() {
	*x = SendIPFSFileResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkgs_grpc_agent_agent_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SendIPFSFileResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SendIPFSFileResp) ProtoMessage() {}

func (x *SendIPFSFileResp) ProtoReflect() protoreflect.Message {
	mi := &file_pkgs_grpc_agent_agent_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SendIPFSFileResp.ProtoReflect.Descriptor instead.
func (*SendIPFSFileResp) Descriptor() ([]byte, []int) {
	return file_pkgs_grpc_agent_agent_proto_rawDescGZIP(), []int{1}
}

func (x *SendIPFSFileResp) GetFileHash() string {
	if x != nil {
		return x.FileHash
	}
	return ""
}

type GetIPFSFileReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FileHash string `protobuf:"bytes,1,opt,name=FileHash,proto3" json:"FileHash,omitempty"`
}

func (x *GetIPFSFileReq) Reset() {
	*x = GetIPFSFileReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkgs_grpc_agent_agent_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetIPFSFileReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetIPFSFileReq) ProtoMessage() {}

func (x *GetIPFSFileReq) ProtoReflect() protoreflect.Message {
	mi := &file_pkgs_grpc_agent_agent_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetIPFSFileReq.ProtoReflect.Descriptor instead.
func (*GetIPFSFileReq) Descriptor() ([]byte, []int) {
	return file_pkgs_grpc_agent_agent_proto_rawDescGZIP(), []int{2}
}

func (x *GetIPFSFileReq) GetFileHash() string {
	if x != nil {
		return x.FileHash
	}
	return ""
}

var File_pkgs_grpc_agent_agent_proto protoreflect.FileDescriptor

var file_pkgs_grpc_agent_agent_proto_rawDesc = []byte{
	0x0a, 0x1b, 0x70, 0x6b, 0x67, 0x73, 0x2f, 0x67, 0x72, 0x70, 0x63, 0x2f, 0x61, 0x67, 0x65, 0x6e,
	0x74, 0x2f, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x4d, 0x0a,
	0x0e, 0x46, 0x69, 0x6c, 0x65, 0x44, 0x61, 0x74, 0x61, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x12,
	0x27, 0x0a, 0x04, 0x54, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x13, 0x2e,
	0x46, 0x69, 0x6c, 0x65, 0x44, 0x61, 0x74, 0x61, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x54, 0x79,
	0x70, 0x65, 0x52, 0x04, 0x54, 0x79, 0x70, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x44, 0x61, 0x74, 0x61,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x44, 0x61, 0x74, 0x61, 0x22, 0x2e, 0x0a, 0x10,
	0x53, 0x65, 0x6e, 0x64, 0x49, 0x50, 0x46, 0x53, 0x46, 0x69, 0x6c, 0x65, 0x52, 0x65, 0x73, 0x70,
	0x12, 0x1a, 0x0a, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x48, 0x61, 0x73, 0x68, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x48, 0x61, 0x73, 0x68, 0x22, 0x2c, 0x0a, 0x0e,
	0x47, 0x65, 0x74, 0x49, 0x50, 0x46, 0x53, 0x46, 0x69, 0x6c, 0x65, 0x52, 0x65, 0x71, 0x12, 0x1a,
	0x0a, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x48, 0x61, 0x73, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x48, 0x61, 0x73, 0x68, 0x2a, 0x27, 0x0a, 0x12, 0x46, 0x69,
	0x6c, 0x65, 0x44, 0x61, 0x74, 0x61, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x54, 0x79, 0x70, 0x65,
	0x12, 0x08, 0x0a, 0x04, 0x44, 0x61, 0x74, 0x61, 0x10, 0x00, 0x12, 0x07, 0x0a, 0x03, 0x45, 0x4f,
	0x46, 0x10, 0x01, 0x32, 0x74, 0x0a, 0x05, 0x41, 0x67, 0x65, 0x6e, 0x74, 0x12, 0x36, 0x0a, 0x0c,
	0x53, 0x65, 0x6e, 0x64, 0x49, 0x50, 0x46, 0x53, 0x46, 0x69, 0x6c, 0x65, 0x12, 0x0f, 0x2e, 0x46,
	0x69, 0x6c, 0x65, 0x44, 0x61, 0x74, 0x61, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x1a, 0x11, 0x2e,
	0x53, 0x65, 0x6e, 0x64, 0x49, 0x50, 0x46, 0x53, 0x46, 0x69, 0x6c, 0x65, 0x52, 0x65, 0x73, 0x70,
	0x22, 0x00, 0x28, 0x01, 0x12, 0x33, 0x0a, 0x0b, 0x47, 0x65, 0x74, 0x49, 0x50, 0x46, 0x53, 0x46,
	0x69, 0x6c, 0x65, 0x12, 0x0f, 0x2e, 0x47, 0x65, 0x74, 0x49, 0x50, 0x46, 0x53, 0x46, 0x69, 0x6c,
	0x65, 0x52, 0x65, 0x71, 0x1a, 0x0f, 0x2e, 0x46, 0x69, 0x6c, 0x65, 0x44, 0x61, 0x74, 0x61, 0x50,
	0x61, 0x63, 0x6b, 0x65, 0x74, 0x22, 0x00, 0x30, 0x01, 0x42, 0x09, 0x5a, 0x07, 0x2e, 0x3b, 0x61,
	0x67, 0x65, 0x6e, 0x74, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkgs_grpc_agent_agent_proto_rawDescOnce sync.Once
	file_pkgs_grpc_agent_agent_proto_rawDescData = file_pkgs_grpc_agent_agent_proto_rawDesc
)

func file_pkgs_grpc_agent_agent_proto_rawDescGZIP() []byte {
	file_pkgs_grpc_agent_agent_proto_rawDescOnce.Do(func() {
		file_pkgs_grpc_agent_agent_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkgs_grpc_agent_agent_proto_rawDescData)
	})
	return file_pkgs_grpc_agent_agent_proto_rawDescData
}

var file_pkgs_grpc_agent_agent_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_pkgs_grpc_agent_agent_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_pkgs_grpc_agent_agent_proto_goTypes = []interface{}{
	(FileDataPacketType)(0),  // 0: FileDataPacketType
	(*FileDataPacket)(nil),   // 1: FileDataPacket
	(*SendIPFSFileResp)(nil), // 2: SendIPFSFileResp
	(*GetIPFSFileReq)(nil),   // 3: GetIPFSFileReq
}
var file_pkgs_grpc_agent_agent_proto_depIdxs = []int32{
	0, // 0: FileDataPacket.Type:type_name -> FileDataPacketType
	1, // 1: Agent.SendIPFSFile:input_type -> FileDataPacket
	3, // 2: Agent.GetIPFSFile:input_type -> GetIPFSFileReq
	2, // 3: Agent.SendIPFSFile:output_type -> SendIPFSFileResp
	1, // 4: Agent.GetIPFSFile:output_type -> FileDataPacket
	3, // [3:5] is the sub-list for method output_type
	1, // [1:3] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_pkgs_grpc_agent_agent_proto_init() }
func file_pkgs_grpc_agent_agent_proto_init() {
	if File_pkgs_grpc_agent_agent_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkgs_grpc_agent_agent_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FileDataPacket); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkgs_grpc_agent_agent_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SendIPFSFileResp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkgs_grpc_agent_agent_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetIPFSFileReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkgs_grpc_agent_agent_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkgs_grpc_agent_agent_proto_goTypes,
		DependencyIndexes: file_pkgs_grpc_agent_agent_proto_depIdxs,
		EnumInfos:         file_pkgs_grpc_agent_agent_proto_enumTypes,
		MessageInfos:      file_pkgs_grpc_agent_agent_proto_msgTypes,
	}.Build()
	File_pkgs_grpc_agent_agent_proto = out.File
	file_pkgs_grpc_agent_agent_proto_rawDesc = nil
	file_pkgs_grpc_agent_agent_proto_goTypes = nil
	file_pkgs_grpc_agent_agent_proto_depIdxs = nil
}

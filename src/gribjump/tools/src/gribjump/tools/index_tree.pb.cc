// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: src/gribjump/tools/index_tree.proto
// Protobuf C++ Version: 5.26.1

#include "src/gribjump/tools/index_tree.pb.h"

#include <algorithm>
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/extension_set.h"
#include "google/protobuf/wire_format_lite.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/generated_message_reflection.h"
#include "google/protobuf/reflection_ops.h"
#include "google/protobuf/wire_format.h"
#include "google/protobuf/generated_message_tctable_impl.h"
// @@protoc_insertion_point(includes)

// Must be included last.
#include "google/protobuf/port_def.inc"
PROTOBUF_PRAGMA_INIT_SEG
namespace _pb = ::google::protobuf;
namespace _pbi = ::google::protobuf::internal;
namespace _fl = ::google::protobuf::internal::field_layout;
namespace index_tree {

inline constexpr Node::Impl_::Impl_(
    ::_pbi::ConstantInitialized) noexcept
      : value_{},
        indexes_{},
        _indexes_cached_byte_size_{0},
        result_{},
        size_result_{},
        _size_result_cached_byte_size_{0},
        children_{},
        axis_(
            &::google::protobuf::internal::fixed_address_empty_string,
            ::_pbi::ConstantInitialized()),
        _cached_size_{0} {}

template <typename>
PROTOBUF_CONSTEXPR Node::Node(::_pbi::ConstantInitialized)
    : _impl_(::_pbi::ConstantInitialized()) {}
struct NodeDefaultTypeInternal {
  PROTOBUF_CONSTEXPR NodeDefaultTypeInternal() : _instance(::_pbi::ConstantInitialized{}) {}
  ~NodeDefaultTypeInternal() {}
  union {
    Node _instance;
  };
};

PROTOBUF_ATTRIBUTE_NO_DESTROY PROTOBUF_CONSTINIT
    PROTOBUF_ATTRIBUTE_INIT_PRIORITY1 NodeDefaultTypeInternal _Node_default_instance_;
}  // namespace index_tree
static ::_pb::Metadata file_level_metadata_src_2fgribjump_2ftools_2findex_5ftree_2eproto[1];
static constexpr const ::_pb::EnumDescriptor**
    file_level_enum_descriptors_src_2fgribjump_2ftools_2findex_5ftree_2eproto = nullptr;
static constexpr const ::_pb::ServiceDescriptor**
    file_level_service_descriptors_src_2fgribjump_2ftools_2findex_5ftree_2eproto = nullptr;
const ::uint32_t
    TableStruct_src_2fgribjump_2ftools_2findex_5ftree_2eproto::offsets[] ABSL_ATTRIBUTE_SECTION_VARIABLE(
        protodesc_cold) = {
        ~0u,  // no _has_bits_
        PROTOBUF_FIELD_OFFSET(::index_tree::Node, _internal_metadata_),
        ~0u,  // no _extensions_
        ~0u,  // no _oneof_case_
        ~0u,  // no _weak_field_map_
        ~0u,  // no _inlined_string_donated_
        ~0u,  // no _split_
        ~0u,  // no sizeof(Split)
        PROTOBUF_FIELD_OFFSET(::index_tree::Node, _impl_.axis_),
        PROTOBUF_FIELD_OFFSET(::index_tree::Node, _impl_.value_),
        PROTOBUF_FIELD_OFFSET(::index_tree::Node, _impl_.indexes_),
        PROTOBUF_FIELD_OFFSET(::index_tree::Node, _impl_.result_),
        PROTOBUF_FIELD_OFFSET(::index_tree::Node, _impl_.size_result_),
        PROTOBUF_FIELD_OFFSET(::index_tree::Node, _impl_.children_),
};

static const ::_pbi::MigrationSchema
    schemas[] ABSL_ATTRIBUTE_SECTION_VARIABLE(protodesc_cold) = {
        {0, -1, -1, sizeof(::index_tree::Node)},
};
static const ::_pb::Message* const file_default_instances[] = {
    &::index_tree::_Node_default_instance_._instance,
};
const char descriptor_table_protodef_src_2fgribjump_2ftools_2findex_5ftree_2eproto[] ABSL_ATTRIBUTE_SECTION_VARIABLE(
    protodesc_cold) = {
    "\n#src/gribjump/tools/index_tree.proto\022\ni"
    "ndex_tree\"}\n\004Node\022\014\n\004axis\030\001 \001(\t\022\r\n\005value"
    "\030\002 \003(\t\022\017\n\007indexes\030\003 \003(\003\022\016\n\006result\030\004 \003(\001\022"
    "\023\n\013size_result\030\005 \003(\003\022\"\n\010children\030\006 \003(\0132\020"
    ".index_tree.Nodeb\006proto3"
};
static ::absl::once_flag descriptor_table_src_2fgribjump_2ftools_2findex_5ftree_2eproto_once;
const ::_pbi::DescriptorTable descriptor_table_src_2fgribjump_2ftools_2findex_5ftree_2eproto = {
    false,
    false,
    184,
    descriptor_table_protodef_src_2fgribjump_2ftools_2findex_5ftree_2eproto,
    "src/gribjump/tools/index_tree.proto",
    &descriptor_table_src_2fgribjump_2ftools_2findex_5ftree_2eproto_once,
    nullptr,
    0,
    1,
    schemas,
    file_default_instances,
    TableStruct_src_2fgribjump_2ftools_2findex_5ftree_2eproto::offsets,
    file_level_metadata_src_2fgribjump_2ftools_2findex_5ftree_2eproto,
    file_level_enum_descriptors_src_2fgribjump_2ftools_2findex_5ftree_2eproto,
    file_level_service_descriptors_src_2fgribjump_2ftools_2findex_5ftree_2eproto,
};

// This function exists to be marked as weak.
// It can significantly speed up compilation by breaking up LLVM's SCC
// in the .pb.cc translation units. Large translation units see a
// reduction of more than 35% of walltime for optimized builds. Without
// the weak attribute all the messages in the file, including all the
// vtables and everything they use become part of the same SCC through
// a cycle like:
// GetMetadata -> descriptor table -> default instances ->
//   vtables -> GetMetadata
// By adding a weak function here we break the connection from the
// individual vtables back into the descriptor table.
PROTOBUF_ATTRIBUTE_WEAK const ::_pbi::DescriptorTable* descriptor_table_src_2fgribjump_2ftools_2findex_5ftree_2eproto_getter() {
  return &descriptor_table_src_2fgribjump_2ftools_2findex_5ftree_2eproto;
}
namespace index_tree {
// ===================================================================

class Node::_Internal {
 public:
};

Node::Node(::google::protobuf::Arena* arena)
    : ::google::protobuf::Message(arena) {
  SharedCtor(arena);
  // @@protoc_insertion_point(arena_constructor:index_tree.Node)
}
inline PROTOBUF_NDEBUG_INLINE Node::Impl_::Impl_(
    ::google::protobuf::internal::InternalVisibility visibility, ::google::protobuf::Arena* arena,
    const Impl_& from)
      : value_{visibility, arena, from.value_},
        indexes_{visibility, arena, from.indexes_},
        _indexes_cached_byte_size_{0},
        result_{visibility, arena, from.result_},
        size_result_{visibility, arena, from.size_result_},
        _size_result_cached_byte_size_{0},
        children_{visibility, arena, from.children_},
        axis_(arena, from.axis_),
        _cached_size_{0} {}

Node::Node(
    ::google::protobuf::Arena* arena,
    const Node& from)
    : ::google::protobuf::Message(arena) {
  Node* const _this = this;
  (void)_this;
  _internal_metadata_.MergeFrom<::google::protobuf::UnknownFieldSet>(
      from._internal_metadata_);
  new (&_impl_) Impl_(internal_visibility(), arena, from._impl_);

  // @@protoc_insertion_point(copy_constructor:index_tree.Node)
}
inline PROTOBUF_NDEBUG_INLINE Node::Impl_::Impl_(
    ::google::protobuf::internal::InternalVisibility visibility,
    ::google::protobuf::Arena* arena)
      : value_{visibility, arena},
        indexes_{visibility, arena},
        _indexes_cached_byte_size_{0},
        result_{visibility, arena},
        size_result_{visibility, arena},
        _size_result_cached_byte_size_{0},
        children_{visibility, arena},
        axis_(arena),
        _cached_size_{0} {}

inline void Node::SharedCtor(::_pb::Arena* arena) {
  new (&_impl_) Impl_(internal_visibility(), arena);
}
Node::~Node() {
  // @@protoc_insertion_point(destructor:index_tree.Node)
  _internal_metadata_.Delete<::google::protobuf::UnknownFieldSet>();
  SharedDtor();
}
inline void Node::SharedDtor() {
  ABSL_DCHECK(GetArena() == nullptr);
  _impl_.axis_.Destroy();
  _impl_.~Impl_();
}

const ::google::protobuf::MessageLite::ClassData*
Node::GetClassData() const {
  PROTOBUF_CONSTINIT static const ::google::protobuf::MessageLite::
      ClassDataFull _data_ = {
          {
              nullptr,  // OnDemandRegisterArenaDtor
              PROTOBUF_FIELD_OFFSET(Node, _impl_._cached_size_),
              false,
          },
          &Node::MergeImpl,
          &Node::kDescriptorMethods,
      };
  return &_data_;
}
PROTOBUF_NOINLINE void Node::Clear() {
// @@protoc_insertion_point(message_clear_start:index_tree.Node)
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  ::uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  _impl_.value_.Clear();
  _impl_.indexes_.Clear();
  _impl_.result_.Clear();
  _impl_.size_result_.Clear();
  _impl_.children_.Clear();
  _impl_.axis_.ClearToEmpty();
  _internal_metadata_.Clear<::google::protobuf::UnknownFieldSet>();
}

const char* Node::_InternalParse(
    const char* ptr, ::_pbi::ParseContext* ctx) {
  ptr = ::_pbi::TcParser::ParseLoop(this, ptr, ctx, &_table_.header);
  return ptr;
}


PROTOBUF_CONSTINIT PROTOBUF_ATTRIBUTE_INIT_PRIORITY1
const ::_pbi::TcParseTable<3, 6, 1, 33, 2> Node::_table_ = {
  {
    0,  // no _has_bits_
    0, // no _extensions_
    6, 56,  // max_field_number, fast_idx_mask
    offsetof(decltype(_table_), field_lookup_table),
    4294967232,  // skipmap
    offsetof(decltype(_table_), field_entries),
    6,  // num_field_entries
    1,  // num_aux_entries
    offsetof(decltype(_table_), aux_entries),
    &_Node_default_instance_._instance,
    ::_pbi::TcParser::GenericFallback,  // fallback
    #ifdef PROTOBUF_PREFETCH_PARSE_TABLE
    ::_pbi::TcParser::GetTable<::index_tree::Node>(),  // to_prefetch
    #endif  // PROTOBUF_PREFETCH_PARSE_TABLE
  }, {{
    {::_pbi::TcParser::MiniParse, {}},
    // string axis = 1;
    {::_pbi::TcParser::FastUS1,
     {10, 63, 0, PROTOBUF_FIELD_OFFSET(Node, _impl_.axis_)}},
    // repeated string value = 2;
    {::_pbi::TcParser::FastUR1,
     {18, 63, 0, PROTOBUF_FIELD_OFFSET(Node, _impl_.value_)}},
    // repeated int64 indexes = 3;
    {::_pbi::TcParser::FastV64P1,
     {26, 63, 0, PROTOBUF_FIELD_OFFSET(Node, _impl_.indexes_)}},
    // repeated double result = 4;
    {::_pbi::TcParser::FastF64P1,
     {34, 63, 0, PROTOBUF_FIELD_OFFSET(Node, _impl_.result_)}},
    // repeated int64 size_result = 5;
    {::_pbi::TcParser::FastV64P1,
     {42, 63, 0, PROTOBUF_FIELD_OFFSET(Node, _impl_.size_result_)}},
    // repeated .index_tree.Node children = 6;
    {::_pbi::TcParser::FastMtR1,
     {50, 63, 0, PROTOBUF_FIELD_OFFSET(Node, _impl_.children_)}},
    {::_pbi::TcParser::MiniParse, {}},
  }}, {{
    65535, 65535
  }}, {{
    // string axis = 1;
    {PROTOBUF_FIELD_OFFSET(Node, _impl_.axis_), 0, 0,
    (0 | ::_fl::kFcSingular | ::_fl::kUtf8String | ::_fl::kRepAString)},
    // repeated string value = 2;
    {PROTOBUF_FIELD_OFFSET(Node, _impl_.value_), 0, 0,
    (0 | ::_fl::kFcRepeated | ::_fl::kUtf8String | ::_fl::kRepSString)},
    // repeated int64 indexes = 3;
    {PROTOBUF_FIELD_OFFSET(Node, _impl_.indexes_), 0, 0,
    (0 | ::_fl::kFcRepeated | ::_fl::kPackedInt64)},
    // repeated double result = 4;
    {PROTOBUF_FIELD_OFFSET(Node, _impl_.result_), 0, 0,
    (0 | ::_fl::kFcRepeated | ::_fl::kPackedDouble)},
    // repeated int64 size_result = 5;
    {PROTOBUF_FIELD_OFFSET(Node, _impl_.size_result_), 0, 0,
    (0 | ::_fl::kFcRepeated | ::_fl::kPackedInt64)},
    // repeated .index_tree.Node children = 6;
    {PROTOBUF_FIELD_OFFSET(Node, _impl_.children_), 0, 0,
    (0 | ::_fl::kFcRepeated | ::_fl::kMessage | ::_fl::kTvTable)},
  }}, {{
    {::_pbi::TcParser::GetTable<::index_tree::Node>()},
  }}, {{
    "\17\4\5\0\0\0\0\0"
    "index_tree.Node"
    "axis"
    "value"
  }},
};

::uint8_t* Node::_InternalSerialize(
    ::uint8_t* target,
    ::google::protobuf::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:index_tree.Node)
  ::uint32_t cached_has_bits = 0;
  (void)cached_has_bits;

  // string axis = 1;
  if (!this->_internal_axis().empty()) {
    const std::string& _s = this->_internal_axis();
    ::google::protobuf::internal::WireFormatLite::VerifyUtf8String(
        _s.data(), static_cast<int>(_s.length()), ::google::protobuf::internal::WireFormatLite::SERIALIZE, "index_tree.Node.axis");
    target = stream->WriteStringMaybeAliased(1, _s, target);
  }

  // repeated string value = 2;
  for (int i = 0, n = this->_internal_value_size(); i < n; ++i) {
    const auto& s = this->_internal_value().Get(i);
    ::google::protobuf::internal::WireFormatLite::VerifyUtf8String(
        s.data(), static_cast<int>(s.length()), ::google::protobuf::internal::WireFormatLite::SERIALIZE, "index_tree.Node.value");
    target = stream->WriteString(2, s, target);
  }

  // repeated int64 indexes = 3;
  {
    int byte_size = _impl_._indexes_cached_byte_size_.Get();
    if (byte_size > 0) {
      target = stream->WriteInt64Packed(
          3, _internal_indexes(), byte_size, target);
    }
  }

  // repeated double result = 4;
  if (this->_internal_result_size() > 0) {
    target = stream->WriteFixedPacked(4, _internal_result(), target);
  }

  // repeated int64 size_result = 5;
  {
    int byte_size = _impl_._size_result_cached_byte_size_.Get();
    if (byte_size > 0) {
      target = stream->WriteInt64Packed(
          5, _internal_size_result(), byte_size, target);
    }
  }

  // repeated .index_tree.Node children = 6;
  for (unsigned i = 0, n = static_cast<unsigned>(
                           this->_internal_children_size());
       i < n; i++) {
    const auto& repfield = this->_internal_children().Get(i);
    target =
        ::google::protobuf::internal::WireFormatLite::InternalWriteMessage(
            6, repfield, repfield.GetCachedSize(),
            target, stream);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target =
        ::_pbi::WireFormat::InternalSerializeUnknownFieldsToArray(
            _internal_metadata_.unknown_fields<::google::protobuf::UnknownFieldSet>(::google::protobuf::UnknownFieldSet::default_instance), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:index_tree.Node)
  return target;
}

::size_t Node::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:index_tree.Node)
  ::size_t total_size = 0;

  ::uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // repeated string value = 2;
  total_size += 1 * ::google::protobuf::internal::FromIntSize(_internal_value().size());
  for (int i = 0, n = _internal_value().size(); i < n; ++i) {
    total_size += ::google::protobuf::internal::WireFormatLite::StringSize(
        _internal_value().Get(i));
  }
  // repeated int64 indexes = 3;
  {
    std::size_t data_size = ::_pbi::WireFormatLite::Int64Size(
        this->_internal_indexes())
    ;
    _impl_._indexes_cached_byte_size_.Set(::_pbi::ToCachedSize(data_size));
    std::size_t tag_size = data_size == 0
        ? 0
        : 1 + ::_pbi::WireFormatLite::Int32Size(
                            static_cast<int32_t>(data_size))
    ;
    total_size += tag_size + data_size;
  }
  // repeated double result = 4;
  {
    std::size_t data_size = std::size_t{8} *
        ::_pbi::FromIntSize(this->_internal_result_size())
    ;
    std::size_t tag_size = data_size == 0
        ? 0
        : 1 + ::_pbi::WireFormatLite::Int32Size(
                            static_cast<int32_t>(data_size))
    ;
    total_size += tag_size + data_size;
  }
  // repeated int64 size_result = 5;
  {
    std::size_t data_size = ::_pbi::WireFormatLite::Int64Size(
        this->_internal_size_result())
    ;
    _impl_._size_result_cached_byte_size_.Set(::_pbi::ToCachedSize(data_size));
    std::size_t tag_size = data_size == 0
        ? 0
        : 1 + ::_pbi::WireFormatLite::Int32Size(
                            static_cast<int32_t>(data_size))
    ;
    total_size += tag_size + data_size;
  }
  // repeated .index_tree.Node children = 6;
  total_size += 1UL * this->_internal_children_size();
  for (const auto& msg : this->_internal_children()) {
    total_size += ::google::protobuf::internal::WireFormatLite::MessageSize(msg);
  }
  // string axis = 1;
  if (!this->_internal_axis().empty()) {
    total_size += 1 + ::google::protobuf::internal::WireFormatLite::StringSize(
                                    this->_internal_axis());
  }

  return MaybeComputeUnknownFieldsSize(total_size, &_impl_._cached_size_);
}


void Node::MergeImpl(::google::protobuf::MessageLite& to_msg, const ::google::protobuf::MessageLite& from_msg) {
  auto* const _this = static_cast<Node*>(&to_msg);
  auto& from = static_cast<const Node&>(from_msg);
  // @@protoc_insertion_point(class_specific_merge_from_start:index_tree.Node)
  ABSL_DCHECK_NE(&from, _this);
  ::uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  _this->_internal_mutable_value()->MergeFrom(from._internal_value());
  _this->_internal_mutable_indexes()->MergeFrom(from._internal_indexes());
  _this->_internal_mutable_result()->MergeFrom(from._internal_result());
  _this->_internal_mutable_size_result()->MergeFrom(from._internal_size_result());
  _this->_internal_mutable_children()->MergeFrom(
      from._internal_children());
  if (!from._internal_axis().empty()) {
    _this->_internal_set_axis(from._internal_axis());
  }
  _this->_internal_metadata_.MergeFrom<::google::protobuf::UnknownFieldSet>(from._internal_metadata_);
}

void Node::CopyFrom(const Node& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:index_tree.Node)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

PROTOBUF_NOINLINE bool Node::IsInitialized() const {
  return true;
}

void Node::InternalSwap(Node* PROTOBUF_RESTRICT other) {
  using std::swap;
  auto* arena = GetArena();
  ABSL_DCHECK_EQ(arena, other->GetArena());
  _internal_metadata_.InternalSwap(&other->_internal_metadata_);
  _impl_.value_.InternalSwap(&other->_impl_.value_);
  _impl_.indexes_.InternalSwap(&other->_impl_.indexes_);
  _impl_.result_.InternalSwap(&other->_impl_.result_);
  _impl_.size_result_.InternalSwap(&other->_impl_.size_result_);
  _impl_.children_.InternalSwap(&other->_impl_.children_);
  ::_pbi::ArenaStringPtr::InternalSwap(&_impl_.axis_, &other->_impl_.axis_, arena);
}

::google::protobuf::Metadata Node::GetMetadata() const {
  return ::_pbi::AssignDescriptors(&descriptor_table_src_2fgribjump_2ftools_2findex_5ftree_2eproto_getter,
                                   &descriptor_table_src_2fgribjump_2ftools_2findex_5ftree_2eproto_once,
                                   file_level_metadata_src_2fgribjump_2ftools_2findex_5ftree_2eproto[0]);
}
// @@protoc_insertion_point(namespace_scope)
}  // namespace index_tree
namespace google {
namespace protobuf {
}  // namespace protobuf
}  // namespace google
// @@protoc_insertion_point(global_scope)
PROTOBUF_ATTRIBUTE_INIT_PRIORITY2
static ::std::false_type _static_init_ PROTOBUF_UNUSED =
    (::_pbi::AddDescriptors(&descriptor_table_src_2fgribjump_2ftools_2findex_5ftree_2eproto),
     ::std::false_type{});
#include "google/protobuf/port_undef.inc"

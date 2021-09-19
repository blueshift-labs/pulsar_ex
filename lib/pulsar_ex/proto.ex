defmodule PulsarEx.Proto.CompressionType do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :NONE | :LZ4 | :ZLIB | :ZSTD | :SNAPPY

  field(:NONE, 0)

  field(:LZ4, 1)

  field(:ZLIB, 2)

  field(:ZSTD, 3)

  field(:SNAPPY, 4)
end

defmodule PulsarEx.Proto.ProducerAccessMode do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :Shared | :Exclusive | :WaitForExclusive

  field(:Shared, 0)

  field(:Exclusive, 1)

  field(:WaitForExclusive, 2)
end

defmodule PulsarEx.Proto.ServerError do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2

  @type t ::
          integer
          | :UnknownError
          | :MetadataError
          | :PersistenceError
          | :AuthenticationError
          | :AuthorizationError
          | :ConsumerBusy
          | :ServiceNotReady
          | :ProducerBlockedQuotaExceededError
          | :ProducerBlockedQuotaExceededException
          | :ChecksumError
          | :UnsupportedVersionError
          | :TopicNotFound
          | :SubscriptionNotFound
          | :ConsumerNotFound
          | :TooManyRequests
          | :TopicTerminatedError
          | :ProducerBusy
          | :InvalidTopicName
          | :IncompatibleSchema
          | :ConsumerAssignError
          | :TransactionCoordinatorNotFound
          | :InvalidTxnStatus
          | :NotAllowedError
          | :TransactionConflict
          | :TransactionNotFound
          | :ProducerFenced

  field(:UnknownError, 0)

  field(:MetadataError, 1)

  field(:PersistenceError, 2)

  field(:AuthenticationError, 3)

  field(:AuthorizationError, 4)

  field(:ConsumerBusy, 5)

  field(:ServiceNotReady, 6)

  field(:ProducerBlockedQuotaExceededError, 7)

  field(:ProducerBlockedQuotaExceededException, 8)

  field(:ChecksumError, 9)

  field(:UnsupportedVersionError, 10)

  field(:TopicNotFound, 11)

  field(:SubscriptionNotFound, 12)

  field(:ConsumerNotFound, 13)

  field(:TooManyRequests, 14)

  field(:TopicTerminatedError, 15)

  field(:ProducerBusy, 16)

  field(:InvalidTopicName, 17)

  field(:IncompatibleSchema, 18)

  field(:ConsumerAssignError, 19)

  field(:TransactionCoordinatorNotFound, 20)

  field(:InvalidTxnStatus, 21)

  field(:NotAllowedError, 22)

  field(:TransactionConflict, 23)

  field(:TransactionNotFound, 24)

  field(:ProducerFenced, 25)
end

defmodule PulsarEx.Proto.AuthMethod do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :AuthMethodNone | :AuthMethodYcaV1 | :AuthMethodAthens

  field(:AuthMethodNone, 0)

  field(:AuthMethodYcaV1, 1)

  field(:AuthMethodAthens, 2)
end

defmodule PulsarEx.Proto.ProtocolVersion do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2

  @type t ::
          integer
          | :v0
          | :v1
          | :v2
          | :v3
          | :v4
          | :v5
          | :v6
          | :v7
          | :v8
          | :v9
          | :v10
          | :v11
          | :v12
          | :v13
          | :v14
          | :v15
          | :v16
          | :v17

  field(:v0, 0)

  field(:v1, 1)

  field(:v2, 2)

  field(:v3, 3)

  field(:v4, 4)

  field(:v5, 5)

  field(:v6, 6)

  field(:v7, 7)

  field(:v8, 8)

  field(:v9, 9)

  field(:v10, 10)

  field(:v11, 11)

  field(:v12, 12)

  field(:v13, 13)

  field(:v14, 14)

  field(:v15, 15)

  field(:v16, 16)

  field(:v17, 17)
end

defmodule PulsarEx.Proto.KeySharedMode do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :AUTO_SPLIT | :STICKY

  field(:AUTO_SPLIT, 0)

  field(:STICKY, 1)
end

defmodule PulsarEx.Proto.TxnAction do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :COMMIT | :ABORT

  field(:COMMIT, 0)

  field(:ABORT, 1)
end

defmodule PulsarEx.Proto.Schema.Type do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2

  @type t ::
          integer
          | :None
          | :String
          | :Json
          | :Protobuf
          | :Avro
          | :Bool
          | :Int8
          | :Int16
          | :Int32
          | :Int64
          | :Float
          | :Double
          | :Date
          | :Time
          | :Timestamp
          | :KeyValue
          | :Instant
          | :LocalDate
          | :LocalTime
          | :LocalDateTime
          | :ProtobufNative

  field(:None, 0)

  field(:String, 1)

  field(:Json, 2)

  field(:Protobuf, 3)

  field(:Avro, 4)

  field(:Bool, 5)

  field(:Int8, 6)

  field(:Int16, 7)

  field(:Int32, 8)

  field(:Int64, 9)

  field(:Float, 10)

  field(:Double, 11)

  field(:Date, 12)

  field(:Time, 13)

  field(:Timestamp, 14)

  field(:KeyValue, 15)

  field(:Instant, 16)

  field(:LocalDate, 17)

  field(:LocalTime, 18)

  field(:LocalDateTime, 19)

  field(:ProtobufNative, 20)
end

defmodule PulsarEx.Proto.CommandSubscribe.SubType do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :Exclusive | :Shared | :Failover | :Key_Shared

  field(:Exclusive, 0)

  field(:Shared, 1)

  field(:Failover, 2)

  field(:Key_Shared, 3)
end

defmodule PulsarEx.Proto.CommandSubscribe.InitialPosition do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :Latest | :Earliest

  field(:Latest, 0)

  field(:Earliest, 1)
end

defmodule PulsarEx.Proto.CommandPartitionedTopicMetadataResponse.LookupType do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :Success | :Failed

  field(:Success, 0)

  field(:Failed, 1)
end

defmodule PulsarEx.Proto.CommandLookupTopicResponse.LookupType do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :Redirect | :Connect | :Failed

  field(:Redirect, 0)

  field(:Connect, 1)

  field(:Failed, 2)
end

defmodule PulsarEx.Proto.CommandAck.AckType do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :Individual | :Cumulative

  field(:Individual, 0)

  field(:Cumulative, 1)
end

defmodule PulsarEx.Proto.CommandAck.ValidationError do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2

  @type t ::
          integer
          | :UncompressedSizeCorruption
          | :DecompressionError
          | :ChecksumMismatch
          | :BatchDeSerializeError
          | :DecryptionError

  field(:UncompressedSizeCorruption, 0)

  field(:DecompressionError, 1)

  field(:ChecksumMismatch, 2)

  field(:BatchDeSerializeError, 3)

  field(:DecryptionError, 4)
end

defmodule PulsarEx.Proto.CommandGetTopicsOfNamespace.Mode do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2
  @type t :: integer | :PERSISTENT | :NON_PERSISTENT | :ALL

  field(:PERSISTENT, 0)

  field(:NON_PERSISTENT, 1)

  field(:ALL, 2)
end

defmodule PulsarEx.Proto.BaseCommand.Type do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto2

  @type t ::
          integer
          | :CONNECT
          | :CONNECTED
          | :SUBSCRIBE
          | :PRODUCER
          | :SEND
          | :SEND_RECEIPT
          | :SEND_ERROR
          | :MESSAGE
          | :ACK
          | :FLOW
          | :UNSUBSCRIBE
          | :SUCCESS
          | :ERROR
          | :CLOSE_PRODUCER
          | :CLOSE_CONSUMER
          | :PRODUCER_SUCCESS
          | :PING
          | :PONG
          | :REDELIVER_UNACKNOWLEDGED_MESSAGES
          | :PARTITIONED_METADATA
          | :PARTITIONED_METADATA_RESPONSE
          | :LOOKUP
          | :LOOKUP_RESPONSE
          | :CONSUMER_STATS
          | :CONSUMER_STATS_RESPONSE
          | :REACHED_END_OF_TOPIC
          | :SEEK
          | :GET_LAST_MESSAGE_ID
          | :GET_LAST_MESSAGE_ID_RESPONSE
          | :ACTIVE_CONSUMER_CHANGE
          | :GET_TOPICS_OF_NAMESPACE
          | :GET_TOPICS_OF_NAMESPACE_RESPONSE
          | :GET_SCHEMA
          | :GET_SCHEMA_RESPONSE
          | :AUTH_CHALLENGE
          | :AUTH_RESPONSE
          | :ACK_RESPONSE
          | :GET_OR_CREATE_SCHEMA
          | :GET_OR_CREATE_SCHEMA_RESPONSE
          | :NEW_TXN
          | :NEW_TXN_RESPONSE
          | :ADD_PARTITION_TO_TXN
          | :ADD_PARTITION_TO_TXN_RESPONSE
          | :ADD_SUBSCRIPTION_TO_TXN
          | :ADD_SUBSCRIPTION_TO_TXN_RESPONSE
          | :END_TXN
          | :END_TXN_RESPONSE
          | :END_TXN_ON_PARTITION
          | :END_TXN_ON_PARTITION_RESPONSE
          | :END_TXN_ON_SUBSCRIPTION
          | :END_TXN_ON_SUBSCRIPTION_RESPONSE

  field(:CONNECT, 2)

  field(:CONNECTED, 3)

  field(:SUBSCRIBE, 4)

  field(:PRODUCER, 5)

  field(:SEND, 6)

  field(:SEND_RECEIPT, 7)

  field(:SEND_ERROR, 8)

  field(:MESSAGE, 9)

  field(:ACK, 10)

  field(:FLOW, 11)

  field(:UNSUBSCRIBE, 12)

  field(:SUCCESS, 13)

  field(:ERROR, 14)

  field(:CLOSE_PRODUCER, 15)

  field(:CLOSE_CONSUMER, 16)

  field(:PRODUCER_SUCCESS, 17)

  field(:PING, 18)

  field(:PONG, 19)

  field(:REDELIVER_UNACKNOWLEDGED_MESSAGES, 20)

  field(:PARTITIONED_METADATA, 21)

  field(:PARTITIONED_METADATA_RESPONSE, 22)

  field(:LOOKUP, 23)

  field(:LOOKUP_RESPONSE, 24)

  field(:CONSUMER_STATS, 25)

  field(:CONSUMER_STATS_RESPONSE, 26)

  field(:REACHED_END_OF_TOPIC, 27)

  field(:SEEK, 28)

  field(:GET_LAST_MESSAGE_ID, 29)

  field(:GET_LAST_MESSAGE_ID_RESPONSE, 30)

  field(:ACTIVE_CONSUMER_CHANGE, 31)

  field(:GET_TOPICS_OF_NAMESPACE, 32)

  field(:GET_TOPICS_OF_NAMESPACE_RESPONSE, 33)

  field(:GET_SCHEMA, 34)

  field(:GET_SCHEMA_RESPONSE, 35)

  field(:AUTH_CHALLENGE, 36)

  field(:AUTH_RESPONSE, 37)

  field(:ACK_RESPONSE, 38)

  field(:GET_OR_CREATE_SCHEMA, 39)

  field(:GET_OR_CREATE_SCHEMA_RESPONSE, 40)

  field(:NEW_TXN, 50)

  field(:NEW_TXN_RESPONSE, 51)

  field(:ADD_PARTITION_TO_TXN, 52)

  field(:ADD_PARTITION_TO_TXN_RESPONSE, 53)

  field(:ADD_SUBSCRIPTION_TO_TXN, 54)

  field(:ADD_SUBSCRIPTION_TO_TXN_RESPONSE, 55)

  field(:END_TXN, 56)

  field(:END_TXN_RESPONSE, 57)

  field(:END_TXN_ON_PARTITION, 58)

  field(:END_TXN_ON_PARTITION_RESPONSE, 59)

  field(:END_TXN_ON_SUBSCRIPTION, 60)

  field(:END_TXN_ON_SUBSCRIPTION_RESPONSE, 61)
end

defmodule PulsarEx.Proto.Schema do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          name: String.t(),
          schema_data: binary,
          type: PulsarEx.Proto.Schema.Type.t(),
          properties: [PulsarEx.Proto.KeyValue.t()]
        }

  defstruct [:name, :schema_data, :type, :properties]

  field(:name, 1, required: true, type: :string)
  field(:schema_data, 3, required: true, type: :bytes)
  field(:type, 4, required: true, type: PulsarEx.Proto.Schema.Type, enum: true)
  field(:properties, 5, repeated: true, type: PulsarEx.Proto.KeyValue)
end

defmodule PulsarEx.Proto.MessageIdData do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          ledgerId: non_neg_integer,
          entryId: non_neg_integer,
          partition: integer,
          batch_index: integer,
          ack_set: [integer],
          batch_size: integer
        }

  defstruct [:ledgerId, :entryId, :partition, :batch_index, :ack_set, :batch_size]

  field(:ledgerId, 1, required: true, type: :uint64)
  field(:entryId, 2, required: true, type: :uint64)
  field(:partition, 3, optional: true, type: :int32, default: -1)
  field(:batch_index, 4, optional: true, type: :int32, default: -1)
  field(:ack_set, 5, repeated: true, type: :int64)
  field(:batch_size, 6, optional: true, type: :int32)
end

defmodule PulsarEx.Proto.KeyValue do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          key: String.t(),
          value: String.t()
        }

  defstruct [:key, :value]

  field(:key, 1, required: true, type: :string)
  field(:value, 2, required: true, type: :string)
end

defmodule PulsarEx.Proto.KeyLongValue do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          key: String.t(),
          value: non_neg_integer
        }

  defstruct [:key, :value]

  field(:key, 1, required: true, type: :string)
  field(:value, 2, required: true, type: :uint64)
end

defmodule PulsarEx.Proto.IntRange do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          start: integer,
          end: integer
        }

  defstruct [:start, :end]

  field(:start, 1, required: true, type: :int32)
  field(:end, 2, required: true, type: :int32)
end

defmodule PulsarEx.Proto.EncryptionKeys do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          key: String.t(),
          value: binary,
          metadata: [PulsarEx.Proto.KeyValue.t()]
        }

  defstruct [:key, :value, :metadata]

  field(:key, 1, required: true, type: :string)
  field(:value, 2, required: true, type: :bytes)
  field(:metadata, 3, repeated: true, type: PulsarEx.Proto.KeyValue)
end

defmodule PulsarEx.Proto.MessageMetadata do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          producer_name: String.t(),
          sequence_id: non_neg_integer,
          publish_time: non_neg_integer,
          properties: [PulsarEx.Proto.KeyValue.t()],
          replicated_from: String.t(),
          partition_key: String.t(),
          replicate_to: [String.t()],
          compression: PulsarEx.Proto.CompressionType.t(),
          uncompressed_size: non_neg_integer,
          num_messages_in_batch: integer,
          event_time: non_neg_integer,
          encryption_keys: [PulsarEx.Proto.EncryptionKeys.t()],
          encryption_algo: String.t(),
          encryption_param: binary,
          schema_version: binary,
          partition_key_b64_encoded: boolean,
          ordering_key: binary,
          deliver_at_time: integer,
          marker_type: integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          highest_sequence_id: non_neg_integer,
          null_value: boolean,
          uuid: String.t(),
          num_chunks_from_msg: integer,
          total_chunk_msg_size: integer,
          chunk_id: integer,
          null_partition_key: boolean
        }

  defstruct [
    :producer_name,
    :sequence_id,
    :publish_time,
    :properties,
    :replicated_from,
    :partition_key,
    :replicate_to,
    :compression,
    :uncompressed_size,
    :num_messages_in_batch,
    :event_time,
    :encryption_keys,
    :encryption_algo,
    :encryption_param,
    :schema_version,
    :partition_key_b64_encoded,
    :ordering_key,
    :deliver_at_time,
    :marker_type,
    :txnid_least_bits,
    :txnid_most_bits,
    :highest_sequence_id,
    :null_value,
    :uuid,
    :num_chunks_from_msg,
    :total_chunk_msg_size,
    :chunk_id,
    :null_partition_key
  ]

  field(:producer_name, 1, required: true, type: :string)
  field(:sequence_id, 2, required: true, type: :uint64)
  field(:publish_time, 3, required: true, type: :uint64)
  field(:properties, 4, repeated: true, type: PulsarEx.Proto.KeyValue)
  field(:replicated_from, 5, optional: true, type: :string)
  field(:partition_key, 6, optional: true, type: :string)
  field(:replicate_to, 7, repeated: true, type: :string)

  field(:compression, 8,
    optional: true,
    type: PulsarEx.Proto.CompressionType,
    default: :NONE,
    enum: true
  )

  field(:uncompressed_size, 9, optional: true, type: :uint32, default: 0)
  field(:num_messages_in_batch, 11, optional: true, type: :int32)
  field(:event_time, 12, optional: true, type: :uint64, default: 0)
  field(:encryption_keys, 13, repeated: true, type: PulsarEx.Proto.EncryptionKeys)
  field(:encryption_algo, 14, optional: true, type: :string)
  field(:encryption_param, 15, optional: true, type: :bytes)
  field(:schema_version, 16, optional: true, type: :bytes)
  field(:partition_key_b64_encoded, 17, optional: true, type: :bool, default: false)
  field(:ordering_key, 18, optional: true, type: :bytes)
  field(:deliver_at_time, 19, optional: true, type: :int64)
  field(:marker_type, 20, optional: true, type: :int32)
  field(:txnid_least_bits, 22, optional: true, type: :uint64)
  field(:txnid_most_bits, 23, optional: true, type: :uint64)
  field(:highest_sequence_id, 24, optional: true, type: :uint64, default: 0)
  field(:null_value, 25, optional: true, type: :bool, default: false)
  field(:uuid, 26, optional: true, type: :string)
  field(:num_chunks_from_msg, 27, optional: true, type: :int32)
  field(:total_chunk_msg_size, 28, optional: true, type: :int32)
  field(:chunk_id, 29, optional: true, type: :int32)
  field(:null_partition_key, 30, optional: true, type: :bool, default: false)
end

defmodule PulsarEx.Proto.SingleMessageMetadata do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          properties: [PulsarEx.Proto.KeyValue.t()],
          partition_key: String.t(),
          payload_size: integer,
          compacted_out: boolean,
          event_time: non_neg_integer,
          partition_key_b64_encoded: boolean,
          ordering_key: binary,
          sequence_id: non_neg_integer,
          null_value: boolean,
          null_partition_key: boolean
        }

  defstruct [
    :properties,
    :partition_key,
    :payload_size,
    :compacted_out,
    :event_time,
    :partition_key_b64_encoded,
    :ordering_key,
    :sequence_id,
    :null_value,
    :null_partition_key
  ]

  field(:properties, 1, repeated: true, type: PulsarEx.Proto.KeyValue)
  field(:partition_key, 2, optional: true, type: :string)
  field(:payload_size, 3, required: true, type: :int32)
  field(:compacted_out, 4, optional: true, type: :bool, default: false)
  field(:event_time, 5, optional: true, type: :uint64, default: 0)
  field(:partition_key_b64_encoded, 6, optional: true, type: :bool, default: false)
  field(:ordering_key, 7, optional: true, type: :bytes)
  field(:sequence_id, 8, optional: true, type: :uint64)
  field(:null_value, 9, optional: true, type: :bool, default: false)
  field(:null_partition_key, 10, optional: true, type: :bool, default: false)
end

defmodule PulsarEx.Proto.BrokerEntryMetadata do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          broker_timestamp: non_neg_integer,
          index: non_neg_integer
        }

  defstruct [:broker_timestamp, :index]

  field(:broker_timestamp, 1, optional: true, type: :uint64)
  field(:index, 2, optional: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandConnect do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          client_version: String.t(),
          auth_method: PulsarEx.Proto.AuthMethod.t(),
          auth_method_name: String.t(),
          auth_data: binary,
          protocol_version: integer,
          proxy_to_broker_url: String.t(),
          original_principal: String.t(),
          original_auth_data: String.t(),
          original_auth_method: String.t(),
          feature_flags: PulsarEx.Proto.FeatureFlags.t() | nil
        }

  defstruct [
    :client_version,
    :auth_method,
    :auth_method_name,
    :auth_data,
    :protocol_version,
    :proxy_to_broker_url,
    :original_principal,
    :original_auth_data,
    :original_auth_method,
    :feature_flags
  ]

  field(:client_version, 1, required: true, type: :string)
  field(:auth_method, 2, optional: true, type: PulsarEx.Proto.AuthMethod, enum: true)
  field(:auth_method_name, 5, optional: true, type: :string)
  field(:auth_data, 3, optional: true, type: :bytes)
  field(:protocol_version, 4, optional: true, type: :int32, default: 0)
  field(:proxy_to_broker_url, 6, optional: true, type: :string)
  field(:original_principal, 7, optional: true, type: :string)
  field(:original_auth_data, 8, optional: true, type: :string)
  field(:original_auth_method, 9, optional: true, type: :string)
  field(:feature_flags, 10, optional: true, type: PulsarEx.Proto.FeatureFlags)
end

defmodule PulsarEx.Proto.FeatureFlags do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          supports_auth_refresh: boolean,
          supports_broker_entry_metadata: boolean
        }

  defstruct [:supports_auth_refresh, :supports_broker_entry_metadata]

  field(:supports_auth_refresh, 1, optional: true, type: :bool, default: false)
  field(:supports_broker_entry_metadata, 2, optional: true, type: :bool, default: false)
end

defmodule PulsarEx.Proto.CommandConnected do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          server_version: String.t(),
          protocol_version: integer,
          max_message_size: integer
        }

  defstruct [:server_version, :protocol_version, :max_message_size]

  field(:server_version, 1, required: true, type: :string)
  field(:protocol_version, 2, optional: true, type: :int32, default: 0)
  field(:max_message_size, 3, optional: true, type: :int32)
end

defmodule PulsarEx.Proto.CommandAuthResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          client_version: String.t(),
          response: PulsarEx.Proto.AuthData.t() | nil,
          protocol_version: integer
        }

  defstruct [:client_version, :response, :protocol_version]

  field(:client_version, 1, optional: true, type: :string)
  field(:response, 2, optional: true, type: PulsarEx.Proto.AuthData)
  field(:protocol_version, 3, optional: true, type: :int32, default: 0)
end

defmodule PulsarEx.Proto.CommandAuthChallenge do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          server_version: String.t(),
          challenge: PulsarEx.Proto.AuthData.t() | nil,
          protocol_version: integer
        }

  defstruct [:server_version, :challenge, :protocol_version]

  field(:server_version, 1, optional: true, type: :string)
  field(:challenge, 2, optional: true, type: PulsarEx.Proto.AuthData)
  field(:protocol_version, 3, optional: true, type: :int32, default: 0)
end

defmodule PulsarEx.Proto.AuthData do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          auth_method_name: String.t(),
          auth_data: binary
        }

  defstruct [:auth_method_name, :auth_data]

  field(:auth_method_name, 1, optional: true, type: :string)
  field(:auth_data, 2, optional: true, type: :bytes)
end

defmodule PulsarEx.Proto.KeySharedMeta do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          keySharedMode: PulsarEx.Proto.KeySharedMode.t(),
          hashRanges: [PulsarEx.Proto.IntRange.t()],
          allowOutOfOrderDelivery: boolean
        }

  defstruct [:keySharedMode, :hashRanges, :allowOutOfOrderDelivery]

  field(:keySharedMode, 1, required: true, type: PulsarEx.Proto.KeySharedMode, enum: true)
  field(:hashRanges, 3, repeated: true, type: PulsarEx.Proto.IntRange)
  field(:allowOutOfOrderDelivery, 4, optional: true, type: :bool, default: false)
end

defmodule PulsarEx.Proto.CommandSubscribe do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          topic: String.t(),
          subscription: String.t(),
          subType: PulsarEx.Proto.CommandSubscribe.SubType.t(),
          consumer_id: non_neg_integer,
          request_id: non_neg_integer,
          consumer_name: String.t(),
          priority_level: integer,
          durable: boolean,
          start_message_id: PulsarEx.Proto.MessageIdData.t() | nil,
          metadata: [PulsarEx.Proto.KeyValue.t()],
          read_compacted: boolean,
          schema: PulsarEx.Proto.Schema.t() | nil,
          initialPosition: PulsarEx.Proto.CommandSubscribe.InitialPosition.t(),
          replicate_subscription_state: boolean,
          force_topic_creation: boolean,
          start_message_rollback_duration_sec: non_neg_integer,
          keySharedMeta: PulsarEx.Proto.KeySharedMeta.t() | nil
        }

  defstruct [
    :topic,
    :subscription,
    :subType,
    :consumer_id,
    :request_id,
    :consumer_name,
    :priority_level,
    :durable,
    :start_message_id,
    :metadata,
    :read_compacted,
    :schema,
    :initialPosition,
    :replicate_subscription_state,
    :force_topic_creation,
    :start_message_rollback_duration_sec,
    :keySharedMeta
  ]

  field(:topic, 1, required: true, type: :string)
  field(:subscription, 2, required: true, type: :string)
  field(:subType, 3, required: true, type: PulsarEx.Proto.CommandSubscribe.SubType, enum: true)
  field(:consumer_id, 4, required: true, type: :uint64)
  field(:request_id, 5, required: true, type: :uint64)
  field(:consumer_name, 6, optional: true, type: :string)
  field(:priority_level, 7, optional: true, type: :int32)
  field(:durable, 8, optional: true, type: :bool, default: true)
  field(:start_message_id, 9, optional: true, type: PulsarEx.Proto.MessageIdData)
  field(:metadata, 10, repeated: true, type: PulsarEx.Proto.KeyValue)
  field(:read_compacted, 11, optional: true, type: :bool)
  field(:schema, 12, optional: true, type: PulsarEx.Proto.Schema)

  field(:initialPosition, 13,
    optional: true,
    type: PulsarEx.Proto.CommandSubscribe.InitialPosition,
    default: :Latest,
    enum: true
  )

  field(:replicate_subscription_state, 14, optional: true, type: :bool)
  field(:force_topic_creation, 15, optional: true, type: :bool, default: true)
  field(:start_message_rollback_duration_sec, 16, optional: true, type: :uint64, default: 0)
  field(:keySharedMeta, 17, optional: true, type: PulsarEx.Proto.KeySharedMeta)
end

defmodule PulsarEx.Proto.CommandPartitionedTopicMetadata do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          topic: String.t(),
          request_id: non_neg_integer,
          original_principal: String.t(),
          original_auth_data: String.t(),
          original_auth_method: String.t()
        }

  defstruct [:topic, :request_id, :original_principal, :original_auth_data, :original_auth_method]

  field(:topic, 1, required: true, type: :string)
  field(:request_id, 2, required: true, type: :uint64)
  field(:original_principal, 3, optional: true, type: :string)
  field(:original_auth_data, 4, optional: true, type: :string)
  field(:original_auth_method, 5, optional: true, type: :string)
end

defmodule PulsarEx.Proto.CommandPartitionedTopicMetadataResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          partitions: non_neg_integer,
          request_id: non_neg_integer,
          response: PulsarEx.Proto.CommandPartitionedTopicMetadataResponse.LookupType.t(),
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t()
        }

  defstruct [:partitions, :request_id, :response, :error, :message]

  field(:partitions, 1, optional: true, type: :uint32)
  field(:request_id, 2, required: true, type: :uint64)

  field(:response, 3,
    optional: true,
    type: PulsarEx.Proto.CommandPartitionedTopicMetadataResponse.LookupType,
    enum: true
  )

  field(:error, 4, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 5, optional: true, type: :string)
end

defmodule PulsarEx.Proto.CommandLookupTopic do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          topic: String.t(),
          request_id: non_neg_integer,
          authoritative: boolean,
          original_principal: String.t(),
          original_auth_data: String.t(),
          original_auth_method: String.t(),
          advertised_listener_name: String.t()
        }

  defstruct [
    :topic,
    :request_id,
    :authoritative,
    :original_principal,
    :original_auth_data,
    :original_auth_method,
    :advertised_listener_name
  ]

  field(:topic, 1, required: true, type: :string)
  field(:request_id, 2, required: true, type: :uint64)
  field(:authoritative, 3, optional: true, type: :bool, default: false)
  field(:original_principal, 4, optional: true, type: :string)
  field(:original_auth_data, 5, optional: true, type: :string)
  field(:original_auth_method, 6, optional: true, type: :string)
  field(:advertised_listener_name, 7, optional: true, type: :string)
end

defmodule PulsarEx.Proto.CommandLookupTopicResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          brokerServiceUrl: String.t(),
          brokerServiceUrlTls: String.t(),
          response: PulsarEx.Proto.CommandLookupTopicResponse.LookupType.t(),
          request_id: non_neg_integer,
          authoritative: boolean,
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t(),
          proxy_through_service_url: boolean
        }

  defstruct [
    :brokerServiceUrl,
    :brokerServiceUrlTls,
    :response,
    :request_id,
    :authoritative,
    :error,
    :message,
    :proxy_through_service_url
  ]

  field(:brokerServiceUrl, 1, optional: true, type: :string)
  field(:brokerServiceUrlTls, 2, optional: true, type: :string)

  field(:response, 3,
    optional: true,
    type: PulsarEx.Proto.CommandLookupTopicResponse.LookupType,
    enum: true
  )

  field(:request_id, 4, required: true, type: :uint64)
  field(:authoritative, 5, optional: true, type: :bool, default: false)
  field(:error, 6, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 7, optional: true, type: :string)
  field(:proxy_through_service_url, 8, optional: true, type: :bool, default: false)
end

defmodule PulsarEx.Proto.CommandProducer do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          topic: String.t(),
          producer_id: non_neg_integer,
          request_id: non_neg_integer,
          producer_name: String.t(),
          encrypted: boolean,
          metadata: [PulsarEx.Proto.KeyValue.t()],
          schema: PulsarEx.Proto.Schema.t() | nil,
          epoch: non_neg_integer,
          user_provided_producer_name: boolean,
          producer_access_mode: PulsarEx.Proto.ProducerAccessMode.t(),
          topic_epoch: non_neg_integer
        }

  defstruct [
    :topic,
    :producer_id,
    :request_id,
    :producer_name,
    :encrypted,
    :metadata,
    :schema,
    :epoch,
    :user_provided_producer_name,
    :producer_access_mode,
    :topic_epoch
  ]

  field(:topic, 1, required: true, type: :string)
  field(:producer_id, 2, required: true, type: :uint64)
  field(:request_id, 3, required: true, type: :uint64)
  field(:producer_name, 4, optional: true, type: :string)
  field(:encrypted, 5, optional: true, type: :bool, default: false)
  field(:metadata, 6, repeated: true, type: PulsarEx.Proto.KeyValue)
  field(:schema, 7, optional: true, type: PulsarEx.Proto.Schema)
  field(:epoch, 8, optional: true, type: :uint64, default: 0)
  field(:user_provided_producer_name, 9, optional: true, type: :bool, default: true)

  field(:producer_access_mode, 10,
    optional: true,
    type: PulsarEx.Proto.ProducerAccessMode,
    default: :Shared,
    enum: true
  )

  field(:topic_epoch, 11, optional: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandSend do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          producer_id: non_neg_integer,
          sequence_id: non_neg_integer,
          num_messages: integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          highest_sequence_id: non_neg_integer,
          is_chunk: boolean
        }

  defstruct [
    :producer_id,
    :sequence_id,
    :num_messages,
    :txnid_least_bits,
    :txnid_most_bits,
    :highest_sequence_id,
    :is_chunk
  ]

  field(:producer_id, 1, required: true, type: :uint64)
  field(:sequence_id, 2, required: true, type: :uint64)
  field(:num_messages, 3, optional: true, type: :int32, default: 1)
  field(:txnid_least_bits, 4, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 5, optional: true, type: :uint64, default: 0)
  field(:highest_sequence_id, 6, optional: true, type: :uint64, default: 0)
  field(:is_chunk, 7, optional: true, type: :bool, default: false)
end

defmodule PulsarEx.Proto.CommandSendReceipt do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          producer_id: non_neg_integer,
          sequence_id: non_neg_integer,
          message_id: PulsarEx.Proto.MessageIdData.t() | nil,
          highest_sequence_id: non_neg_integer
        }

  defstruct [:producer_id, :sequence_id, :message_id, :highest_sequence_id]

  field(:producer_id, 1, required: true, type: :uint64)
  field(:sequence_id, 2, required: true, type: :uint64)
  field(:message_id, 3, optional: true, type: PulsarEx.Proto.MessageIdData)
  field(:highest_sequence_id, 4, optional: true, type: :uint64, default: 0)
end

defmodule PulsarEx.Proto.CommandSendError do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          producer_id: non_neg_integer,
          sequence_id: non_neg_integer,
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t()
        }

  defstruct [:producer_id, :sequence_id, :error, :message]

  field(:producer_id, 1, required: true, type: :uint64)
  field(:sequence_id, 2, required: true, type: :uint64)
  field(:error, 3, required: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 4, required: true, type: :string)
end

defmodule PulsarEx.Proto.CommandMessage do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer,
          message_id: PulsarEx.Proto.MessageIdData.t() | nil,
          redelivery_count: non_neg_integer,
          ack_set: [integer]
        }

  defstruct [:consumer_id, :message_id, :redelivery_count, :ack_set]

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:message_id, 2, required: true, type: PulsarEx.Proto.MessageIdData)
  field(:redelivery_count, 3, optional: true, type: :uint32, default: 0)
  field(:ack_set, 4, repeated: true, type: :int64)
end

defmodule PulsarEx.Proto.CommandAck do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer,
          ack_type: PulsarEx.Proto.CommandAck.AckType.t(),
          message_id: [PulsarEx.Proto.MessageIdData.t()],
          validation_error: PulsarEx.Proto.CommandAck.ValidationError.t(),
          properties: [PulsarEx.Proto.KeyLongValue.t()],
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          request_id: non_neg_integer
        }

  defstruct [
    :consumer_id,
    :ack_type,
    :message_id,
    :validation_error,
    :properties,
    :txnid_least_bits,
    :txnid_most_bits,
    :request_id
  ]

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:ack_type, 2, required: true, type: PulsarEx.Proto.CommandAck.AckType, enum: true)
  field(:message_id, 3, repeated: true, type: PulsarEx.Proto.MessageIdData)

  field(:validation_error, 4,
    optional: true,
    type: PulsarEx.Proto.CommandAck.ValidationError,
    enum: true
  )

  field(:properties, 5, repeated: true, type: PulsarEx.Proto.KeyLongValue)
  field(:txnid_least_bits, 6, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 7, optional: true, type: :uint64, default: 0)
  field(:request_id, 8, optional: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandAckResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t(),
          request_id: non_neg_integer
        }

  defstruct [:consumer_id, :txnid_least_bits, :txnid_most_bits, :error, :message, :request_id]

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:error, 4, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 5, optional: true, type: :string)
  field(:request_id, 6, optional: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandActiveConsumerChange do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer,
          is_active: boolean
        }

  defstruct [:consumer_id, :is_active]

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:is_active, 2, optional: true, type: :bool, default: false)
end

defmodule PulsarEx.Proto.CommandFlow do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer,
          messagePermits: non_neg_integer
        }

  defstruct [:consumer_id, :messagePermits]

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:messagePermits, 2, required: true, type: :uint32)
end

defmodule PulsarEx.Proto.CommandUnsubscribe do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer,
          request_id: non_neg_integer
        }

  defstruct [:consumer_id, :request_id]

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:request_id, 2, required: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandSeek do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer,
          request_id: non_neg_integer,
          message_id: PulsarEx.Proto.MessageIdData.t() | nil,
          message_publish_time: non_neg_integer
        }

  defstruct [:consumer_id, :request_id, :message_id, :message_publish_time]

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:request_id, 2, required: true, type: :uint64)
  field(:message_id, 3, optional: true, type: PulsarEx.Proto.MessageIdData)
  field(:message_publish_time, 4, optional: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandReachedEndOfTopic do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer
        }

  defstruct [:consumer_id]

  field(:consumer_id, 1, required: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandCloseProducer do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          producer_id: non_neg_integer,
          request_id: non_neg_integer
        }

  defstruct [:producer_id, :request_id]

  field(:producer_id, 1, required: true, type: :uint64)
  field(:request_id, 2, required: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandCloseConsumer do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer,
          request_id: non_neg_integer
        }

  defstruct [:consumer_id, :request_id]

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:request_id, 2, required: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandRedeliverUnacknowledgedMessages do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer,
          message_ids: [PulsarEx.Proto.MessageIdData.t()]
        }

  defstruct [:consumer_id, :message_ids]

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:message_ids, 2, repeated: true, type: PulsarEx.Proto.MessageIdData)
end

defmodule PulsarEx.Proto.CommandSuccess do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          schema: PulsarEx.Proto.Schema.t() | nil
        }

  defstruct [:request_id, :schema]

  field(:request_id, 1, required: true, type: :uint64)
  field(:schema, 2, optional: true, type: PulsarEx.Proto.Schema)
end

defmodule PulsarEx.Proto.CommandProducerSuccess do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          producer_name: String.t(),
          last_sequence_id: integer,
          schema_version: binary,
          topic_epoch: non_neg_integer,
          producer_ready: boolean
        }

  defstruct [
    :request_id,
    :producer_name,
    :last_sequence_id,
    :schema_version,
    :topic_epoch,
    :producer_ready
  ]

  field(:request_id, 1, required: true, type: :uint64)
  field(:producer_name, 2, required: true, type: :string)
  field(:last_sequence_id, 3, optional: true, type: :int64, default: -1)
  field(:schema_version, 4, optional: true, type: :bytes)
  field(:topic_epoch, 5, optional: true, type: :uint64)
  field(:producer_ready, 6, optional: true, type: :bool, default: true)
end

defmodule PulsarEx.Proto.CommandError do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t()
        }

  defstruct [:request_id, :error, :message]

  field(:request_id, 1, required: true, type: :uint64)
  field(:error, 2, required: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 3, required: true, type: :string)
end

defmodule PulsarEx.Proto.CommandPing do
  @moduledoc false
  use Protobuf, syntax: :proto2
  @type t :: %__MODULE__{}

  defstruct []
end

defmodule PulsarEx.Proto.CommandPong do
  @moduledoc false
  use Protobuf, syntax: :proto2
  @type t :: %__MODULE__{}

  defstruct []
end

defmodule PulsarEx.Proto.CommandConsumerStats do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          consumer_id: non_neg_integer
        }

  defstruct [:request_id, :consumer_id]

  field(:request_id, 1, required: true, type: :uint64)
  field(:consumer_id, 4, required: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandConsumerStatsResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          error_code: PulsarEx.Proto.ServerError.t(),
          error_message: String.t(),
          msgRateOut: float | :infinity | :negative_infinity | :nan,
          msgThroughputOut: float | :infinity | :negative_infinity | :nan,
          msgRateRedeliver: float | :infinity | :negative_infinity | :nan,
          consumerName: String.t(),
          availablePermits: non_neg_integer,
          unackedMessages: non_neg_integer,
          blockedConsumerOnUnackedMsgs: boolean,
          address: String.t(),
          connectedSince: String.t(),
          type: String.t(),
          msgRateExpired: float | :infinity | :negative_infinity | :nan,
          msgBacklog: non_neg_integer
        }

  defstruct [
    :request_id,
    :error_code,
    :error_message,
    :msgRateOut,
    :msgThroughputOut,
    :msgRateRedeliver,
    :consumerName,
    :availablePermits,
    :unackedMessages,
    :blockedConsumerOnUnackedMsgs,
    :address,
    :connectedSince,
    :type,
    :msgRateExpired,
    :msgBacklog
  ]

  field(:request_id, 1, required: true, type: :uint64)
  field(:error_code, 2, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:error_message, 3, optional: true, type: :string)
  field(:msgRateOut, 4, optional: true, type: :double)
  field(:msgThroughputOut, 5, optional: true, type: :double)
  field(:msgRateRedeliver, 6, optional: true, type: :double)
  field(:consumerName, 7, optional: true, type: :string)
  field(:availablePermits, 8, optional: true, type: :uint64)
  field(:unackedMessages, 9, optional: true, type: :uint64)
  field(:blockedConsumerOnUnackedMsgs, 10, optional: true, type: :bool)
  field(:address, 11, optional: true, type: :string)
  field(:connectedSince, 12, optional: true, type: :string)
  field(:type, 13, optional: true, type: :string)
  field(:msgRateExpired, 14, optional: true, type: :double)
  field(:msgBacklog, 15, optional: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandGetLastMessageId do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          consumer_id: non_neg_integer,
          request_id: non_neg_integer
        }

  defstruct [:consumer_id, :request_id]

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:request_id, 2, required: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandGetLastMessageIdResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          last_message_id: PulsarEx.Proto.MessageIdData.t() | nil,
          request_id: non_neg_integer,
          consumer_mark_delete_position: PulsarEx.Proto.MessageIdData.t() | nil
        }

  defstruct [:last_message_id, :request_id, :consumer_mark_delete_position]

  field(:last_message_id, 1, required: true, type: PulsarEx.Proto.MessageIdData)
  field(:request_id, 2, required: true, type: :uint64)
  field(:consumer_mark_delete_position, 3, optional: true, type: PulsarEx.Proto.MessageIdData)
end

defmodule PulsarEx.Proto.CommandGetTopicsOfNamespace do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          namespace: String.t(),
          mode: PulsarEx.Proto.CommandGetTopicsOfNamespace.Mode.t()
        }

  defstruct [:request_id, :namespace, :mode]

  field(:request_id, 1, required: true, type: :uint64)
  field(:namespace, 2, required: true, type: :string)

  field(:mode, 3,
    optional: true,
    type: PulsarEx.Proto.CommandGetTopicsOfNamespace.Mode,
    default: :PERSISTENT,
    enum: true
  )
end

defmodule PulsarEx.Proto.CommandGetTopicsOfNamespaceResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          topics: [String.t()]
        }

  defstruct [:request_id, :topics]

  field(:request_id, 1, required: true, type: :uint64)
  field(:topics, 2, repeated: true, type: :string)
end

defmodule PulsarEx.Proto.CommandGetSchema do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          topic: String.t(),
          schema_version: binary
        }

  defstruct [:request_id, :topic, :schema_version]

  field(:request_id, 1, required: true, type: :uint64)
  field(:topic, 2, required: true, type: :string)
  field(:schema_version, 3, optional: true, type: :bytes)
end

defmodule PulsarEx.Proto.CommandGetSchemaResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          error_code: PulsarEx.Proto.ServerError.t(),
          error_message: String.t(),
          schema: PulsarEx.Proto.Schema.t() | nil,
          schema_version: binary
        }

  defstruct [:request_id, :error_code, :error_message, :schema, :schema_version]

  field(:request_id, 1, required: true, type: :uint64)
  field(:error_code, 2, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:error_message, 3, optional: true, type: :string)
  field(:schema, 4, optional: true, type: PulsarEx.Proto.Schema)
  field(:schema_version, 5, optional: true, type: :bytes)
end

defmodule PulsarEx.Proto.CommandGetOrCreateSchema do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          topic: String.t(),
          schema: PulsarEx.Proto.Schema.t() | nil
        }

  defstruct [:request_id, :topic, :schema]

  field(:request_id, 1, required: true, type: :uint64)
  field(:topic, 2, required: true, type: :string)
  field(:schema, 3, required: true, type: PulsarEx.Proto.Schema)
end

defmodule PulsarEx.Proto.CommandGetOrCreateSchemaResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          error_code: PulsarEx.Proto.ServerError.t(),
          error_message: String.t(),
          schema_version: binary
        }

  defstruct [:request_id, :error_code, :error_message, :schema_version]

  field(:request_id, 1, required: true, type: :uint64)
  field(:error_code, 2, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:error_message, 3, optional: true, type: :string)
  field(:schema_version, 4, optional: true, type: :bytes)
end

defmodule PulsarEx.Proto.CommandNewTxn do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txn_ttl_seconds: non_neg_integer,
          tc_id: non_neg_integer
        }

  defstruct [:request_id, :txn_ttl_seconds, :tc_id]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txn_ttl_seconds, 2, optional: true, type: :uint64, default: 0)
  field(:tc_id, 3, optional: true, type: :uint64, default: 0)
end

defmodule PulsarEx.Proto.CommandNewTxnResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t()
        }

  defstruct [:request_id, :txnid_least_bits, :txnid_most_bits, :error, :message]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:error, 4, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 5, optional: true, type: :string)
end

defmodule PulsarEx.Proto.CommandAddPartitionToTxn do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          partitions: [String.t()]
        }

  defstruct [:request_id, :txnid_least_bits, :txnid_most_bits, :partitions]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:partitions, 4, repeated: true, type: :string)
end

defmodule PulsarEx.Proto.CommandAddPartitionToTxnResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t()
        }

  defstruct [:request_id, :txnid_least_bits, :txnid_most_bits, :error, :message]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:error, 4, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 5, optional: true, type: :string)
end

defmodule PulsarEx.Proto.Subscription do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          topic: String.t(),
          subscription: String.t()
        }

  defstruct [:topic, :subscription]

  field(:topic, 1, required: true, type: :string)
  field(:subscription, 2, required: true, type: :string)
end

defmodule PulsarEx.Proto.CommandAddSubscriptionToTxn do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          subscription: [PulsarEx.Proto.Subscription.t()]
        }

  defstruct [:request_id, :txnid_least_bits, :txnid_most_bits, :subscription]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:subscription, 4, repeated: true, type: PulsarEx.Proto.Subscription)
end

defmodule PulsarEx.Proto.CommandAddSubscriptionToTxnResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t()
        }

  defstruct [:request_id, :txnid_least_bits, :txnid_most_bits, :error, :message]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:error, 4, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 5, optional: true, type: :string)
end

defmodule PulsarEx.Proto.CommandEndTxn do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          txn_action: PulsarEx.Proto.TxnAction.t()
        }

  defstruct [:request_id, :txnid_least_bits, :txnid_most_bits, :txn_action]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:txn_action, 4, optional: true, type: PulsarEx.Proto.TxnAction, enum: true)
end

defmodule PulsarEx.Proto.CommandEndTxnResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t()
        }

  defstruct [:request_id, :txnid_least_bits, :txnid_most_bits, :error, :message]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:error, 4, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 5, optional: true, type: :string)
end

defmodule PulsarEx.Proto.CommandEndTxnOnPartition do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          topic: String.t(),
          txn_action: PulsarEx.Proto.TxnAction.t(),
          txnid_least_bits_of_low_watermark: non_neg_integer
        }

  defstruct [
    :request_id,
    :txnid_least_bits,
    :txnid_most_bits,
    :topic,
    :txn_action,
    :txnid_least_bits_of_low_watermark
  ]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:topic, 4, optional: true, type: :string)
  field(:txn_action, 5, optional: true, type: PulsarEx.Proto.TxnAction, enum: true)
  field(:txnid_least_bits_of_low_watermark, 6, optional: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandEndTxnOnPartitionResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t()
        }

  defstruct [:request_id, :txnid_least_bits, :txnid_most_bits, :error, :message]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:error, 4, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 5, optional: true, type: :string)
end

defmodule PulsarEx.Proto.CommandEndTxnOnSubscription do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          subscription: PulsarEx.Proto.Subscription.t() | nil,
          txn_action: PulsarEx.Proto.TxnAction.t(),
          txnid_least_bits_of_low_watermark: non_neg_integer
        }

  defstruct [
    :request_id,
    :txnid_least_bits,
    :txnid_most_bits,
    :subscription,
    :txn_action,
    :txnid_least_bits_of_low_watermark
  ]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:subscription, 4, optional: true, type: PulsarEx.Proto.Subscription)
  field(:txn_action, 5, optional: true, type: PulsarEx.Proto.TxnAction, enum: true)
  field(:txnid_least_bits_of_low_watermark, 6, optional: true, type: :uint64)
end

defmodule PulsarEx.Proto.CommandEndTxnOnSubscriptionResponse do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          request_id: non_neg_integer,
          txnid_least_bits: non_neg_integer,
          txnid_most_bits: non_neg_integer,
          error: PulsarEx.Proto.ServerError.t(),
          message: String.t()
        }

  defstruct [:request_id, :txnid_least_bits, :txnid_most_bits, :error, :message]

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:error, 4, optional: true, type: PulsarEx.Proto.ServerError, enum: true)
  field(:message, 5, optional: true, type: :string)
end

defmodule PulsarEx.Proto.BaseCommand do
  @moduledoc false
  use Protobuf, syntax: :proto2

  @type t :: %__MODULE__{
          type: PulsarEx.Proto.BaseCommand.Type.t(),
          connect: PulsarEx.Proto.CommandConnect.t() | nil,
          connected: PulsarEx.Proto.CommandConnected.t() | nil,
          subscribe: PulsarEx.Proto.CommandSubscribe.t() | nil,
          producer: PulsarEx.Proto.CommandProducer.t() | nil,
          send: PulsarEx.Proto.CommandSend.t() | nil,
          send_receipt: PulsarEx.Proto.CommandSendReceipt.t() | nil,
          send_error: PulsarEx.Proto.CommandSendError.t() | nil,
          message: PulsarEx.Proto.CommandMessage.t() | nil,
          ack: PulsarEx.Proto.CommandAck.t() | nil,
          flow: PulsarEx.Proto.CommandFlow.t() | nil,
          unsubscribe: PulsarEx.Proto.CommandUnsubscribe.t() | nil,
          success: PulsarEx.Proto.CommandSuccess.t() | nil,
          error: PulsarEx.Proto.CommandError.t() | nil,
          close_producer: PulsarEx.Proto.CommandCloseProducer.t() | nil,
          close_consumer: PulsarEx.Proto.CommandCloseConsumer.t() | nil,
          producer_success: PulsarEx.Proto.CommandProducerSuccess.t() | nil,
          ping: PulsarEx.Proto.CommandPing.t() | nil,
          pong: PulsarEx.Proto.CommandPong.t() | nil,
          redeliverUnacknowledgedMessages:
            PulsarEx.Proto.CommandRedeliverUnacknowledgedMessages.t() | nil,
          partitionMetadata: PulsarEx.Proto.CommandPartitionedTopicMetadata.t() | nil,
          partitionMetadataResponse:
            PulsarEx.Proto.CommandPartitionedTopicMetadataResponse.t() | nil,
          lookupTopic: PulsarEx.Proto.CommandLookupTopic.t() | nil,
          lookupTopicResponse: PulsarEx.Proto.CommandLookupTopicResponse.t() | nil,
          consumerStats: PulsarEx.Proto.CommandConsumerStats.t() | nil,
          consumerStatsResponse: PulsarEx.Proto.CommandConsumerStatsResponse.t() | nil,
          reachedEndOfTopic: PulsarEx.Proto.CommandReachedEndOfTopic.t() | nil,
          seek: PulsarEx.Proto.CommandSeek.t() | nil,
          getLastMessageId: PulsarEx.Proto.CommandGetLastMessageId.t() | nil,
          getLastMessageIdResponse: PulsarEx.Proto.CommandGetLastMessageIdResponse.t() | nil,
          active_consumer_change: PulsarEx.Proto.CommandActiveConsumerChange.t() | nil,
          getTopicsOfNamespace: PulsarEx.Proto.CommandGetTopicsOfNamespace.t() | nil,
          getTopicsOfNamespaceResponse:
            PulsarEx.Proto.CommandGetTopicsOfNamespaceResponse.t() | nil,
          getSchema: PulsarEx.Proto.CommandGetSchema.t() | nil,
          getSchemaResponse: PulsarEx.Proto.CommandGetSchemaResponse.t() | nil,
          authChallenge: PulsarEx.Proto.CommandAuthChallenge.t() | nil,
          authResponse: PulsarEx.Proto.CommandAuthResponse.t() | nil,
          ackResponse: PulsarEx.Proto.CommandAckResponse.t() | nil,
          getOrCreateSchema: PulsarEx.Proto.CommandGetOrCreateSchema.t() | nil,
          getOrCreateSchemaResponse: PulsarEx.Proto.CommandGetOrCreateSchemaResponse.t() | nil,
          newTxn: PulsarEx.Proto.CommandNewTxn.t() | nil,
          newTxnResponse: PulsarEx.Proto.CommandNewTxnResponse.t() | nil,
          addPartitionToTxn: PulsarEx.Proto.CommandAddPartitionToTxn.t() | nil,
          addPartitionToTxnResponse: PulsarEx.Proto.CommandAddPartitionToTxnResponse.t() | nil,
          addSubscriptionToTxn: PulsarEx.Proto.CommandAddSubscriptionToTxn.t() | nil,
          addSubscriptionToTxnResponse:
            PulsarEx.Proto.CommandAddSubscriptionToTxnResponse.t() | nil,
          endTxn: PulsarEx.Proto.CommandEndTxn.t() | nil,
          endTxnResponse: PulsarEx.Proto.CommandEndTxnResponse.t() | nil,
          endTxnOnPartition: PulsarEx.Proto.CommandEndTxnOnPartition.t() | nil,
          endTxnOnPartitionResponse: PulsarEx.Proto.CommandEndTxnOnPartitionResponse.t() | nil,
          endTxnOnSubscription: PulsarEx.Proto.CommandEndTxnOnSubscription.t() | nil,
          endTxnOnSubscriptionResponse:
            PulsarEx.Proto.CommandEndTxnOnSubscriptionResponse.t() | nil
        }

  defstruct [
    :type,
    :connect,
    :connected,
    :subscribe,
    :producer,
    :send,
    :send_receipt,
    :send_error,
    :message,
    :ack,
    :flow,
    :unsubscribe,
    :success,
    :error,
    :close_producer,
    :close_consumer,
    :producer_success,
    :ping,
    :pong,
    :redeliverUnacknowledgedMessages,
    :partitionMetadata,
    :partitionMetadataResponse,
    :lookupTopic,
    :lookupTopicResponse,
    :consumerStats,
    :consumerStatsResponse,
    :reachedEndOfTopic,
    :seek,
    :getLastMessageId,
    :getLastMessageIdResponse,
    :active_consumer_change,
    :getTopicsOfNamespace,
    :getTopicsOfNamespaceResponse,
    :getSchema,
    :getSchemaResponse,
    :authChallenge,
    :authResponse,
    :ackResponse,
    :getOrCreateSchema,
    :getOrCreateSchemaResponse,
    :newTxn,
    :newTxnResponse,
    :addPartitionToTxn,
    :addPartitionToTxnResponse,
    :addSubscriptionToTxn,
    :addSubscriptionToTxnResponse,
    :endTxn,
    :endTxnResponse,
    :endTxnOnPartition,
    :endTxnOnPartitionResponse,
    :endTxnOnSubscription,
    :endTxnOnSubscriptionResponse
  ]

  field(:type, 1, required: true, type: PulsarEx.Proto.BaseCommand.Type, enum: true)
  field(:connect, 2, optional: true, type: PulsarEx.Proto.CommandConnect)
  field(:connected, 3, optional: true, type: PulsarEx.Proto.CommandConnected)
  field(:subscribe, 4, optional: true, type: PulsarEx.Proto.CommandSubscribe)
  field(:producer, 5, optional: true, type: PulsarEx.Proto.CommandProducer)
  field(:send, 6, optional: true, type: PulsarEx.Proto.CommandSend)
  field(:send_receipt, 7, optional: true, type: PulsarEx.Proto.CommandSendReceipt)
  field(:send_error, 8, optional: true, type: PulsarEx.Proto.CommandSendError)
  field(:message, 9, optional: true, type: PulsarEx.Proto.CommandMessage)
  field(:ack, 10, optional: true, type: PulsarEx.Proto.CommandAck)
  field(:flow, 11, optional: true, type: PulsarEx.Proto.CommandFlow)
  field(:unsubscribe, 12, optional: true, type: PulsarEx.Proto.CommandUnsubscribe)
  field(:success, 13, optional: true, type: PulsarEx.Proto.CommandSuccess)
  field(:error, 14, optional: true, type: PulsarEx.Proto.CommandError)
  field(:close_producer, 15, optional: true, type: PulsarEx.Proto.CommandCloseProducer)
  field(:close_consumer, 16, optional: true, type: PulsarEx.Proto.CommandCloseConsumer)
  field(:producer_success, 17, optional: true, type: PulsarEx.Proto.CommandProducerSuccess)
  field(:ping, 18, optional: true, type: PulsarEx.Proto.CommandPing)
  field(:pong, 19, optional: true, type: PulsarEx.Proto.CommandPong)

  field(:redeliverUnacknowledgedMessages, 20,
    optional: true,
    type: PulsarEx.Proto.CommandRedeliverUnacknowledgedMessages
  )

  field(:partitionMetadata, 21,
    optional: true,
    type: PulsarEx.Proto.CommandPartitionedTopicMetadata
  )

  field(:partitionMetadataResponse, 22,
    optional: true,
    type: PulsarEx.Proto.CommandPartitionedTopicMetadataResponse
  )

  field(:lookupTopic, 23, optional: true, type: PulsarEx.Proto.CommandLookupTopic)
  field(:lookupTopicResponse, 24, optional: true, type: PulsarEx.Proto.CommandLookupTopicResponse)
  field(:consumerStats, 25, optional: true, type: PulsarEx.Proto.CommandConsumerStats)

  field(:consumerStatsResponse, 26,
    optional: true,
    type: PulsarEx.Proto.CommandConsumerStatsResponse
  )

  field(:reachedEndOfTopic, 27, optional: true, type: PulsarEx.Proto.CommandReachedEndOfTopic)
  field(:seek, 28, optional: true, type: PulsarEx.Proto.CommandSeek)
  field(:getLastMessageId, 29, optional: true, type: PulsarEx.Proto.CommandGetLastMessageId)

  field(:getLastMessageIdResponse, 30,
    optional: true,
    type: PulsarEx.Proto.CommandGetLastMessageIdResponse
  )

  field(:active_consumer_change, 31,
    optional: true,
    type: PulsarEx.Proto.CommandActiveConsumerChange
  )

  field(:getTopicsOfNamespace, 32,
    optional: true,
    type: PulsarEx.Proto.CommandGetTopicsOfNamespace
  )

  field(:getTopicsOfNamespaceResponse, 33,
    optional: true,
    type: PulsarEx.Proto.CommandGetTopicsOfNamespaceResponse
  )

  field(:getSchema, 34, optional: true, type: PulsarEx.Proto.CommandGetSchema)
  field(:getSchemaResponse, 35, optional: true, type: PulsarEx.Proto.CommandGetSchemaResponse)
  field(:authChallenge, 36, optional: true, type: PulsarEx.Proto.CommandAuthChallenge)
  field(:authResponse, 37, optional: true, type: PulsarEx.Proto.CommandAuthResponse)
  field(:ackResponse, 38, optional: true, type: PulsarEx.Proto.CommandAckResponse)
  field(:getOrCreateSchema, 39, optional: true, type: PulsarEx.Proto.CommandGetOrCreateSchema)

  field(:getOrCreateSchemaResponse, 40,
    optional: true,
    type: PulsarEx.Proto.CommandGetOrCreateSchemaResponse
  )

  field(:newTxn, 50, optional: true, type: PulsarEx.Proto.CommandNewTxn)
  field(:newTxnResponse, 51, optional: true, type: PulsarEx.Proto.CommandNewTxnResponse)
  field(:addPartitionToTxn, 52, optional: true, type: PulsarEx.Proto.CommandAddPartitionToTxn)

  field(:addPartitionToTxnResponse, 53,
    optional: true,
    type: PulsarEx.Proto.CommandAddPartitionToTxnResponse
  )

  field(:addSubscriptionToTxn, 54,
    optional: true,
    type: PulsarEx.Proto.CommandAddSubscriptionToTxn
  )

  field(:addSubscriptionToTxnResponse, 55,
    optional: true,
    type: PulsarEx.Proto.CommandAddSubscriptionToTxnResponse
  )

  field(:endTxn, 56, optional: true, type: PulsarEx.Proto.CommandEndTxn)
  field(:endTxnResponse, 57, optional: true, type: PulsarEx.Proto.CommandEndTxnResponse)
  field(:endTxnOnPartition, 58, optional: true, type: PulsarEx.Proto.CommandEndTxnOnPartition)

  field(:endTxnOnPartitionResponse, 59,
    optional: true,
    type: PulsarEx.Proto.CommandEndTxnOnPartitionResponse
  )

  field(:endTxnOnSubscription, 60,
    optional: true,
    type: PulsarEx.Proto.CommandEndTxnOnSubscription
  )

  field(:endTxnOnSubscriptionResponse, 61,
    optional: true,
    type: PulsarEx.Proto.CommandEndTxnOnSubscriptionResponse
  )
end

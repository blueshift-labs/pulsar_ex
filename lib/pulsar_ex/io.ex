defmodule PulsarEx.IO do
  alias PulsarEx.{Proto, ProducerMessage, ConsumerMessage}

  alias PulsarEx.Bitset

  @magic_number 3585

  def ack_type(:individual), do: :Individual
  def ack_type(:cumulative), do: :Cumulative
  def ack_type(type), do: type

  def subscription_type(:exclusive), do: :Exclusive
  def subscription_type(:shared), do: :Shared
  def subscription_type(:failover), do: :Failover
  def subscription_type(:key_shared), do: :Key_Shared
  def subscription_type(type), do: type

  def initial_position(:latest), do: :Latest
  def initial_position(:earliest), do: :Earliest
  def initial_position(pos), do: pos

  def producer_access_mode(:shared), do: :Shared
  def producer_access_mode(:exclusive), do: :Exclusive
  def producer_access_mode(:wait_for_exclusive), do: :WaitForExclusive
  def producer_access_mode(mode), do: mode

  # [total_size][command_size][command]
  def encode_command(%Proto.BaseCommand{} = command) do
    encoded_command = Proto.BaseCommand.encode(command)
    command_size = byte_size(encoded_command)
    total_size = 4 + command_size

    <<total_size::32, command_size::32, encoded_command::binary>>
  end

  def encode_command(command), do: to_base_command(command) |> encode_command()

  # [total_size][command_size][command][magic_number][checksum][metadata_size][metadata][payload]
  def encode_message(
        %ProducerMessage{
          producer_id: producer_id,
          producer_name: producer_name,
          sequence_id: sequence_id,
          payload: payload
        } = message
      ) do
    command =
      Proto.CommandSend.new(
        producer_id: producer_id,
        sequence_id: sequence_id,
        num_messages: nil,
        txnid_least_bits: nil,
        txnid_most_bits: nil
      )
      |> to_base_command()

    metadata =
      Proto.MessageMetadata.new(
        producer_name: producer_name,
        sequence_id: sequence_id,
        publish_time: :os.system_time(:millisecond),
        properties: to_kv(message.properties),
        partition_key: message.partition_key,
        event_time: to_timestamp(message.event_time),
        ordering_key: message.ordering_key,
        deliver_at_time: to_timestamp(message.deliver_at_time),
        uncompressed_size: byte_size(payload),
        num_messages_in_batch: nil,
        txnid_least_bits: nil,
        txnid_most_bits: nil
      )

    encoded_command = Proto.BaseCommand.encode(command)
    command_size = byte_size(encoded_command)

    encoded_metadata = Proto.MessageMetadata.encode(metadata)
    metadata_size = byte_size(encoded_metadata)

    checksum = :crc32cer.nif(<<metadata_size::32, encoded_metadata::binary, payload::binary>>)

    total_size = 4 + command_size + 2 + 4 + 4 + metadata_size + byte_size(payload)

    <<total_size::32, command_size::32, encoded_command::binary, @magic_number::16, checksum::32,
      metadata_size::32, encoded_metadata::binary, payload::binary>>
  end

  # [total_size][command_size][command][magic_number][checksum][metadata_size][metadata][single_metadata_size][single_metadata][payload]...
  def encode_messages(
        [
          %ProducerMessage{
            producer_id: producer_id,
            producer_name: producer_name,
            sequence_id: sequence_id
          }
          | _
        ] = messages
      ) do
    payload = messages |> Enum.map(&encode_single_message/1) |> :erlang.iolist_to_binary()

    command =
      Proto.CommandSend.new(
        producer_id: producer_id,
        sequence_id: sequence_id,
        num_messages: length(messages),
        txnid_least_bits: nil,
        txnid_most_bits: nil
      )
      |> to_base_command()

    metadata =
      Proto.MessageMetadata.new(
        producer_name: producer_name,
        sequence_id: sequence_id,
        publish_time: :os.system_time(:millisecond),
        uncompressed_size: byte_size(payload),
        num_messages_in_batch: length(messages),
        txnid_least_bits: nil,
        txnid_most_bits: nil
      )

    encoded_command = Proto.BaseCommand.encode(command)
    command_size = byte_size(encoded_command)

    encoded_metadata = Proto.MessageMetadata.encode(metadata)
    metadata_size = byte_size(encoded_metadata)

    checksum = :crc32cer.nif(<<metadata_size::32, encoded_metadata::binary, payload::binary>>)

    total_size = 4 + command_size + 2 + 4 + 4 + metadata_size + byte_size(payload)

    <<total_size::32, command_size::32, encoded_command::binary, @magic_number::16, checksum::32,
      metadata_size::32, encoded_metadata::binary, payload::binary>>
  end

  def encode_single_message(
        %ProducerMessage{sequence_id: sequence_id, payload: payload} = message
      ) do
    metadata =
      Proto.SingleMessageMetadata.new(
        sequence_id: sequence_id,
        partition_key: message.partition_key,
        ordering_key: message.ordering_key,
        event_time: to_timestamp(message.event_time),
        properties: to_kv(message.properties),
        payload_size: byte_size(payload)
      )

    encoded_metadata = Proto.SingleMessageMetadata.encode(metadata)
    metadata_size = byte_size(encoded_metadata)

    <<metadata_size::32, encoded_metadata::binary, payload::binary>>
  end

  def decode(data), do: decode(data, [])

  def decode(buffer, messages) when byte_size(buffer) < 4, do: {Enum.reverse(messages), buffer}

  def decode(<<total_size::32, data::binary>> = buffer, messages)
      when total_size > byte_size(data) do
    {Enum.reverse(messages), buffer}
  end

  def decode(<<total_size::32, data::binary>>, messages) do
    <<packet::binary-size(total_size), data::binary>> = data
    <<command_size::32, packet::binary>> = packet
    <<encoded_command::binary-size(command_size), payload::binary>> = packet
    command = Proto.BaseCommand.decode(encoded_command) |> to_inner_command()
    decode(data, [{command, decode_payload(command, payload)} | messages])
  end

  def decode_payload(_, <<>>), do: []

  def decode_payload(command, <<@magic_number::16, checksum::32, meta_payload::binary>>) do
    cond do
      checksum != :crc32cer.nif(meta_payload) ->
        []

      true ->
        <<metadata_size::32, meta_payload::binary>> = meta_payload
        <<encoded_metadata::binary-size(metadata_size), payload::binary>> = meta_payload
        metadata = Proto.MessageMetadata.decode(encoded_metadata)
        num_messages = metadata.num_messages_in_batch

        cond do
          num_messages == nil ->
            [to_consumer_message(command, metadata, payload)]

          true ->
            decode_payload(command, metadata, payload)
        end
    end
  end

  def decode_payload(command, metadata, data), do: decode_payload(command, metadata, data, 0, [])

  def decode_payload(_, _, <<>>, _, messages), do: Enum.reverse(messages)

  def decode_payload(
        command,
        %Proto.MessageMetadata{} = metadata,
        <<metadata_size::32, data::binary>>,
        batch_index,
        messages
      ) do
    <<encoded_metadata::binary-size(metadata_size), data::binary>> = data
    single_meta = Proto.SingleMessageMetadata.decode(encoded_metadata)
    payload_size = single_meta.payload_size
    <<payload::binary-size(payload_size), data::binary>> = data
    message = to_consumer_message(command, metadata, single_meta, batch_index, payload)

    case message do
      nil -> decode_payload(command, metadata, data, batch_index + 1, messages)
      _ -> decode_payload(command, metadata, data, batch_index + 1, [message | messages])
    end
  end

  def to_consumer_message(
        %Proto.CommandMessage{} = command,
        %Proto.MessageMetadata{} = metadata,
        payload
      ) do
    %ConsumerMessage{
      message_id: command.message_id,
      compacted_out: false,
      batch_acked: false,
      batch_index: 0,
      batch_size: 1,
      ack_set: [],
      producer_name: metadata.producer_name,
      sequence_id: metadata.sequence_id,
      publish_time: from_timestamp(metadata.publish_time),
      properties: from_kv(metadata.properties),
      partition_key: metadata.partition_key,
      event_time: from_timestamp(metadata.event_time),
      ordering_key: metadata.ordering_key,
      deliver_at_time: from_timestamp(metadata.deliver_at_time),
      redelivery_count: command.redelivery_count,
      payload: payload
    }
  end

  def to_consumer_message(
        %Proto.CommandMessage{ack_set: []} = command,
        %Proto.MessageMetadata{} = metadata,
        %Proto.SingleMessageMetadata{} = single_meta,
        batch_index,
        payload
      ) do
    ack_set =
      Bitset.new(metadata.num_messages_in_batch)
      |> Bitset.set(batch_index)
      |> Bitset.flip()
      |> Bitset.to_words()

    %ConsumerMessage{
      message_id: command.message_id,
      compacted_out: single_meta.compacted_out,
      batch_acked: false,
      batch_index: batch_index,
      batch_size: metadata.num_messages_in_batch,
      ack_set: ack_set,
      producer_name: metadata.producer_name,
      sequence_id: single_meta.sequence_id,
      publish_time: from_timestamp(metadata.publish_time),
      properties: from_kv(single_meta.properties),
      partition_key: single_meta.partition_key,
      event_time: from_timestamp(single_meta.event_time),
      ordering_key: single_meta.ordering_key,
      redelivery_count: command.redelivery_count,
      payload: payload
    }
  end

  def to_consumer_message(
        %Proto.CommandMessage{ack_set: ack_set} = command,
        %Proto.MessageMetadata{} = metadata,
        %Proto.SingleMessageMetadata{} = single_meta,
        batch_index,
        payload
      ) do
    bitset = Bitset.from_words(ack_set, metadata.num_messages_in_batch)

    ack_set =
      Bitset.new(metadata.num_messages_in_batch)
      |> Bitset.set(batch_index)
      |> Bitset.flip()
      |> Bitset.to_words()

    %ConsumerMessage{
      message_id: command.message_id,
      compacted_out: single_meta.compacted_out,
      batch_acked: !Bitset.set?(bitset, batch_index),
      batch_index: batch_index,
      batch_size: metadata.num_messages_in_batch,
      ack_set: ack_set,
      producer_name: metadata.producer_name,
      sequence_id: single_meta.sequence_id,
      publish_time: from_timestamp(metadata.publish_time),
      properties: from_kv(single_meta.properties),
      partition_key: single_meta.partition_key,
      event_time: from_timestamp(single_meta.event_time),
      ordering_key: single_meta.ordering_key,
      redelivery_count: command.redelivery_count,
      payload: payload
    }
  end

  def to_timestamp(%DateTime{} = ts), do: DateTime.to_unix(ts, :millisecond)
  def to_timestamp(ts), do: ts

  def from_timestamp(nil), do: nil
  def from_timestamp(0), do: nil
  def from_timestamp(ts) when is_integer(ts), do: Timex.from_unix(ts, :millisecond)

  def to_kv(nil), do: []
  def to_kv(properties) when is_list(properties), do: to_kv(properties |> Enum.into(%{}))

  def to_kv(properties) when is_map(properties) do
    properties
    |> Enum.reduce([], fn {k, v}, acc ->
      [Proto.KeyValue.new(key: "#{k}", value: "#{v}") | acc]
    end)
  end

  def from_kv(nil), do: %{}

  def from_kv(metadata) when is_list(metadata) do
    metadata
    |> Enum.reduce(%{}, fn %Proto.KeyValue{key: k, value: v}, acc ->
      Map.put(acc, k, v)
    end)
  end

  def to_base_command(%Proto.BaseCommand{} = command), do: command

  def to_base_command(%Proto.CommandConnect{} = command),
    do: Proto.BaseCommand.new(type: :CONNECT, connect: command)

  def to_base_command(%Proto.CommandConnected{} = command),
    do: Proto.BaseCommand.new(type: :CONNECTED, connected: command)

  def to_base_command(%Proto.CommandSubscribe{} = command),
    do: Proto.BaseCommand.new(type: :SUBSCRIBE, subscribe: command)

  def to_base_command(%Proto.CommandProducer{} = command),
    do: Proto.BaseCommand.new(type: :PRODUCER, producer: command)

  def to_base_command(%Proto.CommandSend{} = command),
    do: Proto.BaseCommand.new(type: :SEND, send: command)

  def to_base_command(%Proto.CommandSendReceipt{} = command),
    do: Proto.BaseCommand.new(type: :SEND_RECEIPT, send_receipt: command)

  def to_base_command(%Proto.CommandSendError{} = command),
    do: Proto.BaseCommand.new(type: :SEND_ERROR, send_error: command)

  def to_base_command(%Proto.CommandMessage{} = command),
    do: Proto.BaseCommand.new(type: :MESSAGE, message: command)

  def to_base_command(%Proto.CommandAck{} = command),
    do: Proto.BaseCommand.new(type: :ACK, ack: command)

  def to_base_command(%Proto.CommandFlow{} = command),
    do: Proto.BaseCommand.new(type: :FLOW, flow: command)

  def to_base_command(%Proto.CommandUnsubscribe{} = command),
    do: Proto.BaseCommand.new(type: :UNSUBSCRIBE, unsubscribe: command)

  def to_base_command(%Proto.CommandSuccess{} = command),
    do: Proto.BaseCommand.new(type: :SUCCESS, success: command)

  def to_base_command(%Proto.CommandError{} = command),
    do: Proto.BaseCommand.new(type: :ERROR, error: command)

  def to_base_command(%Proto.CommandCloseProducer{} = command),
    do: Proto.BaseCommand.new(type: :CLOSE_PRODUCER, close_producer: command)

  def to_base_command(%Proto.CommandCloseConsumer{} = command),
    do: Proto.BaseCommand.new(type: :CLOSE_CONSUMER, close_consumer: command)

  def to_base_command(%Proto.CommandProducerSuccess{} = command),
    do: Proto.BaseCommand.new(type: :PRODUCER_SUCCESS, producer_success: command)

  def to_base_command(%Proto.CommandPing{} = command),
    do: Proto.BaseCommand.new(type: :PING, ping: command)

  def to_base_command(%Proto.CommandPong{} = command),
    do: Proto.BaseCommand.new(type: :PONG, pong: command)

  def to_base_command(%Proto.CommandRedeliverUnacknowledgedMessages{} = command),
    do:
      Proto.BaseCommand.new(
        type: :REDELIVER_UNACKNOWLEDGED_MESSAGES,
        redeliverUnacknowledgedMessages: command
      )

  def to_base_command(%Proto.CommandPartitionedTopicMetadata{} = command),
    do: Proto.BaseCommand.new(type: :PARTITIONED_METADATA, partitionMetadata: command)

  def to_base_command(%Proto.CommandPartitionedTopicMetadataResponse{} = command),
    do:
      Proto.BaseCommand.new(
        type: :PARTITIONED_METADATA_RESPONSE,
        partitionMetadataResponse: command
      )

  def to_base_command(%Proto.CommandLookupTopic{} = command),
    do: Proto.BaseCommand.new(type: :LOOKUP, lookupTopic: command)

  def to_base_command(%Proto.CommandLookupTopicResponse{} = command),
    do: Proto.BaseCommand.new(type: :LOOKUP_RESPONSE, lookupTopicResponse: command)

  def to_base_command(%Proto.CommandConsumerStats{} = command),
    do: Proto.BaseCommand.new(type: :CONSUMER_STATS, consumerStats: command)

  def to_base_command(%Proto.CommandConsumerStatsResponse{} = command),
    do: Proto.BaseCommand.new(type: :CONSUMER_STATS_RESPONSE, consumerStatsResponse: command)

  def to_base_command(%Proto.CommandReachedEndOfTopic{} = command),
    do: Proto.BaseCommand.new(type: :REACHED_END_OF_TOPIC, reachedEndOfTopic: command)

  def to_base_command(%Proto.CommandSeek{} = command),
    do: Proto.BaseCommand.new(type: :SEEK, seek: command)

  def to_base_command(%Proto.CommandGetLastMessageId{} = command),
    do: Proto.BaseCommand.new(type: :GET_LAST_MESSAGE_ID, getLastMessageId: command)

  def to_base_command(%Proto.CommandGetLastMessageIdResponse{} = command),
    do:
      Proto.BaseCommand.new(
        type: :GET_LAST_MESSAGE_ID_RESPONSE,
        getLastMessageIdResponse: command
      )

  def to_base_command(%Proto.CommandActiveConsumerChange{} = command),
    do: Proto.BaseCommand.new(type: :ACTIVE_CONSUMER_CHANGE, active_consumer_change: command)

  def to_base_command(%Proto.CommandGetTopicsOfNamespace{} = command),
    do: Proto.BaseCommand.new(type: :GET_TOPICS_OF_NAMESPACE, getTopicsOfNamespace: command)

  def to_base_command(%Proto.CommandGetTopicsOfNamespaceResponse{} = command),
    do:
      Proto.BaseCommand.new(
        type: :GET_TOPICS_OF_NAMESPACE_RESPONSE,
        getTopicsOfNamespaceResponse: command
      )

  def to_base_command(%Proto.CommandGetSchema{} = command),
    do: Proto.BaseCommand.new(type: :GET_SCHEMA, getSchema: command)

  def to_base_command(%Proto.CommandGetSchemaResponse{} = command),
    do: Proto.BaseCommand.new(type: :GET_SCHEMA_RESPONSE, getSchemaResponse: command)

  def to_base_command(%Proto.CommandAuthChallenge{} = command),
    do: Proto.BaseCommand.new(type: :AUTH_CHALLENGE, authChallenge: command)

  def to_base_command(%Proto.CommandAuthResponse{} = command),
    do: Proto.BaseCommand.new(type: :AUTH_RESPONSE, authResponse: command)

  def to_base_command(%Proto.CommandAckResponse{} = command),
    do: Proto.BaseCommand.new(type: :ACK_RESPONSE, ackResponse: command)

  def to_base_command(%Proto.CommandGetOrCreateSchema{} = command),
    do: Proto.BaseCommand.new(type: :GET_OR_CREATE_SCHEMA, getOrCreateSchema: command)

  def to_base_command(%Proto.CommandGetOrCreateSchemaResponse{} = command),
    do:
      Proto.BaseCommand.new(
        type: :GET_OR_CREATE_SCHEMA_RESPONSE,
        getOrCreateSchemaResponse: command
      )

  def to_base_command(%Proto.CommandNewTxn{} = command),
    do: Proto.BaseCommand.new(type: :NEW_TXN, newTxn: command)

  def to_base_command(%Proto.CommandNewTxnResponse{} = command),
    do: Proto.BaseCommand.new(type: :NEW_TXN_RESPONSE, newTxnResponse: command)

  def to_base_command(%Proto.CommandAddPartitionToTxn{} = command),
    do: Proto.BaseCommand.new(type: :ADD_PARTITION_TO_TXN, addPartitionToTxn: command)

  def to_base_command(%Proto.CommandAddPartitionToTxnResponse{} = command),
    do:
      Proto.BaseCommand.new(
        type: :ADD_PARTITION_TO_TXN_RESPONSE,
        addPartitionToTxnResponse: command
      )

  def to_base_command(%Proto.CommandAddSubscriptionToTxn{} = command),
    do: Proto.BaseCommand.new(type: :ADD_SUBSCRIPTION_TO_TXN, addSubscriptionToTxn: command)

  def to_base_command(%Proto.CommandAddSubscriptionToTxnResponse{} = command),
    do:
      Proto.BaseCommand.new(
        type: :ADD_SUBSCRIPTION_TO_TXN_RESPONSE,
        addSubscriptionToTxnResponse: command
      )

  def to_base_command(%Proto.CommandEndTxn{} = command),
    do: Proto.BaseCommand.new(type: :END_TXN, endTxn: command)

  def to_base_command(%Proto.CommandEndTxnResponse{} = command),
    do: Proto.BaseCommand.new(type: :END_TXN_RESPONSE, endTxnResponse: command)

  def to_base_command(%Proto.CommandEndTxnOnPartition{} = command),
    do: Proto.BaseCommand.new(type: :END_TXN_ON_PARTITION, endTxnOnPartition: command)

  def to_base_command(%Proto.CommandEndTxnOnPartitionResponse{} = command),
    do:
      Proto.BaseCommand.new(
        type: :END_TXN_ON_PARTITION_RESPONSE,
        endTxnOnPartitionResponse: command
      )

  def to_base_command(%Proto.CommandEndTxnOnSubscription{} = command),
    do: Proto.BaseCommand.new(type: :END_TXN_ON_SUBSCRIPTION, endTxnOnSubscription: command)

  def to_base_command(%Proto.CommandEndTxnOnSubscriptionResponse{} = command),
    do:
      Proto.BaseCommand.new(
        type: :END_TXN_ON_SUBSCRIPTION_RESPONSE,
        endTxnOnSubscriptionResponse: command
      )

  def to_inner_command(%Proto.BaseCommand{type: :CONNECT, connect: command}), do: command
  def to_inner_command(%Proto.BaseCommand{type: :CONNECTED, connected: command}), do: command
  def to_inner_command(%Proto.BaseCommand{type: :SUBSCRIBE, subscribe: command}), do: command
  def to_inner_command(%Proto.BaseCommand{type: :PRODUCER, producer: command}), do: command
  def to_inner_command(%Proto.BaseCommand{type: :SEND, send: command}), do: command

  def to_inner_command(%Proto.BaseCommand{type: :SEND_RECEIPT, send_receipt: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{type: :SEND_ERROR, send_error: command}), do: command
  def to_inner_command(%Proto.BaseCommand{type: :MESSAGE, message: command}), do: command
  def to_inner_command(%Proto.BaseCommand{type: :ACK, ack: command}), do: command
  def to_inner_command(%Proto.BaseCommand{type: :FLOW, flow: command}), do: command

  def to_inner_command(%Proto.BaseCommand{type: :UNSUBSCRIBE, unsubscribe: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{type: :SUCCESS, success: command}), do: command
  def to_inner_command(%Proto.BaseCommand{type: :ERROR, error: command}), do: command

  def to_inner_command(%Proto.BaseCommand{type: :CLOSE_PRODUCER, close_producer: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{type: :CLOSE_CONSUMER, close_consumer: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{type: :PRODUCER_SUCCESS, producer_success: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{type: :PING, ping: command}), do: command
  def to_inner_command(%Proto.BaseCommand{type: :PONG, pong: command}), do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :REDELIVER_UNACKNOWLEDGED_MESSAGES,
        redeliverUnacknowledgedMessages: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :PARTITIONED_METADATA,
        partitionMetadata: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :PARTITIONED_METADATA_RESPONSE,
        partitionMetadataResponse: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{type: :LOOKUP, lookupTopic: command}), do: command

  def to_inner_command(%Proto.BaseCommand{type: :LOOKUP_RESPONSE, lookupTopicResponse: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{type: :CONSUMER_STATS, consumerStats: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :CONSUMER_STATS_RESPONSE,
        consumerStatsResponse: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :REACHED_END_OF_TOPIC,
        reachedEndOfTopic: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{type: :SEEK, seek: command}), do: command

  def to_inner_command(%Proto.BaseCommand{type: :GET_LAST_MESSAGE_ID, getLastMessageId: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :GET_LAST_MESSAGE_ID_RESPONSE,
        getLastMessageIdResponse: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :ACTIVE_CONSUMER_CHANGE,
        active_consumer_change: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :GET_TOPICS_OF_NAMESPACE,
        getTopicsOfNamespace: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :GET_TOPICS_OF_NAMESPACE_RESPONSE,
        getTopicsOfNamespaceResponse: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{type: :GET_SCHEMA, getSchema: command}), do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :GET_SCHEMA_RESPONSE,
        getSchemaResponse: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{type: :AUTH_CHALLENGE, authChallenge: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{type: :AUTH_RESPONSE, authResponse: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{type: :ACK_RESPONSE, ackResponse: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :GET_OR_CREATE_SCHEMA,
        getOrCreateSchema: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :GET_OR_CREATE_SCHEMA_RESPONSE,
        getOrCreateSchemaResponse: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{type: :NEW_TXN, newTxn: command}), do: command

  def to_inner_command(%Proto.BaseCommand{type: :NEW_TXN_RESPONSE, newTxnResponse: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :ADD_PARTITION_TO_TXN,
        addPartitionToTxn: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :ADD_PARTITION_TO_TXN_RESPONSE,
        addPartitionToTxnResponse: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :ADD_SUBSCRIPTION_TO_TXN,
        addSubscriptionToTxn: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :ADD_SUBSCRIPTION_TO_TXN_RESPONSE,
        addSubscriptionToTxnResponse: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{type: :END_TXN, endTxn: command}), do: command

  def to_inner_command(%Proto.BaseCommand{type: :END_TXN_RESPONSE, endTxnResponse: command}),
    do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :END_TXN_ON_PARTITION,
        endTxnOnPartition: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :END_TXN_ON_PARTITION_RESPONSE,
        endTxnOnPartitionResponse: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :END_TXN_ON_SUBSCRIPTION,
        endTxnOnSubscription: command
      }),
      do: command

  def to_inner_command(%Proto.BaseCommand{
        type: :END_TXN_ON_SUBSCRIPTION_RESPONSE,
        endTxnOnSubscriptionResponse: command
      }),
      do: command

  def to_inner_command(command), do: command
end

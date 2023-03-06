defmodule PulsarEx.Message do
  @enforce_keys [
    :topic,
    :payload
  ]

  defstruct [
    :topic,
    :payload,
    :properties,
    :partition_key,
    :ordering_key,
    :event_time,
    :deliver_at_time
  ]
end

defmodule PulsarEx.ProducerMessage do
  @enforce_keys [
    :producer_id,
    :producer_name,
    :sequence_id,
    :payload
  ]

  defstruct [
    :topic,
    :producer_id,
    :producer_name,
    :sequence_id,
    :payload,
    :properties,
    :partition_key,
    :ordering_key,
    :event_time,
    :deliver_at_time,
    :compression
  ]
end

defmodule PulsarEx.ConsumerMessage do
  @enforce_keys [
    :message_id,
    :batch_index,
    :batch_size,
    :redelivery_count,
    :producer_name,
    :sequence_id,
    :publish_time,
    :payload
  ]

  defstruct [
    :topic,
    :message_id,
    :batch_index,
    :batch_size,
    :redelivery_count,
    :producer_name,
    :sequence_id,
    :publish_time,
    :payload,
    :properties,
    :partition_key,
    :ordering_key,
    :event_time,
    :deliver_at_time
  ]
end

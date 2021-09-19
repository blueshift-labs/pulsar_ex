defmodule PulsarEx.ProducerMessage do
  @enforce_keys [:payload]
  defstruct [
    :producer_id,
    :sequence_id,
    :producer_name,
    :properties,
    :partition_key,
    :ordering_key,
    :event_time,
    :deliver_at_time,
    :payload
  ]
end

defmodule PulsarEx.ConsumerMessage do
  @enforce_keys [
    :message_id,
    :compacted_out,
    :batch_acked,
    :redelivery_count,
    :producer_name,
    :sequence_id,
    :publish_time,
    :payload
  ]
  defstruct [
    :message_id,
    :compacted_out,
    :batch_acked,
    :redelivery_count,
    :producer_name,
    :sequence_id,
    :publish_time,
    :properties,
    :partition_key,
    :event_time,
    :ordering_key,
    :deliver_at_time,
    :payload,
    :consumer
  ]

  alias PulsarEx.{ConsumerMessage, Consumer}

  def ack(%ConsumerMessage{consumer: consumer} = message) when not is_nil(consumer) do
    Consumer.ack(consumer, message)
  end

  def nack(%ConsumerMessage{consumer: consumer} = message) when not is_nil(consumer) do
    Consumer.nack(consumer, message)
  end
end

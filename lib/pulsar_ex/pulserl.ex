require RecStruct
import RecStruct

defheader PulsarEx.Pulserl, "include/pulserl.hrl" do
  defrecstruct(Topic, :topic)
  defrecstruct(Batch, :batch)
  defrecstruct(MessageID, :messageId)
  defrecstruct(ProducerMessage, :producerMessage)
  defrecstruct(ConsumerMessage, :consumerMessage)
end

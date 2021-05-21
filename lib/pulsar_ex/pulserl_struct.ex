defmodule Pulserl do
  require RecStruct
  import RecStruct

  defheader Header, "include/pulserl.hrl" do
    defrecstruct(Topic, :topic)
    defrecstruct(Batch, :batch)
    defrecstruct(MessageID, :messageId)
    defrecstruct(ProducerMessage, :producerMessage)
    defrecstruct(ConsumerMessage, :consumerMessage)
  end
end

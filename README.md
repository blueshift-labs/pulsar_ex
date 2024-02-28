# PulsarEx

A pulsar client implemented purely in elixir.

## Installation

```elixir
def deps do
  [
    {:pulsar_ex, "~> 0.14"}
  ]
end
```

## Features
* Topic discovery on partition changes
* Graceful shutdown for both producer and consumer
* compressed message payload on producer - lz4, zlib, zstd, snappy
* Batched producer
* Consumer with batch index support
* Producer Retry Strategy
* Consumer Workers with rich features
* Job Interceptors
* Job Middlewares
* Verbose Logging and Telemetry
* Multiple Clusters
* Regex Based Consumer

## Producer
### Basic Usage
```elixir
PulsarEx.produce("persistent://public/default/test.json", "payload")
```
This creates a producer on the fly with default options, check `PulsarEx.Producer`
### Produce with message options
```elixir
PulsarEx.produce("persistent://public/default/test.json", "payload", [properties: %{prop1: "1", prop2: "2", partition_key: "123", ordering_key: "321", event_time: Timex.now()}])
```
### Produce a message with delayed delivery - 5 seconds
```elixir
PulsarEx.produce("persistent://public/default/test.json", "payload", [delay: 5_000])
```
### Produce a message with delayed delivery, at a specific time
```elixir
deliver_at = Timex.add(Timex.now(), Timex.Duration.from_milliseconds(10_000))
PulsarEx.produce("persistent://public/default/test.json", "payload", [deliver_at_time: deliver_at])
```
### Increase pool size of producers
All the producers are pooled, by default the pool size is 1, and to create a pool of producers 5 producers
```elixir
PulsarEx.produce("persistent://public/default/test.json", "payload", [], [num_producers: 5])
```
**Note: the pool is per partition, so if you have a partitioned topics with 10 partitions, you will get total of 10 pools, each with 5 producers. Messages are routed to partitions based on partition_key, or in a round-robin fashion if no partition key is specified.
If the topic already exists on pulsar, PulsarEx will lookup the topic and find out how many partitions the topic has. PulsarEx also periodically detects if there are more partitions added to the topic, so no application restart is needed.**

### Batched Producer
By default, messages are not batched when publishing to pulsar. To enable batch, and change batch size
```elixir
PulsarEx.produce("persistent://public/default/test.json", "payload", [], [batch_enabled: true, batch_size: 100])
```
You can also change the flush interval for batched producing
```elixir
PulsarEx.produce("persistent://public/default/test.json", "payload", [], [batch_enabled: true, batch_size: 100, flush_interval: 1_000])
```
### Compress the messages
First, you will need to add `nimble_lz4` to your deps
```elixir
defp deps do
  {:nimble_lz4, "~> 0.1.2"}
end
```
And then
```elixir
PulsarEx.produce("persistent://public/default/test.json", "payload", [], [compression: :lz4])
```

## Consumer
### Create a raw consumer
```elixir
def MyConsumer do
  use PulsarEx.Consumer

  @impl ConsumerCallback
  def handle_messages(messages, _state) do
    Enum.map(messages, &Logger.info/1)
  end
end
```
And then
```elixir
MyConsumer.start()
```
To see the supported options, check `PulsarEx.Consumer`.

**Note: Raw consumers always handle messages in batches, so the `handle_messages` callback expects a list of messages. However, batch_size can be specified at time of starting the consumer**
```elixir
MyConsumer.start(batch_size: 1)
```
And then you can simply do:
```elixir
@impl PulsarEx.ConsumerCallback
def handle_messages([message], _state) do
  IO.inspect(message)
  [:ok]
end
```
**Important Note: to instruct the consumer to send ACK or NACK, PulsarEx.Consumer relies on the returned value of your handle_messages callback. This callback expects a list of 3 possible values: :ok | {:ok, any()} | {:error, any()}, see `PulsarEx.ConsumerCallback`**

You can also specify the consumer options in the definition of your consumer:
```elixir
def MyConsumer do
  use PulsarEx.Consumer,
    batch_size: 1,
    subscription: "test",
    subscription_type: :failover,
    receiving_queue_size: 1000,
    redelivery_policy: :exp,
    max_redelivery_attempts: 5,
    dead_letter_topic: "persistent://public/default/dlq",
    dead_letter_producer_opts: [num_producers: 3]

  @impl ConsumerCallback
  def handle_messages(messages, _state) do
    Enum.map(messages, &Logger.info/1)
  end
end
```

### PulsarEx.Worker
#### Basic Usages
If you are looking for using PulsarEx as a job queue. You should use `PulsarEx.Worker`, instead of `PulsarEx.Consumer`. The worker module provides a rich set of tools and functions, as well as logging & stats for you to work with pulsar topics as your job queue. And the worker module also supports all the options that are supported by the `PulsarEx.Consumer` module, except for the `batch_size`, that the worker module only processes 1 message at a time.

See `PulsarEx.WorkerCallback` for the callbacks expected to implement your worker.

```elixir
defmodule TestWorker do
  use PulsarEx.Worker,
    otp_app: :my_app,
    topic: "persistent://public/default/test.json",
    subscription: "test",
    subscription_type: :failover,
    receiving_queue_size: 1000,
    redelivery_policy: :exp,
    max_redelivery_attempts: 5,
    dead_letter_topic: "persistent://public/default/dlq",
    dead_letter_producer_opts: [num_producers: 3]

  def handle_job(%JobState{} = message) do
    IO.inspect(message)
    :ok
  end
end
```

**Note: while in `PulsarEx.Consumer`, you are handling a list of `PulsarEx.ConsumerMessage`, in the PulsarEx.Worker, you are handling a struct that's wrapping the `PulsarEx.ConsumerMessage`, which is the `PulsarEx.JobState`**

#### Middlewares

`PulsarEx.JobState` is useful for writing consumer middlewares, we have two middlewares that are loaded by default for each worker:
`PulsarEx.Middlewares.Logging` and `PulsarEx.Middlewares.Telemetry`

To write your own middleware and load it into your worker:

```elixir
defmodule MyWorker.Middlewares.Test do
  @behaviour PulsarEx.Middleware

  import PulsarEx.JobState

  @impl true
  def call(handler) do
    fn %PulsarEx.JobState{} = job_state ->
      job_state
      |> assign(:property1, "My test middleware")
      |> handler.()
    end
  end
end
```
And then
```elixir
defmodule MyWorker do
  use PulsarEx.Worker,
    otp_app: :my_app,
    middlewares: [
      MyWorker.Middlewares.Test
    ]
  ...
end
```

Middlewares are useful especially when you have multiple workers that share the same execution logic, such as logging, stats, exception handling, etc.

#### Enqueue a job for your worker
At the very basic level, you can do
```elixir
MyWorker.enqueue(%{"my_job_context" => 123} = _params)
```

Params are encoded into json and published to pulsar as payload. You can also specify message options:
```elixir
MyWorker.enqueue(%{"my_job_context" => 123}, properties: %{prop1: "1"}, partition_key: 123)
```

If you want to include your partition logic in your worker, you can instead implement the `partition_key` or `ordering_key` callbacks

```elixir
defmodule MyWorker do
  use PulsarEx.Worker,
    otp_app: :my_app,
    middlewares: [
      MyWorker.Middlewares.Test
    ]

  @impl PulsarEx.WorkerCallback
  def partition_key(%{id: id} = _params, _message_opts) do
    "#{id}"
  end

  @impl PulsarEx.WorkerCallback
  def ordering_key(%{ip_address: ip_address} = _params, _message_opts) do
    "#{ip_address}"
  end

  ...
end
```

And then
```elixir
MyWorker.enqueue(%{"my_job_context" => 123}, properties: %{prop1: "1"})
```
partition_key and ordering_key will be calculated based on your job payload.


#### What if you want to use your pulsar topic for multiple related jobs?
```elixir
defmodule MyWorker do
  use PulsarEx.Worker,
    otp_app: :my_app,
    middlewares: [
      MyWorker.Middlewares.Test
    ],
    jobs: [:create, :update]

  @impl PulsarEx.WorkerCallback
  def handle_job(:create, %{properties: properties} = state) do
    IO.inspect(state)
    :ok
  end

  @impl PulsarEx.WorkerCallback
  def handle_job(:update, %{properties: properties} = state) do
    IO.inspect(state)
    :ok
  end
end
```

So when you enqueue the job
```elixir
MyWorker.enqueue_job(:create, %{prop1: "test"})
```

This is useful when you want to take advantage of elixir's pattern matching to handle a bundle of similar processing of messages.

#### Consumer that subscribes to multiple topics

There are several ways to make it possible:

**Consume topics in the same namespace:**

Consumes any topics with `.json` suffix under `my-tenant/my-namespace`
```elixir
defmodule MyWorker do
  use PulsarEx.Worker,
    otp_app: :my_app,
    tenant: "my-tenant",
    namespace: "my-namespace",
    topic: ~r/.*\.json/
    ...

  @impl PulsarEx.WorkerCallback
  def topic(%{id: 1}, _message_opts) do
    "persistent://my-tenant/my-namespace/1.json"
  end

  def topic(%{id: 2}, _message_opts) do
    "persistent://my-tenant/my-namespace/2.json"
  end

  ...
end
```
```elixir
MyWorker.start()
```

**Define a consumer without topic/subscription**
```elixir
defmodule MyWorker do
  use PulsarEx.Worker,
    otp_app: :my_app,
    ...

  @impl PulsarEx.WorkerCallback
  def topic(%{id: 1}, _message_opts) do
    "persistent://my-tenant/my-namespace/1.json"
  end

  def topic(%{id: 2}, _message_opts) do
    "persistent://my-tenant/my-namespace/2.json"
  end

  ...
end

```

```elixir

PulsarEx.start_consumer(topic, subscription, MyWorker, opts)

PulsarEx.start_consumer(tenant_or_regex, namespace_or_regex, topic_or_regex, subscription, MyWorker, opts)

```

PulsarEx manages to discover topics based on all your tenant, namespace, and topic, either they are string or regex patterns. Consumer will also periodically discover new topics when they are added.

#### Interceptors
Sometimes you want to have some logic that allows you to skip publishing a job, for example deduping, or simply have more stats/logs. So you can write an interceptor

```elixir
defmodule MyInterceptor do
  @behaviour PulsarEx.Interceptor

  alias PulsarEx.JobInfo
  require Logger

  @impl true
  def call(handler) do
    fn %JobInfo{worker: worker, topic: topic, job: job} = job_info ->
      Logger.info("publishing job #{job} for worker #{worker} to topic #{topic}")
      handler.(job_info)
    end
  end
end
```

And you can load it up in your worker:

```elixir
defmodule MyWorker do
  use PulsarEx.Worker
    interceptors: [MyInterceptor]
end
```

#### Finally, testing
How to write tests that involves enqueue/consumer logic?

In your config/test.exs, you can do

```elixir
config :my_app, MyWorker,
  workers: 5, # starts a pool of 5 consumers per partition
  max_redelivery_attempts: 3,
  inline: true
```
**All the options you have specified in the definition of your worker can also be specified here.**

The `inline` option in your test environment enables you to run your `handle_job` logic, inline with your `enqueue/enqueue_job` call. Which by-passes the actual message routing through pulsar, making your tests independent of a pulsar server. For example, in your test:

```elixir
def MyWorker do
  use PulsarEx.Worker,
    jobs: [:create]

  def handle_job(:create, %{payload: %{test: true}}) do
    raise "This is a test"
  end
end

defmodule MyWorkerTest do
  use ExUnit.Case

  describe "create" do
    test "raise exception for test" do
      assert_raise RuntimeError, "This is a test", fn ->
        MyWorker.enqueue_job(:create, %{test: true})
      end
    end
  end
end
```

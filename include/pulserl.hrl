%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------

-define(UNDEF, undefined).
-define(ERROR_CLIENT_NOT_STARTED, {error, client_not_started}).
-define(PERSISTENT_DOMAIN, <<"persistent">>).
-define(NON_PERSISTENT_DOMAIN, <<"non-persistent">>).
-define(PUBLIC_TENANT, <<"public">>).
-define(DEFAULT_NAMESPACE, <<"default">>).

-record(partitionMeta, {partitions :: integer()}).
-record(topic,
        {domain = ?PERSISTENT_DOMAIN,
         tenant = ?PUBLIC_TENANT,
         namespace = ?DEFAULT_NAMESPACE,
         local,
         %%local
         parent :: #topic{}}).
-record(batch, {index = -1 :: integer(), size :: non_neg_integer()}).
-record(messageId,
        {ledger_id :: integer(),
         entry_id :: integer(),
         topic :: integer() | ?UNDEF,
         partition = -1 :: integer(),
         batch :: #batch{} | ?UNDEF}).
-record(consumerMessage,
        {id :: #messageId{},
         topic :: binary(),
         partition_key :: key() | ?UNDEF,
         ordering_key :: key() | ?UNDEF,
         event_time :: integer() | ?UNDEF,
         publish_time :: integer(),
         properties = #{} :: properties(),
         redelivery_count = 0 :: integer(),
         payload :: payload(),
         consumer :: pid()}).
-record(producerMessage,
        {partition_key :: key() | ?UNDEF,
         ordering_key :: key() | ?UNDEF,
         event_time :: integer() | ?UNDEF,
         properties = [] :: properties(),
         payload :: payload(),
         deliver_at_time :: integer() | ?UNDEF}).
-record(clientConfig,
        {socket_options = [] :: list(),
         connect_timeout_ms = 15000 :: pos_integer(),
         max_connections_per_broker = 1 :: pos_integer(),
         tls_trust_certs_file :: string() | ?UNDEF}).

-type key() :: string() | binary().
-type value() :: string() | binary().
-type payload() :: string() | binary().
-type topic() :: string() | binary() | #topic{}.
-type options() :: [{atom(), term()}, ...].
-type properties() :: map() | [{key(), value()}, ...].

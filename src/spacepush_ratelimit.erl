-module(spacepush_ratelimit).
-moduledoc """
Limits API requests per client within a fixed one-minute window, separately
for writes (registrations) and reads. Reads get a higher limit: many members
of a space often share one public address.
""".
-behaviour(gen_server).

-export([start_link/0, allow/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TABLE, ?MODULE).
-define(WINDOW_MS, 60000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec allow(write | read, term()) -> boolean().
allow(Bucket, Client) ->
    LimitKey =
        case Bucket of
            write -> rate_limit_per_minute;
            read -> read_rate_limit_per_minute
        end,
    {ok, Limit} = application:get_env(spacepush, LimitKey),
    Key = {Bucket, Client},
    ets:update_counter(?TABLE, Key, 1, {Key, 0}) =< Limit.

init([]) ->
    ets:new(?TABLE, [named_table, public, set, {write_concurrency, true}]),
    erlang:send_after(?WINDOW_MS, self(), reset),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(reset, State) ->
    ets:delete_all_objects(?TABLE),
    erlang:send_after(?WINDOW_MS, self(), reset),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

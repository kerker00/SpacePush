-module(spacepush_ratelimit).
-moduledoc "Limits API requests per client within a fixed one-minute window.".
-behaviour(gen_server).

-export([start_link/0, allow/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TABLE, ?MODULE).
-define(WINDOW_MS, 60000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec allow(term()) -> boolean().
allow(Client) ->
    Limit = application:get_env(spacepush, rate_limit_per_minute, 30),
    ets:update_counter(?TABLE, Client, 1, {Client, 0}) =< Limit.

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

-module(spacepush_registry).
-moduledoc """
Device registrations, kept on disk in DETS.

Each device token maps to its APNs environment and the topics it subscribed to.
A registration replaces the previous one for the same token.
""".
-behaviour(gen_server).

-export([start_link/0, register/3, unregister/1, subscribers/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-define(TABLE, ?MODULE).

-type environment() :: sandbox | production.
-export_type([environment/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register(binary(), environment(), [spacepush_state:topic()]) -> ok.
register(Token, Environment, Topics) ->
    gen_server:call(?MODULE, {register, Token, Environment, Topics}).

-spec unregister(binary()) -> ok.
unregister(Token) ->
    gen_server:call(?MODULE, {unregister, Token}).

-spec subscribers(spacepush_state:topic()) -> [{binary(), environment()}].
subscribers(Topic) ->
    gen_server:call(?MODULE, {subscribers, Topic}).

init([]) ->
    process_flag(trap_exit, true),
    File = application:get_env(spacepush, registry_file, "data/registry.dets"),
    ok = filelib:ensure_dir(File),
    {ok, ?TABLE} = dets:open_file(?TABLE, [{file, File}, {type, set}]),
    {ok, #{}}.

handle_call({register, Token, Environment, Topics}, _From, State) ->
    ok = dets:insert(?TABLE, {Token, Environment, Topics, erlang:system_time(second)}),
    {reply, ok, State};
handle_call({unregister, Token}, _From, State) ->
    ok = dets:delete(?TABLE, Token),
    {reply, ok, State};
handle_call({subscribers, Topic}, _From, State) ->
    Subscribers = dets:foldl(
        fun({Token, Environment, Topics, _UpdatedAt}, Acc) ->
            case lists:member(Topic, Topics) of
                true -> [{Token, Environment} | Acc];
                false -> Acc
            end
        end,
        [],
        ?TABLE
    ),
    {reply, Subscribers, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    dets:close(?TABLE).

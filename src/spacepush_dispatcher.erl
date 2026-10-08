-module(spacepush_dispatcher).
-moduledoc """
Debounces state changes and notifies the subscribers once a state holds.

A change waits `debounce_ms` before anyone is notified. If the state flips
back within that time, nothing is sent; if it moves on to a third state,
only the latest one is sent.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, state_changed/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec state_changed(spacepush_state:change()) -> ok.
state_changed(Change) ->
    gen_server:cast(?MODULE, {changed, Change}).

init([]) ->
    {ok, #{debounce_ms => application:get_env(spacepush, debounce_ms, 120000), pending => #{}}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({changed, {Topic, Name, From, To}}, #{pending := Pending, debounce_ms := Delay} = State) ->
    case Pending of
        #{Topic := #{from := To, timer := Timer}} ->
            erlang:cancel_timer(Timer),
            {noreply, State#{pending := maps:remove(Topic, Pending)}};
        #{Topic := Change} ->
            {noreply, State#{pending := Pending#{Topic := Change#{to := To, name := Name}}}};
        #{} ->
            Timer = erlang:send_after(Delay, self(), {settled, Topic}),
            Change = #{from => From, to => To, name => Name, timer => Timer},
            {noreply, State#{pending := Pending#{Topic => Change}}}
    end.

handle_info({settled, Topic}, #{pending := Pending} = State) ->
    case maps:take(Topic, Pending) of
        {#{to := To, name := Name}, Rest} ->
            notify(Topic, Name, To),
            {noreply, State#{pending := Rest}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

notify(Topic, Name, To) ->
    Subscribers = spacepush_registry:subscribers(Topic),
    ?LOG_INFO(#{msg => state_settled, topic => Topic, state => To, subscribers => length(Subscribers)}),
    Payload = spacepush_notification:payload(Topic, Name, To),
    [spacepush_apns:push(Token, Environment, Payload) || {Token, Environment} <- Subscribers],
    ok.

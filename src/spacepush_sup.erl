-module(spacepush_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Ordered so that each process finds the ones it calls already running.
init([]) ->
    Children = [
        worker(spacepush_registry),
        worker(spacepush_ratelimit),
        worker(spacepush_apns),
        worker(spacepush_dispatcher),
        worker(spacepush_poller)
    ],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 60}, Children}}.

worker(Module) ->
    #{id => Module, start => {Module, start_link, []}}.

-module(spacepush_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Ordered so that each process finds the ones it calls already running.
%% rest_for_one restarts everything after a crashed process, so the sender
%% never holds requests for an outbox or registry that was restarted.
%% Pending work survives in the outbox and the tracker file.
%% The statistics come last: the others only cast to them, which never fails,
%% and a crash there restarts nothing else.
init([]) ->
    Children = [
        worker(spacepush_ratelimit),
        worker(spacepush_registry),
        worker(spacepush_directory),
        worker(spacepush_cache),
        worker(spacepush_outbox),
        worker(spacepush_apns),
        worker(spacepush_poller),
        worker(spacepush_stats)
    ],
    {ok, {#{strategy => rest_for_one, intensity => 5, period => 60}, Children}}.

worker(Module) ->
    #{id => Module, start => {Module, start_link, []}}.

-module(spacepush_stats).
-moduledoc """
Usage statistics for monitoring, served by `GET /v1/stats`.

Counts events per day (requests, registrations, deliveries, fetches, state
changes) and the distinct app installs per day, ISO week and month. Days
follow the server's local time.

Installs are counted by the random ID the apps send in `X-SpaceState-Install`.
SpacePush never keeps that ID: it stores an HMAC of it under a secret salt,
only for the last `?INSTALL_DAYS` days, and keeps just the counts after that.
Client addresses are not recorded at all.

Recording is a cast, so a caller never waits for or fails because of this
process. Everything is saved to `stats_file` every minute and on shutdown.

On disk: `{stats, 1, Salt, Days, Installs, Uniques, LastSuccess, Processed}`.
""".
-behaviour(gen_server).

-export([start_link/0, request/2, rate_limited/1, count/1, count/2, duration/2, success/1, report/1]).
-export([install_id/1, user_agent/1, periods_closed_by/1, week_label/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TICK_MS, 60000).
%% Long enough to compute every week and month that ends within it.
-define(INSTALL_DAYS, 40).
-define(COUNTER_DAYS, 400).
-define(MAX_REPORT_DAYS, 400).

-type install() :: {Version :: binary(), Os :: binary(), OsVersion :: binary()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc """
Records an API request of `Kind` (such as `<<"directory">>`). Requests from
the apps carry their install ID; widgets and other clients are only counted.
""".
-spec request(binary(), cowboy_req:req()) -> ok.
request(Kind, Req) ->
    Client = user_agent(cowboy_req:header(<<"user-agent">>, Req)),
    Install = install_id(cowboy_req:header(<<"x-spacestate-install">>, Req)),
    gen_server:cast(?MODULE, {request, today(), Kind, Client, Install}).

-spec rate_limited(read | write) -> ok.
rate_limited(Bucket) ->
    count(<<"rate_limited.", (atom_to_binary(Bucket))/binary>>).

-spec count(binary()) -> ok.
count(Key) ->
    count(Key, 1).

-spec count(binary(), non_neg_integer()) -> ok.
count(_Key, 0) ->
    ok;
count(Key, N) ->
    gen_server:cast(?MODULE, {count, today(), Key, N}).

-doc "Adds a duration in milliseconds to `Key.ms_sum` and raises `Key.ms_max`.".
-spec duration(binary(), non_neg_integer()) -> ok.
duration(Key, Ms) ->
    gen_server:cast(?MODULE, {duration, today(), Key, Ms}).

-doc "Notes that `Source` (such as `<<\"directory\">>`) was fetched successfully just now.".
-spec success(binary()) -> ok.
success(Source) ->
    gen_server:cast(?MODULE, {success, Source, erlang:system_time(millisecond)}).

-doc "The statistics as a JSON-ready map, with daily counters for the last `Days` days.".
-spec report(pos_integer()) -> map().
report(Days) ->
    gen_server:call(?MODULE, {report, min(max(Days, 1), ?MAX_REPORT_DAYS)}).

-doc "The install ID if it is a UUID, lowercased; anything else is ignored.".
-spec install_id(term()) -> binary() | none.
install_id(Id) when is_binary(Id), byte_size(Id) =:= 36 ->
    Pattern = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    case re:run(Id, Pattern, [{capture, none}]) of
        match -> string:lowercase(Id);
        nomatch -> none
    end;
install_id(_) ->
    none.

-doc """
Reads the apps' user agent, `SpaceState/2.0.0 (iOS 26.0)` or
`SpaceStateWidget/2.0.0 (macOS 26.0)`. Other clients are `other`.
""".
-spec user_agent(term()) -> {app | widget, install()} | other.
user_agent(Agent) when is_binary(Agent), byte_size(Agent) =< 256 ->
    Pattern = "^(SpaceState|SpaceStateWidget)/([0-9A-Za-z.]{1,16}) \\((iOS|iPadOS|macOS) ([0-9.]{1,16})\\)",
    case re:run(Agent, Pattern, [{capture, all_but_first, binary}]) of
        {match, [<<"SpaceState">>, Version, Os, OsVersion]} -> {app, {Version, Os, OsVersion}};
        {match, [<<"SpaceStateWidget">>, Version, Os, OsVersion]} -> {widget, {Version, Os, OsVersion}};
        nomatch -> other
    end;
user_agent(_) ->
    other.

-doc "The periods that end with `Date`: always its day, plus its ISO week on Sundays and its month on its last day.".
-spec periods_closed_by(calendar:date()) -> [{binary(), [calendar:date()]}].
periods_closed_by({Year, Month, Day} = Date) ->
    Week =
        case calendar:day_of_the_week(Date) of
            7 -> [{week_label(Date), [add_days(Date, -N) || N <- lists:seq(6, 0, -1)]}];
            _ -> []
        end,
    MonthPeriod =
        case calendar:last_day_of_the_month(Year, Month) of
            Day -> [{month_label(Date), [{Year, Month, D} || D <- lists:seq(1, Day)]}];
            _ -> []
        end,
    [{day_label(Date), [Date]} | Week ++ MonthPeriod].

-spec week_label(calendar:date()) -> binary().
week_label(Date) ->
    {Year, Week} = calendar:iso_week_number(Date),
    iolist_to_binary(io_lib:format("~4..0B-W~2..0B", [Year, Week])).

init([]) ->
    process_flag(trap_exit, true),
    {ok, File} = application:get_env(spacepush, stats_file),
    Today = today(),
    State0 =
        case spacepush_store:load(File, none) of
            {stats, 1, Salt, Days, Installs, Uniques, LastSuccess, Processed} ->
                #{salt => Salt, days => Days, installs => Installs, uniques => Uniques,
                  last_success => LastSuccess, processed => Processed};
            _ ->
                #{salt => crypto:strong_rand_bytes(32), days => #{}, installs => #{}, uniques => #{},
                  last_success => #{}, processed => add_days(Today, -1)}
        end,
    erlang:send_after(?TICK_MS, self(), tick),
    {ok, close_days(State0#{file => File, dirty => true}, Today)}.

handle_call({report, Days}, _From, State0) ->
    State = close_days(State0, today()),
    {reply, build_report(Days, State), State}.

handle_cast({request, Date, Kind, Client, Install}, State) ->
    State1 = add(Date, <<"requests.", Kind/binary>>, 1, State),
    State2 =
        case Client of
            {widget, _} -> add(Date, <<"requests.widget">>, 1, State1);
            other -> add(Date, <<"requests.other">>, 1, State1);
            {app, _} -> State1
        end,
    {noreply, record_install(Date, Client, Install, State2)};
handle_cast({count, Date, Key, N}, State) ->
    {noreply, add(Date, Key, N, State)};
handle_cast({duration, Date, Key, Ms}, State) ->
    State1 = add(Date, <<Key/binary, ".ms_sum">>, Ms, State),
    {noreply, update(Date, <<Key/binary, ".ms_max">>, fun(Max) -> max(Max, Ms) end, State1)};
handle_cast({success, Source, At}, #{last_success := LastSuccess} = State) ->
    {noreply, State#{last_success := LastSuccess#{Source => At}, dirty := true}}.

handle_info(tick, State) ->
    erlang:send_after(?TICK_MS, self(), tick),
    {noreply, save(close_days(State, today()))};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    save(State).

%% Only app requests with a valid install ID count as installs.
record_install(Date, {app, Info}, Install, #{salt := Salt, installs := Installs} = State) when is_binary(Install) ->
    Hash = binary:part(crypto:mac(hmac, sha256, Salt, Install), 0, 16),
    Day = maps:get(Date, Installs, #{}),
    State#{installs := Installs#{Date => Day#{Hash => Info}}, dirty := true};
record_install(Date, {app, _Info}, none, State) ->
    add(Date, <<"requests.without_install">>, 1, State);
record_install(_Date, _Client, _Install, State) ->
    State.

add(Date, Key, N, State) ->
    update(Date, Key, fun(Value) -> Value + N end, State).

update(Date, Key, Fun, #{days := Days} = State) ->
    Counters = maps:get(Date, Days, #{}),
    State#{days := Days#{Date => Counters#{Key => Fun(maps:get(Key, Counters, 0))}}, dirty := true}.

%% Stores the distinct installs of every period that ended before `Today`,
%% then drops install hashes and counters that are no longer needed.
close_days(#{processed := Processed} = State0, Today) ->
    First = later(add_days(Processed, 1), add_days(Today, -?COUNTER_DAYS)),
    Closed = dates(First, add_days(Today, -1)),
    State1 = lists:foldl(fun close_day/2, State0, Closed),
    State2 =
        case Closed of
            [] -> State1;
            _ -> State1#{processed := lists:last(Closed), dirty := true}
        end,
    prune(State2, Today).

close_day(Date, #{uniques := Uniques, installs := Installs} = State) ->
    New = maps:from_list([{Label, distinct(PeriodDates, Installs)} || {Label, PeriodDates} <- periods_closed_by(Date)]),
    State#{uniques := maps:merge(Uniques, New)}.

prune(#{installs := Installs, days := Days} = State, Today) ->
    InstallCutoff = add_days(Today, -?INSTALL_DAYS),
    CounterCutoff = add_days(Today, -?COUNTER_DAYS),
    State#{
        installs := maps:filter(fun(Date, _) -> Date >= InstallCutoff end, Installs),
        days := maps:filter(fun(Date, _) -> Date >= CounterCutoff end, Days)
    }.

distinct(Dates, Installs) ->
    map_size(union(Dates, Installs)).

union(Dates, Installs) ->
    lists:foldl(fun(Date, Acc) -> maps:merge(Acc, maps:get(Date, Installs, #{})) end, #{}, Dates).

save(#{dirty := false} = State) ->
    State;
save(#{file := File, salt := Salt, days := Days, installs := Installs, uniques := Uniques,
       last_success := LastSuccess, processed := Processed} = State) ->
    spacepush_store:save(File, {stats, 1, Salt, Days, Installs, Uniques, LastSuccess, Processed}),
    State#{dirty := false}.

build_report(Days, #{days := Counters, installs := Installs, uniques := Uniques, last_success := LastSuccess}) ->
    Today = today(),
    Monday = add_days(Today, 1 - calendar:day_of_the_week(Today)),
    {Year, Month, _} = Today,
    Recent = union(dates(add_days(Today, -6), Today), Installs),
    #{
        <<"generated_at">> => rfc3339(erlang:system_time(millisecond)),
        <<"service">> => service(),
        <<"devices">> => devices(),
        <<"outbox">> => #{<<"pending">> => spacepush_outbox:pending()},
        <<"installs">> => #{
            <<"today">> => distinct([Today], Installs),
            <<"this_week">> => distinct(dates(Monday, Today), Installs),
            <<"this_month">> => distinct(dates({Year, Month, 1}, Today), Installs),
            <<"days">> => history(<<"-">>, 10, Uniques),
            <<"weeks">> => history(<<"-W">>, 12, Uniques),
            <<"months">> => history(<<>>, 13, Uniques),
            <<"last_7_days">> => #{
                <<"installs">> => map_size(Recent),
                <<"versions">> => tally(fun({Version, _Os, _OsVersion}) -> Version end, Recent),
                <<"platforms">> => tally(fun({_Version, Os, _OsVersion}) -> Os end, Recent),
                <<"os_versions">> => tally(fun({_Version, Os, OsVersion}) -> <<Os/binary, " ", OsVersion/binary>> end, Recent)
            }
        },
        <<"last_success">> => maps:map(fun(_Source, At) -> rfc3339(At) end, LastSuccess),
        <<"days">> => [
            (with_average(maps:get(Date, Counters, #{})))#{<<"date">> => day_label(Date)}
         || Date <- lists:reverse(dates(add_days(Today, 1 - Days), Today))
        ]
    }.

%% The registry's summary, with the spaces' names from the directory.
devices() ->
    #{<<"subscriptions">> := Subscriptions} = Summary = spacepush_registry:summary(),
    Names = maps:from_list([{Endpoint, Name} || #{endpoint := Endpoint, name := Name} <- spacepush_directory:entries()]),
    Summary#{<<"subscriptions">> := [S#{<<"name">> => maps:get(E, Names, null)} || #{<<"endpoint">> := E} = S <- Subscriptions]}.

%% The newest `Count` closed periods whose label has the given shape:
%% days `2026-10-09`, weeks `2026-W41`, months `2026-10`.
history(Separator, Count, Uniques) ->
    Labels = [Label || Label := _ <- Uniques, kind(Label) =:= Separator],
    Newest = lists:sublist(lists:reverse(lists:sort(Labels)), Count),
    [#{<<"period">> => Label, <<"installs">> => maps:get(Label, Uniques)} || Label <- Newest].

kind(<<_Year:4/binary, "-W", _Week:2/binary>>) -> <<"-W">>;
kind(<<_Year:4/binary, "-", _Month:2/binary, "-", _Day:2/binary>>) -> <<"-">>;
kind(<<_Year:4/binary, "-", _Month:2/binary>>) -> <<>>;
kind(_) -> undefined.

tally(Key, Installs) ->
    maps:fold(fun(_Hash, Info, Acc) -> maps:update_with(Key(Info), fun(N) -> N + 1 end, 1, Acc) end, #{}, Installs).

with_average(#{<<"poll.ms_sum">> := Sum, <<"poll.rounds">> := Rounds} = Counters) when Rounds > 0 ->
    Counters#{<<"poll.ms_avg">> => Sum div Rounds};
with_average(Counters) ->
    Counters.

service() ->
    {ok, Version} = application:get_key(spacepush, vsn),
    {Uptime, _} = erlang:statistics(wall_clock),
    #{
        <<"version">> => list_to_binary(Version),
        <<"uptime_s">> => Uptime div 1000,
        <<"memory_mb">> => erlang:memory(total) div 1048576,
        <<"processes">> => erlang:system_info(process_count)
    }.

today() ->
    {Date, _Time} = calendar:local_time(),
    Date.

add_days(Date, N) ->
    calendar:gregorian_days_to_date(calendar:date_to_gregorian_days(Date) + N).

later(A, B) when A >= B -> A;
later(_A, B) -> B.

%% Empty when `From` is after `To`, for example after the clock was set back.
dates(From, To) ->
    [
        calendar:gregorian_days_to_date(N)
     || N <- lists:seq(calendar:date_to_gregorian_days(From), max(calendar:date_to_gregorian_days(To), calendar:date_to_gregorian_days(From) - 1))
    ].

day_label({Year, Month, Day}) ->
    iolist_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B", [Year, Month, Day])).

month_label({Year, Month, _Day}) ->
    iolist_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

rfc3339(Ms) ->
    list_to_binary(calendar:system_time_to_rfc3339(Ms, [{unit, millisecond}, {offset, "Z"}])).

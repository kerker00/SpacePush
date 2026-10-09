-module(spacepush_stats).
-moduledoc """
Usage statistics for monitoring, served by `GET /v1/stats`.

Counts events per day (requests, registrations, deliveries, fetches, state
changes) and the active app installs per day, ISO week and month. Days
follow the server's local time.

Installs are counted without identifying them: an app sends
`X-SpaceState-First: day, week, month` (or a part of it) with its first request
of each period, and SpacePush adds one per named period. The app versions,
platforms and OS versions are tallied per ISO week from those week reports.
No ID and no client address is recorded.

Recording is a cast, so a caller never waits for or fails because of this
process. Everything is saved to `stats_file` every minute and on shutdown.

On disk: `{stats, 2, Days, Weeks, LastSuccess}`. Format 1 files, which held
hashed install IDs, are migrated on start and keep only their counters.
""".
-behaviour(gen_server).

-export([start_link/0, request/2, rate_limited/1, count/1, count/2, duration/2, success/1, report/1]).
-export([first_periods/1, user_agent/1, week_label/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TICK_MS, 60000).
%% Long enough for the 13 months before the current one.
-define(COUNTER_DAYS, 430).
-define(MAX_REPORT_DAYS, 400).
-define(HISTORY_WEEKS, 12).

-type install() :: {Version :: binary(), Os :: binary(), OsVersion :: binary()}.
-type period() :: day | week | month.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc """
Records an API request of `Kind` (such as `<<"directory">>`). Requests from
the apps may open a day, week or month; widgets and other clients are only counted.
""".
-spec request(binary(), cowboy_req:req()) -> ok.
request(Kind, Req) ->
    Client = user_agent(cowboy_req:header(<<"user-agent">>, Req)),
    First = first_periods(cowboy_req:header(<<"x-spacestate-first">>, Req)),
    gen_server:cast(?MODULE, {request, today(), Kind, Client, First}).

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

-doc "The periods named in `X-SpaceState-First`, such as `day, week`; unknown names are ignored.".
-spec first_periods(term()) -> [period()].
first_periods(Header) when is_binary(Header), byte_size(Header) =< 64 ->
    Names = [string:trim(Name) || Name <- binary:split(Header, <<",">>, [global])],
    [Period || {Name, Period} <- [{<<"day">>, day}, {<<"week">>, week}, {<<"month">>, month}], lists:member(Name, Names)];
first_periods(_) ->
    [].

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
            {stats, 2, Days, Weeks, LastSuccess} ->
                #{days => Days, weeks => Weeks, last_success => LastSuccess};
            %% Format 1 kept hashed install IDs; only the counters are carried over.
            {stats, 1, _Salt, Days, _Installs, _Uniques, LastSuccess, _Processed} ->
                #{days => Days, weeks => #{}, last_success => LastSuccess};
            _ ->
                #{days => #{}, weeks => #{}, last_success => #{}}
        end,
    erlang:send_after(?TICK_MS, self(), tick),
    {ok, prune(State0#{file => File, dirty => true}, Today)}.

handle_call({report, Days}, _From, State) ->
    {reply, build_report(Days, State), State}.

handle_cast({request, Date, Kind, Client, First}, State) ->
    State1 = add(Date, <<"requests.", Kind/binary>>, 1, State),
    State2 =
        case Client of
            {widget, _} -> add(Date, <<"requests.widget">>, 1, State1);
            other -> add(Date, <<"requests.other">>, 1, State1);
            {app, _} -> State1
        end,
    {noreply, record_first(Date, Client, First, State2)};
handle_cast({count, Date, Key, N}, State) ->
    {noreply, add(Date, Key, N, State)};
handle_cast({duration, Date, Key, Ms}, State) ->
    State1 = add(Date, <<Key/binary, ".ms_sum">>, Ms, State),
    {noreply, update(Date, <<Key/binary, ".ms_max">>, fun(Max) -> max(Max, Ms) end, State1)};
handle_cast({success, Source, At}, #{last_success := LastSuccess} = State) ->
    {noreply, State#{last_success := LastSuccess#{Source => At}, dirty := true}}.

handle_info(tick, State) ->
    erlang:send_after(?TICK_MS, self(), tick),
    {noreply, save(prune(State, today()))};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    save(State).

%% Only the apps report the periods they open; a week report also tallies their version.
record_first(Date, {app, Info}, First, State0) ->
    lists:foldl(
        fun(Period, State) ->
            State1 = add(Date, <<"installs.", (atom_to_binary(Period))/binary>>, 1, State),
            case Period of
                week -> tally_week(week_label(Date), Info, State1);
                _ -> State1
            end
        end,
        State0,
        First
    );
record_first(_Date, _Client, _First, State) ->
    State.

tally_week(Label, {Version, Os, OsVersion}, #{weeks := Weeks} = State) ->
    Week0 = maps:get(Label, Weeks, #{}),
    Week = lists:foldl(
        fun({Group, Key}, Acc) ->
            Counts = maps:get(Group, Acc, #{}),
            Acc#{Group => Counts#{Key => maps:get(Key, Counts, 0) + 1}}
        end,
        Week0,
        [{<<"versions">>, Version}, {<<"platforms">>, Os}, {<<"os_versions">>, <<Os/binary, " ", OsVersion/binary>>}]
    ),
    State#{weeks := Weeks#{Label => Week}, dirty := true}.

add(Date, Key, N, State) ->
    update(Date, Key, fun(Value) -> Value + N end, State).

update(Date, Key, Fun, #{days := Days} = State) ->
    Counters = maps:get(Date, Days, #{}),
    State#{days := Days#{Date => Counters#{Key => Fun(maps:get(Key, Counters, 0))}}, dirty := true}.

%% Drops counters and weekly tallies that are no longer reported.
prune(#{days := Days, weeks := Weeks} = State, Today) ->
    CounterCutoff = add_days(Today, -?COUNTER_DAYS),
    Recent = [week_label(add_days(Today, -7 * N)) || N <- lists:seq(0, ?HISTORY_WEEKS)],
    State#{
        days := maps:filter(fun(Date, _) -> Date >= CounterCutoff end, Days),
        weeks := maps:with(Recent, Weeks)
    }.

save(#{dirty := false} = State) ->
    State;
save(#{file := File, days := Days, weeks := Weeks, last_success := LastSuccess} = State) ->
    spacepush_store:save(File, {stats, 2, Days, Weeks, LastSuccess}),
    State#{dirty := false}.

build_report(Days, #{days := Counters, weeks := Weeks, last_success := LastSuccess}) ->
    Today = today(),
    Monday = add_days(Today, 1 - calendar:day_of_the_week(Today)),
    {Year, Month, _} = Today,
    Sum = fun(Period, Dates) -> installs(Period, Dates, Counters) end,
    ThisWeek = maps:get(week_label(Today), Weeks, #{}),
    #{
        <<"generated_at">> => rfc3339(erlang:system_time(millisecond)),
        <<"service">> => service(),
        <<"devices">> => devices(),
        <<"outbox">> => #{<<"pending">> => spacepush_outbox:pending()},
        <<"installs">> => #{
            <<"today">> => Sum(day, [Today]),
            <<"this_week">> => Sum(week, dates(Monday, Today)),
            <<"this_month">> => Sum(month, dates({Year, Month, 1}, Today)),
            <<"days">> => history(day, past_days(Today, 10), Counters),
            <<"weeks">> => history(week, past_weeks(Monday, ?HISTORY_WEEKS), Counters),
            <<"months">> => history(month, past_months(Today, 13), Counters),
            <<"this_week_by">> => maps:merge(#{<<"versions">> => #{}, <<"platforms">> => #{}, <<"os_versions">> => #{}}, ThisWeek)
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

installs(Period, Dates, Counters) ->
    Key = <<"installs.", (atom_to_binary(Period))/binary>>,
    lists:sum([maps:get(Key, maps:get(Date, Counters, #{}), 0) || Date <- Dates]).

%% Past periods, newest first, as `{Label, Dates}`; only those the service has counters for.
history(Period, Periods, Counters) ->
    [
        #{<<"period">> => Label, <<"installs">> => installs(Period, Dates, Counters)}
     || {Label, Dates} <- Periods, lists:any(fun(Date) -> maps:is_key(Date, Counters) end, Dates)
    ].

past_days(Today, Count) ->
    [{day_label(Date), [Date]} || N <- lists:seq(1, Count), Date <- [add_days(Today, -N)]].

past_weeks(Monday, Count) ->
    [{week_label(Start), dates(Start, add_days(Start, 6))} || N <- lists:seq(1, Count), Start <- [add_days(Monday, -7 * N)]].

past_months({Year, Month, _Day}, Count) ->
    [
        {month_label({Y, M, 1}), dates({Y, M, 1}, {Y, M, calendar:last_day_of_the_month(Y, M)})}
     || N <- lists:seq(1, Count), {Y, M} <- [month_before(Year, Month, N)]
    ].

month_before(Year, Month, N) ->
    Index = Year * 12 + Month - 1 - N,
    {Index div 12, Index rem 12 + 1}.

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

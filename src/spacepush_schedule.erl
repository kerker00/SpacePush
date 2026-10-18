-module(spacepush_schedule).
-moduledoc "Per-endpoint polling deadlines with spread background startup and subscriber priority.".
-export([select/7]).

%% Times are monotonic milliseconds. Dropped directory entries also lose their
%% deadlines. Promotion to subscribed takes effect at the next scheduler tick.
select(Urls, Subscribed, Previous, Now, ActiveInterval, BackgroundInterval, Limit) ->
    Active = maps:from_list([{Url, true} || Url <- Subscribed]),
    Schedule = maps:from_list([begin
        Interval = case maps:is_key(Url, Active) of true -> ActiveInterval; false -> BackgroundInterval end,
        Entry = case maps:find(Url, Previous) of
            error ->
                Offset = case maps:is_key(Url, Active) of
                    true -> 0;
                    false -> erlang:phash2(Url, BackgroundInterval)
                end,
                #{next => Now + Offset, interval => Interval};
            {ok, #{next := Next, interval := OldInterval} = Old} when Interval < OldInterval ->
                Old#{next := min(Next, Now), interval := Interval};
            {ok, #{next := Next, interval := OldInterval} = Old} when Interval > OldInterval ->
                Later = case maps:find(last_attempt, Old) of
                    {ok, Last} -> Last + Interval;
                    error -> Now + erlang:phash2(Url, BackgroundInterval)
                end,
                Old#{next := max(Next, Later), interval := Interval};
            {ok, Old} -> Old#{interval := Interval}
        end,
        {Url, Entry}
    end || Url <- lists:usort(Urls)]),
    Due = lists:sort([{case maps:is_key(Url, Active) of true -> 0; false -> 1 end, Next, Url}
        || Url := #{next := Next} <- Schedule, Next =< Now]),
    Selected = [Url || {_, _, Url} <- lists:sublist(Due, Limit)],
    Updated = lists:foldl(fun(Url, Acc) ->
        #{interval := Interval} = Entry = maps:get(Url, Acc),
        Acc#{Url := Entry#{next := Now + Interval, last_attempt => Now}}
    end, Schedule, Selected),
    {Selected, Updated}.

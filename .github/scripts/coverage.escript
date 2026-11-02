#!/usr/bin/env escript
%% Export coverage for application modules only, from this EUnit run.
-mode(compile).

main([CoverData, AppFile, Output]) ->
    {ok, [{application, spacepush, Properties}]} = file:consult(AppFile),
    Modules = proplists:get_value(modules, Properties),
    true = is_list(Modules) andalso Modules =/= [],
    {ok, _} = cover:start(),
    ok = cover:import(CoverData),
    {result, Results, []} = cover:analyse(Modules, coverage, module),
    Counts = [module_counts(Module, Results) || Module <- lists:sort(Modules)],
    Covered = lists:sum([C || #{<<"coveredLines">> := C} <- Counts]),
    Uncovered = lists:sum([U || #{<<"uncoveredLines">> := U} <- Counts]),
    true = Covered + Uncovered > 0,
    Metrics = #{
        <<"sourceSha">> => unicode:characters_to_binary(os:getenv("GITHUB_SHA", "local")),
        <<"otp">> => list_to_binary(erlang:system_info(otp_release)),
        <<"coveredLines">> => Covered,
        <<"uncoveredLines">> => Uncovered,
        <<"modules">> => Counts
    },
    ok = filelib:ensure_dir(Output),
    ok = file:write_file(Output, json:encode(Metrics)),
    io:format("EUnit line coverage: ~.1f% (~p/~p application lines)~n",
        [Covered * 100 / (Covered + Uncovered), Covered, Covered + Uncovered]);
main(_) ->
    io:format(standard_error, "Usage: coverage.escript COVERDATA APPLICATION.app OUTPUT.json~n", []),
    halt(1).

module_counts(Module, Results) ->
    %% A missing application module is an error, not silently excluded.
    {Module, {Covered, Uncovered}} = lists:keyfind(Module, 1, Results),
    #{<<"module">> => atom_to_binary(Module),
      <<"coveredLines">> => Covered, <<"uncoveredLines">> => Uncovered}.

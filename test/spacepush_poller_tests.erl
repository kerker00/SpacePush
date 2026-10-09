-module(spacepush_poller_tests).
-include_lib("eunit/include/eunit.hrl").

-define(A, <<"https://a.example/">>).
-define(B, <<"https://b.example/">>).

%% Fails in rounds 0, 1 and 2, then succeeds in the next round it is due.
backoff_doubles_and_resets_test() ->
    B1 = spacepush_poller:after_round(#{?A => {error, timeout}}, #{}, 0),
    ?assertEqual(#{?A => {1, 1}}, B1),
    ?assertEqual([?A], spacepush_poller:due([?A], B1, 1)),
    B2 = spacepush_poller:after_round(#{?A => {error, timeout}}, B1, 1),
    ?assertEqual(#{?A => {2, 3}}, B2),
    ?assertEqual([], spacepush_poller:due([?A], B2, 2)),
    ?assertEqual([?A], spacepush_poller:due([?A], B2, 3)),
    B3 = spacepush_poller:after_round(#{?A => invalid}, B2, 3),
    ?assertEqual(#{?A => {3, 7}}, B3),
    ?assertEqual(#{}, spacepush_poller:after_round(#{?A => {ok, ok}}, B3, 7)).

backoff_is_capped_test() ->
    Backoff = spacepush_poller:after_round(#{?A => {error, timeout}}, #{?A => {20, 0}}, 100),
    ?assertEqual(#{?A => {21, 130}}, Backoff).

other_urls_are_due_test() ->
    ?assertEqual([?B], spacepush_poller:due([?A, ?B], #{?A => {1, 5}}, 2)).

-module(spacepush_fetch).
-moduledoc """
HTTP GET for SpaceAPI endpoints, which are run by third parties and may be
hostile, and a variant that fetches many URLs with bounded parallelism.

Every request:

- resolves the host itself and connects only to a public address, pinned for
  the request, so neither the directory nor DNS can point SpacePush at
  loopback, private or link-local addresses; TLS is still verified against
  the host name,
- does not follow redirects, which could lead to internal addresses,
- reads the body in chunks and aborts beyond a size limit: `max_body_bytes`
  for spaces, `directory_max_bytes` for the aggregator's list,
- gives up after `fetch_timeout_ms` in total.

The function doing the request is configurable (`http_get`), so tests can
replace the network.
""".

-export([get/1, get/2, many/2, http_get/2, public_address/1]).

%% get/1 is this module's fetch, not the process dictionary.
-compile({no_auto_import, [get/1]}).

-define(USER_AGENT, <<"SpacePush/0.3 (+https://github.com/kerker00/SpacePush)">>).

-type result() :: {ok, binary()} | {error, term()}.
-export_type([result/0]).

-doc "Fetches a space's document, limited to `max_body_bytes`.".
-spec get(binary() | string()) -> result().
get(Url) ->
    {ok, MaxBytes} = application:get_env(spacepush, max_body_bytes),
    get(Url, MaxBytes).

-spec get(binary() | string(), pos_integer()) -> result().
get(Url, MaxBytes) ->
    {Module, Function} = application:get_env(spacepush, http_get, {?MODULE, http_get}),
    Module:Function(unicode:characters_to_list(Url), MaxBytes).

-doc "Fetches all URLs, at most `Concurrency` at a time, and returns each result.".
-spec many([binary()], pos_integer()) -> #{binary() => result()}.
many(Urls, Concurrency) ->
    many(Urls, Concurrency, #{}, #{}).

many([], _Concurrency, Running, Results) when map_size(Running) =:= 0 ->
    Results;
many([Url | Rest], Concurrency, Running, Results) when map_size(Running) < Concurrency ->
    {_Pid, Ref} = spawn_monitor(fun() -> exit({fetched, get(Url)}) end),
    many(Rest, Concurrency, Running#{Ref => Url}, Results);
many(Urls, Concurrency, Running, Results) ->
    receive
        {'DOWN', Ref, process, _Pid, Reason} when is_map_key(Ref, Running) ->
            Result =
                case Reason of
                    {fetched, Fetched} -> Fetched;
                    Other -> {error, {crashed, Other}}
                end,
            many(Urls, Concurrency, maps:remove(Ref, Running), Results#{maps:get(Ref, Running) => Result})
    end.

-doc "The default `http_get`: a real request with gun.".
-spec http_get(string(), pos_integer()) -> result().
http_get(Url, MaxBytes) ->
    {ok, Timeout} = application:get_env(spacepush, fetch_timeout_ms),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    case uri_string:parse(Url) of
        #{scheme := Scheme, host := Host} = Parsed when Scheme =:= "https"; Scheme =:= "http" ->
            case resolve(Host) of
                {ok, Address} -> request(Scheme, Host, Address, Parsed, Deadline, MaxBytes);
                {error, Reason} -> {error, Reason}
            end;
        _ ->
            {error, invalid_url}
    end.

request(Scheme, Host, Address, Parsed, Deadline, MaxBytes) ->
    Port = maps:get(port, Parsed, default_port(Scheme)),
    Transport =
        case Scheme of
            "https" -> #{transport => tls, tls_opts => spacepush_tls:client_opts(Host)};
            "http" -> #{transport => tcp}
        end,
    Options = Transport#{protocols => [http], retry => 0, connect_timeout => remaining(Deadline)},
    {ok, Conn} = gun:open(Address, Port, Options),
    try
        case gun:await_up(Conn, remaining(Deadline)) of
            {ok, _Protocol} ->
                Headers = [
                    {<<"host">>, host_header(Host, Port, Scheme)},
                    {<<"user-agent">>, ?USER_AGENT},
                    {<<"accept">>, <<"application/json">>}
                ],
                Ref = gun:get(Conn, path(Parsed), Headers),
                response(Conn, Ref, Deadline, MaxBytes);
            {error, Reason} ->
                {error, Reason}
        end
    after
        gun:close(Conn)
    end.

response(Conn, Ref, Deadline, MaxBytes) ->
    case gun:await(Conn, Ref, remaining(Deadline)) of
        {response, nofin, 200, _Headers} -> body(Conn, Ref, Deadline, MaxBytes, []);
        {response, fin, 200, _Headers} -> {ok, <<>>};
        {response, _IsFin, Status, _Headers} -> {error, {http_status, Status}};
        {error, Reason} -> {error, Reason}
    end.

body(Conn, Ref, Deadline, MaxBytes, Acc) ->
    case gun:await(Conn, Ref, remaining(Deadline)) of
        {data, IsFin, Data} ->
            Acc1 = [Acc, Data],
            case {iolist_size(Acc1) > MaxBytes, IsFin} of
                {true, _} -> {error, too_large};
                {false, fin} -> {ok, iolist_to_binary(Acc1)};
                {false, nofin} -> body(Conn, Ref, Deadline, MaxBytes, Acc1)
            end;
        {trailers, _Trailers} ->
            {ok, iolist_to_binary(Acc)};
        {error, Reason} ->
            {error, Reason}
    end.

%% The address to connect to; an error unless every address of the host is public.
resolve(Host) ->
    case inet:parse_address(Host) of
        {ok, Address} ->
            check_public([Address]);
        {error, einval} ->
            case addresses(Host, inet) ++ addresses(Host, inet6) of
                [] -> {error, nxdomain};
                Addresses -> check_public(Addresses)
            end
    end.

addresses(Host, Family) ->
    case inet:getaddrs(Host, Family) of
        {ok, Addresses} -> Addresses;
        {error, _} -> []
    end.

%% A host that resolves to any non-public address is refused, even if it also
%% has public ones: that is not how legitimate SpaceAPI hosts look.
check_public(Addresses) ->
    case lists:all(fun public_address/1, Addresses) of
        true -> {ok, hd(Addresses)};
        false -> {error, non_public_address}
    end.

-doc "False for loopback, private, link-local, shared, multicast and reserved addresses.".
-spec public_address(inet:ip_address()) -> boolean().
public_address({0, _, _, _}) -> false;
public_address({10, _, _, _}) -> false;
public_address({100, B, _, _}) when B >= 64, B =< 127 -> false;
public_address({127, _, _, _}) -> false;
public_address({169, 254, _, _}) -> false;
public_address({172, B, _, _}) when B >= 16, B =< 31 -> false;
public_address({192, 0, 0, _}) -> false;
public_address({192, 168, _, _}) -> false;
public_address({198, B, _, _}) when B =:= 18; B =:= 19 -> false;
public_address({A, _, _, _}) when A >= 224 -> false;
public_address({_, _, _, _}) -> true;
public_address({0, 0, 0, 0, 0, 0, 0, _}) -> false;
public_address({0, 0, 0, 0, 0, 16#ffff, A, B}) -> public_address({A bsr 8, A band 255, B bsr 8, B band 255});
public_address({A, _, _, _, _, _, _, _}) when A band 16#fe00 =:= 16#fc00 -> false;
public_address({A, _, _, _, _, _, _, _}) when A band 16#ffc0 =:= 16#fe80 -> false;
public_address({A, _, _, _, _, _, _, _}) when A band 16#ff00 =:= 16#ff00 -> false;
public_address({16#2001, 16#db8, _, _, _, _, _, _}) -> false;
public_address({_, _, _, _, _, _, _, _}) -> true.

default_port("https") -> 443;
default_port("http") -> 80.

host_header(Host, 443, "https") -> unicode:characters_to_binary(Host);
host_header(Host, 80, "http") -> unicode:characters_to_binary(Host);
host_header(Host, Port, _Scheme) -> unicode:characters_to_binary([Host, $:, integer_to_list(Port)]).

path(Parsed) ->
    Path =
        case maps:get(path, Parsed, "") of
            "" -> "/";
            P -> P
        end,
    case maps:get(query, Parsed, undefined) of
        undefined -> unicode:characters_to_binary(Path);
        Query -> unicode:characters_to_binary([Path, $?, Query])
    end.

remaining(Deadline) ->
    max(0, Deadline - erlang:monotonic_time(millisecond)).

-module(spacepush_tls).
-moduledoc "Client TLS options that verify the server against the OS trust store.".

-export([client_opts/1]).

-spec client_opts(string()) -> [ssl:tls_client_option()].
client_opts(Host) ->
    [
        {verify, verify_peer},
        {cacerts, public_key:cacerts_get()},
        {server_name_indication, Host},
        {customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}
    ].

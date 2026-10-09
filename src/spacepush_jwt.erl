-module(spacepush_jwt).
-moduledoc "Provider tokens for APNs token-based authentication (ES256 JWT).".

-include_lib("public_key/include/public_key.hrl").

-export([token/4, read_key/1]).

-doc "Reads the `.p8` signing key downloaded from the Apple Developer account.".
-spec read_key(file:filename_all()) -> #'ECPrivateKey'{}.
read_key(File) ->
    {ok, Pem} = file:read_file(File),
    [Entry] = public_key:pem_decode(Pem),
    #'ECPrivateKey'{} = public_key:pem_entry_decode(Entry).

-spec token(KeyId :: binary(), TeamId :: binary(), #'ECPrivateKey'{}, IssuedAt :: integer()) -> binary().
token(KeyId, TeamId, Key, IssuedAt) ->
    Header = encode(#{<<"alg">> => <<"ES256">>, <<"kid">> => KeyId}),
    Claims = encode(#{<<"iss">> => TeamId, <<"iat">> => IssuedAt}),
    Input = <<Header/binary, ".", Claims/binary>>,
    %% JWS wants the raw R || S pair, public_key returns it DER encoded.
    #'ECDSA-Sig-Value'{r = R, s = S} =
        public_key:der_decode('ECDSA-Sig-Value', public_key:sign(Input, sha256, Key)),
    <<Input/binary, ".", (base64url(<<R:256, S:256>>))/binary>>.

encode(Map) ->
    base64url(iolist_to_binary(json:encode(Map))).

base64url(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).

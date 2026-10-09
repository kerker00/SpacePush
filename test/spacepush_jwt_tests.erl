-module(spacepush_jwt_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

new_key() ->
    public_key:generate_key({namedCurve, secp256r1}).

decode_part(Part) ->
    json:decode(base64:decode(Part, #{mode => urlsafe, padding => false})).

token_structure_test() ->
    Token = spacepush_jwt:token(<<"KEY123">>, <<"TEAM456">>, new_key(), 1700000000),
    [Header, Claims, _Signature] = binary:split(Token, <<".">>, [global]),
    ?assertEqual(#{<<"alg">> => <<"ES256">>, <<"kid">> => <<"KEY123">>}, decode_part(Header)),
    ?assertEqual(#{<<"iss">> => <<"TEAM456">>, <<"iat">> => 1700000000}, decode_part(Claims)).

signature_verifies_test() ->
    #'ECPrivateKey'{publicKey = Point, parameters = Params} = Key = new_key(),
    Token = spacepush_jwt:token(<<"K">>, <<"T">>, Key, 1),
    [Header, Claims, Signature] = binary:split(Token, <<".">>, [global]),
    <<R:256, S:256>> = base64:decode(Signature, #{mode => urlsafe, padding => false}),
    Der = public_key:der_encode('ECDSA-Sig-Value', #'ECDSA-Sig-Value'{r = R, s = S}),
    ?assert(public_key:verify(<<Header/binary, ".", Claims/binary>>, sha256, Der, {#'ECPoint'{point = Point}, Params})).

read_key_test() ->
    Key = new_key(),
    Pem = public_key:pem_encode([public_key:pem_entry_encode('PrivateKeyInfo', Key)]),
    File = filename:join(spacepush_test_util:tmp_dir("jwt"), "AuthKey_TEST.p8"),
    ok = file:write_file(File, Pem),
    ?assertEqual(Key#'ECPrivateKey'.privateKey, (spacepush_jwt:read_key(File))#'ECPrivateKey'.privateKey).

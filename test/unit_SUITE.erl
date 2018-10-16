%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("rabbit_memory.hrl").

-compile(export_all).

all() ->
    [
        {group, parallel_tests},
        {group, parse_mem_limit},
        {group, gen_server2}
    ].

groups() ->
    [
        {parallel_tests, [parallel], [
            data_coercion_to_proplist,
            data_coercion_to_list,
            data_coercion_to_map,
            pget,
            encrypt_decrypt,
            encrypt_decrypt_term,
            version_equivalence,
            pid_decompose_compose,
            platform_and_version
        ]},
        {parse_mem_limit, [parallel], [
            parse_mem_limit_relative_exactly_max,
            parse_mem_relative_above_max,
            parse_mem_relative_integer,
            parse_mem_relative_invalid
        ]},
        {gen_server2, [parallel], [
            stats_timer_is_working,
            stats_timer_writes_gen_server2_metrics_if_core_metrics_ets_exists,
            stop_stats_timer_on_hibernation,
            stop_stats_timer_on_backoff,
            stop_stats_timer_on_backoff_when_backoff_less_than_stats_timeout
        ]}
    ].

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(_, Config) -> Config.

end_per_testcase(stats_timer_is_working, Config) ->
    reset_stats_interval(),
    Config;
end_per_testcase(stop_stats_timer_on_hibernation, Config) ->
    reset_stats_interval(),
    Config;
end_per_testcase(stop_stats_timer_on_backoff, Config) ->
    reset_stats_interval(),
    Config;
end_per_testcase(stop_stats_timer_on_backoff_when_backoff_less_than_stats_timeout, Config) ->
    reset_stats_interval(),
    Config;
end_per_testcase(stats_timer_writes_gen_server2_metrics_if_core_metrics_ets_exists, Config) ->
    rabbit_core_metrics:terminate(),
    reset_stats_interval(),
    Config;
end_per_testcase(_, Config) -> Config.

stats_timer_is_working(_) ->
    StatsInterval = 300,
    set_stats_interval(StatsInterval),

    {ok, TestServer} = gen_server2_test_server:start_link(count_stats),
    %% Start the emission
    % TestServer ! emit_gen_server2_stats,

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount = gen_server2_test_server:stats_count(TestServer),
    4 = StatsCount.

stats_timer_writes_gen_server2_metrics_if_core_metrics_ets_exists(_) ->
    rabbit_core_metrics:init(),

    StatsInterval = 300,
    set_stats_interval(StatsInterval),

    {ok, TestServer} = gen_server2_test_server:start_link(),
    timer:sleep(StatsInterval * 4),

    %% No messages in the buffer
    0 = rabbit_core_metrics:get_gen_server2_stats(TestServer),

    %% Sleep to accumulate messages
    gen_server2:cast(TestServer, {sleep, StatsInterval + 100}),

    %% Sleep to get results
    gen_server2:cast(TestServer, {sleep, 1000}),
    gen_server2:cast(TestServer, ignore),
    gen_server2:cast(TestServer, ignore),
    gen_server2:cast(TestServer, ignore),

    timer:sleep(StatsInterval + 150),
    4 = rabbit_core_metrics:get_gen_server2_stats(TestServer).

stop_stats_timer_on_hibernation(_) ->
    StatsInterval = 300,
    set_stats_interval(StatsInterval),

    %% No backoff configured
    {ok, TestServer} = gen_server2_test_server:start_link(count_stats),

    ok = gen_server2:call(TestServer, hibernate),

    timer:sleep(50),

    {current_function,{erlang,hibernate,3}} =
        erlang:process_info(TestServer, current_function),

    timer:sleep(StatsInterval * 6 + 100),
    StatsCount1 = gen_server2_test_server:stats_count(TestServer),
    %% The timer was stopped. No stats collected
    %% The count is 1 because hibernation emits stats
    1 = StatsCount1,

    %% A message will wake up the process
    gen_server2:call(TestServer, wake_up),
    gen_server2:call(TestServer, wake_up),
    gen_server2:call(TestServer, wake_up),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount5 = gen_server2_test_server:stats_count(TestServer),
    5 = StatsCount5,

    ok = gen_server2:call(TestServer, hibernate),

    timer:sleep(50),

    {current_function,{erlang,hibernate,3}} =
        erlang:process_info(TestServer, current_function),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount6 = gen_server2_test_server:stats_count(TestServer),
    %% The timer was stopped. No stats collected
    %% The count is 1 because hibernation emits stats
    6 = StatsCount6.

stop_stats_timer_on_backoff(_) ->
    StatsInterval = 300,
    set_stats_interval(StatsInterval),

    Backoff = 1000,
    {ok, TestServer} =
        gen_server2_test_server:start_link(
            count_stats,
            {backoff, Backoff, Backoff, 10000}),

    ok = gen_server2:call(TestServer, hibernate),

    {current_function,{gen_server2,process_next_msg,1}} =
        erlang:process_info(TestServer, current_function),

    %% Receiving messages during backoff period does not emit stats
    timer:sleep(Backoff div 2),
    ok = gen_server2:call(TestServer, hibernate),

    timer:sleep(Backoff div 2 + 50),
    {current_function,{gen_server2,process_next_msg,1}} =
        erlang:process_info(TestServer, current_function),

    %% Hibernate after backoff time after last message
    timer:sleep(Backoff div 2),
    {current_function,{erlang,hibernate,3}} =
        erlang:process_info(TestServer, current_function),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount = gen_server2_test_server:stats_count(TestServer),
    %% The timer was stopped. No stats collected
    %% The count is 1 because hibernation emits stats
    1 = StatsCount,

    %% A message will wake up the process
    gen_server2:call(TestServer, wake_up),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount5 = gen_server2_test_server:stats_count(TestServer),
    5 = StatsCount5.

stop_stats_timer_on_backoff_when_backoff_less_than_stats_timeout(_) ->
    StatsInterval = 300,
    set_stats_interval(StatsInterval),

    Backoff = 200,
    {ok, TestServer} =
        gen_server2_test_server:start_link(
            count_stats,
            {backoff, Backoff, Backoff, 10000}),

    ok = gen_server2:call(TestServer, hibernate),

    {current_function,{gen_server2,process_next_msg,1}} =
        erlang:process_info(TestServer, current_function),

    timer:sleep(Backoff + 50),

    {current_function,{erlang,hibernate,3}} =
        erlang:process_info(TestServer, current_function),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount = gen_server2_test_server:stats_count(TestServer),
    %% The timer was stopped. No stats collected
    %% The count is 1 because hibernation emits stats
    1 = StatsCount,

    %% A message will wake up the process
    gen_server2:call(TestServer, wake_up),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount5 = gen_server2_test_server:stats_count(TestServer),
    5 = StatsCount5.

parse_mem_limit_relative_exactly_max(_Config) ->
    MemLimit = vm_memory_monitor:parse_mem_limit(1.0),
    case MemLimit of
        ?MAX_VM_MEMORY_HIGH_WATERMARK -> ok;
        _ ->    ct:fail(
                    "Expected memory limit to be ~p, but it was ~p",
                    [?MAX_VM_MEMORY_HIGH_WATERMARK, MemLimit]
                )
    end.

parse_mem_relative_above_max(_Config) ->
    MemLimit = vm_memory_monitor:parse_mem_limit(1.01),
    case MemLimit of
        ?MAX_VM_MEMORY_HIGH_WATERMARK -> ok;
        _ ->    ct:fail(
                    "Expected memory limit to be ~p, but it was ~p",
                    [?MAX_VM_MEMORY_HIGH_WATERMARK, MemLimit]
                )
    end.

parse_mem_relative_integer(_Config) ->
    MemLimit = vm_memory_monitor:parse_mem_limit(1),
    case MemLimit of
        ?MAX_VM_MEMORY_HIGH_WATERMARK -> ok;
        _ ->    ct:fail(
                    "Expected memory limit to be ~p, but it was ~p",
                    [?MAX_VM_MEMORY_HIGH_WATERMARK, MemLimit]
                )
    end.

parse_mem_relative_invalid(_Config) ->
    MemLimit = vm_memory_monitor:parse_mem_limit([255]),
    case MemLimit of
        ?DEFAULT_VM_MEMORY_HIGH_WATERMARK -> ok;
        _ ->    ct:fail(
                    "Expected memory limit to be ~p, but it was ~p",
                    [?DEFAULT_VM_MEMORY_HIGH_WATERMARK, MemLimit]
                )
    end.

platform_and_version(_Config) ->
    MajorVersion = erlang:system_info(otp_release),
    Result = rabbit_misc:platform_and_version(),
    RegExp = "^Erlang/OTP\s" ++ MajorVersion,
    case re:run(Result, RegExp) of
        nomatch -> ct:fail("~p does not match ~p", [Result, RegExp]);
        {error, ErrType} -> ct:fail("~p", [ErrType]);
        _ -> ok
    end.

data_coercion_to_map(_Config) ->
    ?assertEqual(#{a => 1}, rabbit_data_coercion:to_map([{a, 1}])),
    ?assertEqual(#{a => 1}, rabbit_data_coercion:to_map(#{a => 1})).

data_coercion_to_proplist(_Config) ->
    ?assertEqual([{a, 1}], rabbit_data_coercion:to_proplist([{a, 1}])),
    ?assertEqual([{a, 1}], rabbit_data_coercion:to_proplist(#{a => 1})).

data_coercion_to_list(_Config) ->
    ?assertEqual([{a, 1}], rabbit_data_coercion:to_list([{a, 1}])),
    ?assertEqual([{a, 1}], rabbit_data_coercion:to_list(#{a => 1})).

pget(_Config) ->
    ?assertEqual(1, rabbit_misc:pget(a, [{a, 1}])),
    ?assertEqual(undefined, rabbit_misc:pget(b, [{a, 1}])),

    ?assertEqual(1, rabbit_misc:pget(a, #{a => 1})),
    ?assertEqual(undefined, rabbit_misc:pget(b, #{a => 1})).

pid_decompose_compose(_Config) ->
    Pid = self(),
    {Node, Cre, Id, Ser} = rabbit_misc:decompose_pid(Pid),
    Node = node(Pid),
    Pid = rabbit_misc:compose_pid(Node, Cre, Id, Ser),
    OtherNode = 'some_node@localhost',
    PidOnOtherNode = rabbit_misc:pid_change_node(Pid, OtherNode),
    {OtherNode, Cre, Id, Ser} = rabbit_misc:decompose_pid(PidOnOtherNode).

encrypt_decrypt(_Config) ->
    %% Take all available block ciphers.
    Hashes = rabbit_pbe:supported_hashes(),
    Ciphers = rabbit_pbe:supported_ciphers(),
    %% For each cipher, try to encrypt and decrypt data sizes from 0 to 64 bytes
    %% with a random passphrase.
    _ = [begin
             PassPhrase = crypto:strong_rand_bytes(16),
             Iterations = rand:uniform(100),
             Data = crypto:strong_rand_bytes(64),
             [begin
                  Expected = binary:part(Data, 0, Len),
                  Enc = rabbit_pbe:encrypt(C, H, Iterations, PassPhrase, Expected),
                  Expected = iolist_to_binary(rabbit_pbe:decrypt(C, H, Iterations, PassPhrase, Enc))
              end || Len <- lists:seq(0, byte_size(Data))]
         end || H <- Hashes, C <- Ciphers],
    ok.

encrypt_decrypt_term(_Config) ->
    %% Take all available block ciphers.
    Hashes = rabbit_pbe:supported_hashes(),
    Ciphers = rabbit_pbe:supported_ciphers(),
    %% Different Erlang terms to try encrypting.
    DataSet = [
        10000,
        [5672],
        [{"127.0.0.1", 5672},
            {"::1",       5672}],
        [{connection, info}, {channel, info}],
        [{cacertfile,           "/path/to/testca/cacert.pem"},
            {certfile,             "/path/to/server/cert.pem"},
            {keyfile,              "/path/to/server/key.pem"},
            {verify,               verify_peer},
            {fail_if_no_peer_cert, false}],
        [<<".*">>, <<".*">>, <<".*">>]
    ],
    _ = [begin
             PassPhrase = crypto:strong_rand_bytes(16),
             Iterations = rand:uniform(100),
             Enc = rabbit_pbe:encrypt_term(C, H, Iterations, PassPhrase, Data),
             Data = rabbit_pbe:decrypt_term(C, H, Iterations, PassPhrase, Enc)
         end || H <- Hashes, C <- Ciphers, Data <- DataSet],
    ok.

version_equivalence(_Config) ->
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.0"),
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.1"),
    true = rabbit_misc:version_minor_equivalent("%%VSN%%", "%%VSN%%"),
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0"),
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.0.1"),
    true = rabbit_misc:version_minor_equivalent("3.0.0.1", "3.0.0.3"),
    true = rabbit_misc:version_minor_equivalent("3.0.0.1", "3.0.1.3"),
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.foo"),
    false = rabbit_misc:version_minor_equivalent("3.0.0", "3.1.0"),
    false = rabbit_misc:version_minor_equivalent("3.0.0.1", "3.1.0.1"),

    false = rabbit_misc:version_minor_equivalent("3.5.7", "3.6.7"),
    false = rabbit_misc:version_minor_equivalent("3.6.5", "3.6.6"),
    false = rabbit_misc:version_minor_equivalent("3.6.6", "3.7.0"),
    true = rabbit_misc:version_minor_equivalent("3.6.7", "3.6.6"),

    %% Starting with RabbitMQ 3.7.x and feature flags introduced in
    %% RabbitMQ 3.8.x, versions are considered equivalent and the actual
    %% check is deferred to the feature flags module.
    false = rabbit_misc:version_minor_equivalent("3.6.0", "3.8.0"),
    true = rabbit_misc:version_minor_equivalent("3.7.0", "3.8.0"),
    true = rabbit_misc:version_minor_equivalent("3.7.0", "3.10.0"),

    true = rabbit_misc:version_minor_equivalent(<<"3.0.0">>, <<"3.0.0">>),
    true = rabbit_misc:version_minor_equivalent(<<"3.0.0">>, <<"3.0.1">>),
    true = rabbit_misc:version_minor_equivalent(<<"%%VSN%%">>, <<"%%VSN%%">>),
    true = rabbit_misc:version_minor_equivalent(<<"3.0.0">>, <<"3.0">>),
    true = rabbit_misc:version_minor_equivalent(<<"3.0.0">>, <<"3.0.0.1">>),
    false = rabbit_misc:version_minor_equivalent(<<"3.0.0">>, <<"3.1.0">>),
    false = rabbit_misc:version_minor_equivalent(<<"3.0.0.1">>, <<"3.1.0.1">>).

set_stats_interval(Interval) ->
    application:set_env(rabbit, collect_statistics, coarse),
    application:set_env(rabbit, collect_statistics_interval, Interval).

reset_stats_interval() ->
    application:unset_env(rabbit, collect_statistics),
    application:unset_env(rabbit, collect_statistics_interval).

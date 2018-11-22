%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(amqqueue). %% Could become amqqueue_v2 in the future.

-include_lib("rabbit_common/include/rabbit.hrl").

-export([new/9,
         fields/0,
         field_vhost/0,
         % arguments
         get_arguments/1,
         set_arguments/2,
         % decorators
         get_decorators/1,
         set_decorators/2,
         % exclusive_owner
         get_exclusive_owner/1,
         % gm_pids
         get_gm_pids/1,
         set_gm_pids/2,
         get_leader/1,
         % name (#resource)
         get_name/1,
         set_name/2,
         % operator_policy
         get_operator_policy/1,
         set_operator_policy/2,
         get_options/1,
         % pid
         get_pid/1,
         set_pid/2,
         % policy
         get_policy/1,
         set_policy/2,
         % policy_version
         get_policy_version/1,
         set_policy_version/2,
         % quorum_nodes
         get_quorum_nodes/1,
         set_quorum_nodes/2,
         % recoverable_slaves
         get_recoverable_slaves/1,
         set_recoverable_slaves/2,
         % slave_pids
         get_slave_pids/1,
         set_slave_pids/2,
         % slave_pids_pending_shutdown
         get_slave_pids_pending_shutdown/1,
         set_slave_pids_pending_shutdown/2,
         % state
         get_state/1,
         set_state/2,
         % sync_slave_pids
         get_sync_slave_pids/1,
         set_sync_slave_pids/2,
         get_type/1,
         get_vhost/1,
         is_amqqueue/1,
         is_auto_delete/1,
         is_durable/1,
         is_classic/1,
         is_quorum/1,
         pattern_match_all/0,
         pattern_match_on_name/1,
         reset_mirroring_and_decorators/1,
         set_immutable/1,
         qnode/1,
         macros/0]).

-define(record_version, amqqueue_v2).

new(Name,
    Pid,
    Durable,
    AutoDelete,
    Owner,
    Args,
    VHost,
    ActingUser,
    Type
   ) ->
    case quorum_queue_ff_enabled() of
        true ->
            #amqqueue{name               = Name,
                      durable            = Durable,
                      auto_delete        = AutoDelete,
                      arguments          = Args,
                      exclusive_owner    = Owner,
                      pid                = Pid,
                      slave_pids         = [],
                      sync_slave_pids    = [],
                      recoverable_slaves = [],
                      gm_pids            = [],
                      state              = live,
                      policy_version     = 0,
                      slave_pids_pending_shutdown = [],
                      vhost                       = VHost,
                      options = #{user => ActingUser},
                      type               = Type,
                      created_at         = erlang:monotonic_time()};
        false ->
            amqqueue_v1:new(
              Name,
              Pid,
              Durable,
              AutoDelete,
              Owner,
              Args,
              VHost,
              ActingUser)
    end.

is_amqqueue(#amqqueue{}) -> true;
is_amqqueue(Queue)       -> amqqueue_v1:is_amqqueue(Queue).

quorum_queue_ff_enabled() ->
    %% TODO: Check if the featurue flag is enabled or not.
    %%
    %% Possible solutions:
    %%   - We make a call to an Mnesia/ETS local table.
    %%   - We use the code_version moduel to "rebuild" this module on
    %%     the fly, so the function returns statically "true" or "false".
    true.

% arguments

get_arguments(#amqqueue{arguments = Args}) ->
    Args;
get_arguments(Queue) ->
    amqqueue_v1:get_arguments(Queue).

set_arguments(#amqqueue{} = Queue, Args) ->
    Queue#amqqueue{arguments = Args};
set_arguments(Queue, Args) ->
    amqqueue_v1:set_arguments(Queue, Args).

% decorators

get_decorators(#amqqueue{decorators = Decorators}) ->
    Decorators;
get_decorators(Queue) ->
    amqqueue_v1:get_decorators(Queue).

set_decorators(#amqqueue{} = Queue, Decorators) ->
    Queue#amqqueue{decorators = Decorators};
set_decorators(Queue, Decorators) ->
    amqqueue_v1:set_decorators(Queue, Decorators).

get_exclusive_owner(#amqqueue{exclusive_owner = Owner}) ->
    Owner;
get_exclusive_owner(Queue) ->
    amqqueue_v1:get_exclusive_owner(Queue).

get_gm_pids(#amqqueue{gm_pids = GMPids}) ->
    GMPids;
get_gm_pids(Queue) ->
    amqqueue_v1:get_gm_pids(Queue).

set_gm_pids(#amqqueue{} = Queue, GMPids) ->
    Queue#amqqueue{gm_pids = GMPids};
set_gm_pids(Queue, GMPids) ->
    amqqueue_v1:set_gm_pids(Queue, GMPids).

get_leader(#amqqueue{type = quorum, pid = {_, Leader}}) -> Leader.

% operator_policy

get_operator_policy(#amqqueue{operator_policy = OpPolicy}) -> OpPolicy;
get_operator_policy(Queue) -> amqqueue_v1:get_operator_policy(Queue).

set_operator_policy(#amqqueue{} = Queue, Policy) ->
    Queue#amqqueue{operator_policy = Policy};
set_operator_policy(Queue, Policy) ->
    amqqueue_v1:set_operator_policy(Queue, Policy).

% name

get_name(#amqqueue{name = Name}) -> Name;
get_name(Queue)                  -> amqqueue_v1:get_name(Queue).

set_name(#amqqueue{} = Queue, Name) ->
    Queue#amqqueue{name = Name};
set_name(Queue, Name) ->
    amqqueue_v1:set_name(Queue, Name).

get_options(#amqqueue{options = Options}) -> Options;
get_options(Queue)                        -> amqqueue_v1:get_options(Queue).

% pid

get_pid(#amqqueue{pid = Pid}) -> Pid;
get_pid(Queue)                -> amqqueue_v1:get_pid(Queue).

set_pid(#amqqueue{} = Queue, Pid) ->
    Queue#amqqueue{pid = Pid};
set_pid(Queue, Pid) ->
    amqqueue_v1:set_pid(Queue, Pid).

% policy

get_policy(#amqqueue{policy = Policy}) -> Policy;
get_policy(Queue) -> amqqueue_v1:get_policy(Queue).

set_policy(#amqqueue{} = Queue, Policy) ->
    Queue#amqqueue{policy = Policy};
set_policy(Queue, Policy) ->
    amqqueue_v1:set_policy(Queue, Policy).

% policy_version

get_policy_version(#amqqueue{policy_version = PV}) ->
    PV;
get_policy_version(Queue) ->
    amqqueue_v1:get_policy_version(Queue).

set_policy_version(#amqqueue{} = Queue, PV) ->
    Queue#amqqueue{policy_version = PV};
set_policy_version(Queue, PV) ->
    amqqueue_v1:set_policy_version(Queue, PV).

% recoverable_slaves

get_recoverable_slaves(#amqqueue{recoverable_slaves = Slaves}) ->
    Slaves;
get_recoverable_slaves(Queue) ->
    amqqueue_v1:get_recoverable_slaves(Queue).

set_recoverable_slaves(#amqqueue{} = Queue, Slaves) ->
    Queue#amqqueue{recoverable_slaves = Slaves};
set_recoverable_slaves(Queue, Slaves) ->
    amqqueue_v1:set_recoverable_slaves(Queue, Slaves).

% quorum_nodes (new in v2)

get_quorum_nodes(#amqqueue{quorum_nodes = Nodes}) -> Nodes;
get_quorum_nodes(_)                               -> [].

set_quorum_nodes(#amqqueue{} = Queue, Nodes) ->
    Queue#amqqueue{quorum_nodes = Nodes};
set_quorum_nodes(Queue, _Nodes) ->
    Queue.

% slave_pids

get_slave_pids(#amqqueue{slave_pids = Slaves}) ->
    Slaves;
get_slave_pids(Queue) ->
    amqqueue_v1:get_slave_pids(Queue).

set_slave_pids(#amqqueue{} = Queue, SlavePids) ->
    Queue#amqqueue{slave_pids = SlavePids};
set_slave_pids(Queue, SlavePids) ->
    amqqueue_v1:set_slave_pids(Queue, SlavePids).

% slave_pids_pending_shutdown

get_slave_pids_pending_shutdown(#amqqueue{slave_pids_pending_shutdown = Slaves}) ->
    Slaves;
get_slave_pids_pending_shutdown(Queue) ->
    amqqueue_v1:get_slave_pids_pending_shutdown(Queue).

set_slave_pids_pending_shutdown(#amqqueue{} = Queue, SlavePids) ->
    Queue#amqqueue{slave_pids_pending_shutdown = SlavePids};
set_slave_pids_pending_shutdown(Queue, SlavePids) ->
    amqqueue_v1:set_slave_pids_pending_shutdown(Queue, SlavePids).

% state

get_state(#amqqueue{state = State}) -> State;
get_state(Queue)                    -> amqqueue_v1:get_state(Queue).

set_state(#amqqueue{} = Queue, State) ->
    Queue#amqqueue{state = State};
set_state(Queue, State) ->
    amqqueue_v1:set_state(Queue, State).

% sync_slave_pids

get_sync_slave_pids(#amqqueue{sync_slave_pids = Pids}) ->
    Pids;
get_sync_slave_pids(Queue) ->
    amqqueue_v1:get_sync_slave_pids(Queue).

set_sync_slave_pids(#amqqueue{} = Queue, Pids) ->
    Queue#amqqueue{sync_slave_pids = Pids};
set_sync_slave_pids(Queue, Pids) ->
    amqqueue_v1:set_sync_slave_pids(Queue, Pids).

%% New in v2.

get_type(Queue) when ?is_amqqueue(Queue) ->
    priv_get_type(Queue).

get_vhost(#amqqueue{vhost = VHost}) -> VHost;
get_vhost(Queue)                    -> amqqueue_v1:get_vhost(Queue).

is_auto_delete(#amqqueue{auto_delete = AutoDelete}) ->
    AutoDelete;
is_auto_delete(Queue) ->
    amqqueue_v1:is_auto_delete(Queue).

is_durable(#amqqueue{durable = Durable}) -> Durable;
is_durable(Queue)                        -> amqqueue_v1:is_durable(Queue).

is_classic(Queue) ->
    get_type(Queue) =:= ?amqqueue_v1_type.

is_quorum(Queue) ->
    get_type(Queue) =:= quorum.

fields() ->
    case quorum_queue_ff_enabled() of
        true  -> record_info(fields, amqqueue);
        false -> amqqueue_v1:fields()
    end.

field_vhost() ->
    case quorum_queue_ff_enabled() of
        true  -> #amqqueue.vhost;
        false -> amqqueue_v1:field_vhost()
    end.

pattern_match_all() ->
    case quorum_queue_ff_enabled() of
        true  -> #amqqueue{_ = '_'};
        false -> amqqueue_v1:pattern_match_all()
    end.

pattern_match_on_name(Name) ->
    case quorum_queue_ff_enabled() of
        true  -> #amqqueue{name = Name, _ = '_'};
        false -> amqqueue_v1:pattern_match_on_name(Name)
    end.

reset_mirroring_and_decorators(#amqqueue{} = Queue) ->
    Queue#amqqueue{slave_pids      = [],
                   sync_slave_pids = [],
                   gm_pids         = [],
                   decorators      = undefined};
reset_mirroring_and_decorators(Queue) ->
    amqqueue_v1:reset_mirroring_and_decorators(Queue).

set_immutable(#amqqueue{} = Queue) ->
    Queue#amqqueue{pid                = none,
                   slave_pids         = none,
                   sync_slave_pids    = none,
                   recoverable_slaves = none,
                   gm_pids            = none,
                   policy             = none,
                   decorators         = none,
                   state              = none};
set_immutable(Queue) ->
    amqqueue_v1:set_immutable(Queue).

qnode(Queue) when ?is_amqqueue(Queue) ->
    QPid = get_pid(Queue),
    qnode(QPid);
qnode(QPid) when is_pid(QPid) ->
    node(QPid);
qnode({_, Node}) ->
    Node.

% private

priv_get_type(#amqqueue{type = Type}) ->
    Type;
priv_get_type(_Queue) ->
    ?amqqueue_v1_type.

macros() ->
    io:format(
      "-define(is_~s(Q), is_record(Q, amqqueue, ~b)).~n~n",
      [?record_version, record_info(size, amqqueue)]),
    %% The field number starts at 2 because the first element is the
    %% record name.
    macros(record_info(fields, amqqueue), 2).

macros([Field | Rest], I) ->
    io:format(
      "-define(~s_field_~s(Q), element(~b, Q)).~n",
      [?record_version, Field, I]),
    macros(Rest, I + 1);
macros([], _) ->
    ok.

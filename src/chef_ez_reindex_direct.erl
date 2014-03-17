%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% Copyright 2014 Chef Software, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

-module(chef_ez_reindex_direct).

-include_lib("chef_objects/include/chef_types.hrl").
-include_lib("ej/include/ej.hrl").

%% A binary() index is taken to be a data bag name.
-type index() :: client | environment | node | role | binary().

-export([
         all_ids/2,
         name_id_dict/2,
         reindex/1,
         reindex/2,
         reindex_from_file/1,
         reindex_from_list/1,
         fetch_org_indexes/1,
         make_context/0
         ]).


%% @doc Reindex the specified `Index' in `OrgName'. This function avoids use of rabbitmq and
%% POSTs updates directly to solr. Flatten/expand is handled by `chef_index_expand'
%% which also takes care of POSTing the data to solr. When this function returns, all
%% objects in the specified index will have been reindexed and received by solr.
%%
%% The `chef_wm' app's `bulk_fetch_batch_size' is used to determine both the size of the
%% batch of objects to `bulk_get' from the db and the size of the multi-doc POSTs sent to
%% solr. Parallelization of flatten/expand is the responsibility of `chef_index_expand' and
%% is not a concern of this code.
-spec reindex('open-source-chef' | binary(), string()) -> [ok].
reindex(OrgName) ->
    DbCtx = make_context(),
    OrgId = chef_db:fetch_org_id(DbCtx, OrgName),
    AllIndexes = fetch_org_indexes(OrgId),
    reindex(DbCtx, OrgId, OrgName, AllIndexes).
reindex(OrgName, OrgId) ->
    DbCtx = make_context(),
    AllIndexes = fetch_org_indexes(OrgId),
    reindex(DbCtx, OrgId, OrgName, AllIndexes).
reindex(DbCtx,OrgId,OrgName, AllIndexes) ->
    [ begin
          NameIdDict = chef_db:create_name_id_dict(DbCtx, Idx, OrgId),
          AllIds = all_ids_from_name_id_dict(NameIdDict),
          error_logger:info_msg("reindex: start ~p ~p ~p ~p",
                                [OrgName, OrgId, Idx, length(AllIds)]),
          BatchSize = envy:get(chef_db, bulk_fetch_batch_size, pos_integer),
          ObjType = chef_object_type(Idx),
          DoBatch = fun(Batch, _Acc) ->
                            Objects = chef_db:bulk_get(DbCtx, OrgName, ObjType, Batch),
                            send_to_solr(OrgId, Idx, Objects, NameIdDict)
                    end,
          chefp:batch_fold(DoBatch, AllIds, ok, BatchSize),
          error_logger:info_msg("reindex: complete ~p ~p ~p",
                                [OrgName, OrgId, Idx]),
          ok
      end || Idx <- AllIndexes ].

reindex_from_file(FileName) ->
    {ok, OrgFile} = file:open(FileName, [read, raw, binary, {read_ahead, 1024}]),
    OrgNames = read_org_file(OrgFile, []),
    reindex_from_list(OrgNames).

reindex_from_list(OrgNames) ->
    [ begin
          reindex(OrgName)
      end || OrgName  <- OrgNames ].

read_org_file(OrgFile, OrgNames) ->
    case file:read_line(OrgFile) of
        {ok, OrgLine} ->
            OrgName = binary:replace(OrgLine, <<"\n">>, <<"">>),
            read_org_file(OrgFile, [OrgName | OrgNames]);
        eof ->
            OrgNames;
        {error, Reason} ->
            {error, Reason}
    end.

all_ids_from_name_id_dict(NameIdDict) ->
    dict:fold(fun(_K, V, Acc) -> [V|Acc] end,
              [],
              NameIdDict).

-spec name_id_dict(binary(), index()) -> dict().
name_id_dict(OrgName, Index) ->
    Ctx = make_context(),
    OrgId = chef_db:fetch_org_id(Ctx, OrgName),
    chef_db:create_name_id_dict(Ctx, Index, OrgId).

-spec all_ids(binary(), index()) -> [binary()].
all_ids(OrgName, Index) ->
    NameIdDict = name_id_dict(OrgName, Index),
    dict:fold(fun(_K, V, Acc) -> [V | Acc] end,
              [], NameIdDict).

make_context() ->
    Time = string:join([ integer_to_list(I) || I <- tuple_to_list(os:timestamp())], "-"),
    ReqId = erlang:iolist_to_binary([atom_to_list(node()),
                                     "-chef_ez_reindex-",
                                     Time
                                    ]),
    chef_db:make_context(ReqId).

fetch_org_indexes(OrgId) ->
    BuiltInIndexes = [node, role, environment, client],
    DataBags = chef_sql:fetch_object_names(#chef_data_bag{org_id = OrgId}),
    BuiltInIndexes ++ DataBags.

chef_object_type(Index) when is_binary(Index) -> data_bag_item;
chef_object_type(Index)                       -> Index.

send_to_solr(_, _, {error, _} = Error, _) ->
    %% handle error from chef_db:bulk_get
    erlang:error(Error);
send_to_solr(OrgId, Index, Objects, NameIdDict) ->
    %% NOTE: we could handle the mapping of Object to Id in the caller and pass in here a
    %% list of {Id, Object} tuples. This might be better?
    SolrCtx = lists:foldl(
      fun(SO, Ctx) ->
              {Id, IndexEjson} = ejson_for_indexing(Index, OrgId, SO, NameIdDict),
              chef_index_expand:add_item(Ctx, Id, IndexEjson, Index, OrgId)
      end, chef_index_expand:init_items(length(Objects)), Objects),
    case chef_index_expand:send_items(SolrCtx) of
        ok ->
            ok;
        {error, Why} ->
            erlang:error({error, {"chef_index_expand:send_items", Why}})
    end.

ejson_for_indexing(Index, OrgId, SO, NameIdDict) ->
    PrelimEJson = decompress_and_decode(SO),
    NameKey = name_key(chef_object_type(Index)),
    ItemName = ej:get({NameKey}, PrelimEJson),
    {ok, ObjectId} = dict:find(ItemName, NameIdDict),
    StubRec = stub_record(Index, OrgId, ObjectId, ItemName, PrelimEJson),
    IndexEjson = chef_object:ejson_for_indexing(StubRec, PrelimEJson),
    {ObjectId, IndexEjson}.

%% All object types are returned from chef_db:bulk_get/4 as
%% binaries (compressed or not) EXCEPT for clients, which are
%% returned as EJson directly, because none of their
%% information is actually stored as a JSON "blob" in the
%% database.
decompress_and_decode(Bin) when is_binary(Bin) ->
    chef_db_compression:decompress_and_decode(Bin);
decompress_and_decode(Object) ->
    Object.

%% @doc Determine the proper key to use to retrieve the unique name of
%% an object from its EJson representation.
name_key(data_bag_item) -> <<"id">>;
name_key(_Type)         -> <<"name">>.

%% The {@link chef_object_db:add_to_solr} expects a Chef object record and EJSON as
%% arguments. Since we have only some meta data and the EJSON, we stub out enough of an
%% object record to work. This is a fragile hack. The reindexing function should retrieve a
%% list of complete records from the db and process those.
stub_record(client, OrgId, Id, Name, _EJson) ->
    #chef_client{org_id = OrgId, id = Id, name = Name};
stub_record(environment, OrgId, Id, Name, _EJson) ->
    #chef_environment{org_id = OrgId, id = Id, name = Name};
stub_record(node, OrgId, Id, Name, EJson) ->
    #chef_node{org_id = OrgId, id = Id, name = Name,
               environment = ej:get({<<"chef_environment">>}, EJson)};
stub_record(role, OrgId, Id, Name, _EJson) ->
    #chef_role{org_id = OrgId, id = Id, name = Name};
stub_record(DataBagName, OrgId, Id, _Name, EJson) ->
    ItemName = ej:get({<<"id">>}, EJson),
    #chef_data_bag_item{org_id = OrgId, id = Id,
                        data_bag_name = DataBagName,
                        item_name = ItemName}.


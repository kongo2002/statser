module Api exposing
  -- nodes API
  ( addNode
  , removeNode
  -- fetch API
  , fetch
  , fetchMetrics
  , fetchNodes
  , fetchAggregations
  , fetchStorages
  -- aggregation API
  , addAggregation
  -- storage API
  , addStorage
  )


import Http
import Json.Encode as Encode
import Json.Decode as Decode

import Types exposing (..)


-- TODO: configurable base url
baseUrl : String
baseUrl = "http://localhost:8080"


url : String -> String
url path =
  baseUrl ++ path


addNode : String -> Cmd Msg
addNode node =
  let url0 = url "/.statser/control/nodes"
      body = Http.jsonBody <| Encode.object [("node", Encode.string node)]
      request = Http.post url0 body Types.mkSuccess
  in  Http.send (BoolResult AddNodeCommand) request


removeNode : String -> Cmd Msg
removeNode node =
  let url0 = url "/.statser/control/nodes"
      body = Http.jsonBody <| Encode.object [("node", Encode.string node)]
      request = delete url0 body Types.mkSuccess
  in  Http.send (BoolResult RemoveNodeCommand) request


addAggregation : Aggregation -> Cmd Msg
addAggregation aggregation =
  let url0 = url "/.statser/control/aggregations"
      body = Http.jsonBody <| encodeAggregation aggregation
      request = Http.post url0 body Types.mkSuccess
  in  Http.send (BoolResult AddAggregationCommand) request


addStorage : Storage -> Cmd Msg
addStorage storage =
  let url0 = url "/.statser/control/storages"
      body = Http.jsonBody <| encodeStorage storage
      request = Http.post url0 body Types.mkSuccess
  in  Http.send (BoolResult AddStorageCommand) request


encodeAggregation : Aggregation -> Encode.Value
encodeAggregation aggregation =
  Encode.object
    [ ("name", Encode.string aggregation.name)
    , ("pattern", Encode.string aggregation.pattern)
    , ("aggregation", Encode.string aggregation.aggregation)
    , ("factor", Encode.float aggregation.factor)
    ]


encodeStorage : Storage -> Encode.Value
encodeStorage storage =
  let rets = List.map Encode.string storage.retentions
  in  Encode.object
    [ ("name", Encode.string storage.name)
    , ("pattern", Encode.string storage.pattern)
    , ("retentions", Encode.list rets)
    ]


fetch : Route -> List (Cmd Msg)
fetch route =
  case route of
    Dashboard -> [ fetchMetrics ]
    Control -> [ fetchNodes, fetchAggregations, fetchStorages ]
    _ -> []


fetchMetrics : Cmd Msg
fetchMetrics =
  let url0 = url "/.statser/metrics"
      request = Http.get url0 Types.mkStats
  in  Http.send StatsUpdate request


fetchNodes : Cmd Msg
fetchNodes =
  let url0 = url "/.statser/control/nodes"
      request = Http.get url0 Types.mkNodes
  in  Http.send NodesUpdate request


fetchAggregations : Cmd Msg
fetchAggregations =
  let url0 = url "/.statser/control/aggregations"
      request = Http.get url0 Types.mkAggregations
  in  Http.send AggregationsUpdate request


fetchStorages : Cmd Msg
fetchStorages =
  let url0 = url "/.statser/control/storages"
      request = Http.get url0 Types.mkStorages
  in  Http.send StoragesUpdate request


delete : String -> Http.Body -> Decode.Decoder a -> Http.Request a
delete url body decoder =
  Http.request
    { method = "DELETE"
    , headers = []
    , url = url
    , body = body
    , expect = Http.expectJson decoder
    , timeout = Nothing
    , withCredentials = False
    }


-- vim: et sw=2 sts=2

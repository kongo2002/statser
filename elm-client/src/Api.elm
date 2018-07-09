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
  , fetchRateLimits
  -- aggregation API
  , addAggregation
  -- storage API
  , addStorage
  -- rate limits API
  , updateRateLimits
  )


import Http
import Json.Encode as Encode
import Json.Decode as Decode

import Types exposing (..)


addNode : String -> Cmd Msg
addNode node =
  let url = "/.statser/control/nodes"
      body = Http.jsonBody <| Encode.object [("node", Encode.string node)]
      request = Http.post url body Types.mkSuccess
  in  Http.send (BoolResult AddNodeCommand) request


removeNode : String -> Cmd Msg
removeNode node =
  let url = "/.statser/control/nodes"
      body = Http.jsonBody <| Encode.object [("node", Encode.string node)]
      request = delete url body Types.mkSuccess
  in  Http.send (BoolResult RemoveNodeCommand) request


addAggregation : Aggregation -> Cmd Msg
addAggregation aggregation =
  let url = "/.statser/control/aggregations"
      body = Http.jsonBody <| encodeAggregation aggregation
      request = Http.post url body Types.mkSuccess
  in  Http.send (BoolResult AddAggregationCommand) request


addStorage : Storage -> Cmd Msg
addStorage storage =
  let url = "/.statser/control/storages"
      body = Http.jsonBody <| encodeStorage storage
      request = Http.post url body Types.mkSuccess
  in  Http.send (BoolResult AddStorageCommand) request


updateRateLimits : List RateLimit -> Cmd Msg
updateRateLimits rateLimits =
  let url = "/.statser/control/settings/ratelimits"
      body = Http.jsonBody <| encodeRateLimits rateLimits
      request = Http.post url body Types.mkRateLimits
  in  Http.send RateLimitsUpdate request


encodeRateLimits : List RateLimit -> Encode.Value
encodeRateLimits limits =
  let limit l =
      Encode.object
        [ ("type", Encode.string l.typ)
        , ("limit", Encode.int l.limit)
        ]
  in  Encode.list (List.map limit limits)


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
    Settings -> [ fetchRateLimits ]
    _ -> []


fetchMetrics : Cmd Msg
fetchMetrics =
  let url = "/.statser/metrics"
      request = Http.get url Types.mkStats
  in  Http.send StatsUpdate request


fetchNodes : Cmd Msg
fetchNodes =
  let url = "/.statser/control/nodes"
      request = Http.get url Types.mkNodes
  in  Http.send NodesUpdate request


fetchAggregations : Cmd Msg
fetchAggregations =
  let url = "/.statser/control/aggregations"
      request = Http.get url Types.mkAggregations
  in  Http.send AggregationsUpdate request


fetchStorages : Cmd Msg
fetchStorages =
  let url = "/.statser/control/storages"
      request = Http.get url Types.mkStorages
  in  Http.send StoragesUpdate request


fetchRateLimits : Cmd Msg
fetchRateLimits =
  let url = "/.statser/control/settings/ratelimits"
      request = Http.get url Types.mkRateLimits
  in  Http.send RateLimitsUpdate request


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

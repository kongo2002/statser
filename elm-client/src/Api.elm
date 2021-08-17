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
  in Http.post { url = url, body = body, expect = Http.expectJson (BoolResult AddNodeCommand) Types.mkSuccess }


removeNode : String -> Cmd Msg
removeNode node =
  let url = "/.statser/control/nodes"
      body = Http.jsonBody <| Encode.object [("node", Encode.string node)]
  in delete url body (BoolResult RemoveNodeCommand) Types.mkSuccess


addAggregation : Aggregation -> Cmd Msg
addAggregation aggregation =
  let url = "/.statser/control/aggregations"
      body = Http.jsonBody <| encodeAggregation aggregation
  in Http.post { url = url, body = body, expect = Http.expectJson (BoolResult AddAggregationCommand) Types.mkSuccess }


addStorage : Storage -> Cmd Msg
addStorage storage =
  let url = "/.statser/control/storages"
      body = Http.jsonBody <| encodeStorage storage
  in Http.post { url = url, body = body, expect = Http.expectJson (BoolResult AddStorageCommand) Types.mkSuccess }


updateRateLimits : List RateLimit -> Cmd Msg
updateRateLimits rateLimits =
  let url = "/.statser/control/settings/ratelimits"
      body = Http.jsonBody <| encodeRateLimits rateLimits
  in Http.post { url = url, body = body, expect = Http.expectJson RateLimitsUpdate Types.mkRateLimits }


encodeRateLimits : List RateLimit -> Encode.Value
encodeRateLimits limits =
  let limit l =
          Encode.object
            [ ("type", Encode.string l.typ)
            , ("limit", Encode.int l.limit)
            ]
  in  Encode.list limit limits


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
  Encode.object
    [ ("name", Encode.string storage.name)
    , ("pattern", Encode.string storage.pattern)
    , ("retentions", Encode.list Encode.string storage.retentions)
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
  in Http.get { url = url, expect = Http.expectJson StatsUpdate Types.mkStats }


fetchNodes : Cmd Msg
fetchNodes =
  let url = "/.statser/control/nodes"
  in Http.get { url = url, expect = Http.expectJson NodesUpdate Types.mkNodes }


fetchAggregations : Cmd Msg
fetchAggregations =
  let url = "/.statser/control/aggregations"
  in Http.get { url = url, expect = Http.expectJson AggregationsUpdate Types.mkAggregations }


fetchStorages : Cmd Msg
fetchStorages =
  let url = "/.statser/control/storages"
  in Http.get { url = url, expect = Http.expectJson StoragesUpdate Types.mkStorages }


fetchRateLimits : Cmd Msg
fetchRateLimits =
  let url = "/.statser/control/settings/ratelimits"
  in Http.get { url = url, expect = Http.expectJson RateLimitsUpdate Types.mkRateLimits }


--delete : String -> Http.Body -> Decode.Decoder a -> Http.Request a
delete url body msg decoder =
  Http.request
    { method = "DELETE"
    , headers = []
    , url = url
    , body = body
    , expect = Http.expectJson msg decoder
    , timeout = Nothing
    , tracker = Nothing
    }


-- vim: et sw=2 sts=2

module Api exposing ( addNode, removeNode, fetch, fetchMetrics, fetchNodes, fetchAggregations, fetchStorages )

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


addNode node =
  let url0 = url "/.statser/control/nodes"
      body = Http.jsonBody <| Encode.object [("node", Encode.string node)]
      request = Http.post url0 body Types.mkSuccess
  in  Http.send (BoolResult AddNodeCommand) request


removeNode node =
  let url0 = url "/.statser/control/nodes"
      body = Http.jsonBody <| Encode.object [("node", Encode.string node)]
      request = delete url0 body Types.mkSuccess
  in  Http.send (BoolResult RemoveNodeCommand) request


fetch route =
  case route of
    Dashboard -> [ fetchMetrics ]
    Control -> [ fetchNodes, fetchAggregations, fetchStorages ]
    _ -> []


fetchMetrics =
  let url0 = url "/.statser/metrics"
      request = Http.get url0 Types.mkStats
  in  Http.send StatsUpdate request


fetchNodes =
  let url0 = url "/.statser/control/nodes"
      request = Http.get url0 Types.mkNodes
  in  Http.send NodesUpdate request


fetchAggregations =
  let url0 = url "/.statser/control/aggregations"
      request = Http.get url0 Types.mkAggregations
  in  Http.send AggregationsUpdate request


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

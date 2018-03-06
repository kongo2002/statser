module Api exposing ( addNode, fetch, fetchMetrics, fetchNodes )

import Http
import Json.Encode as Encode

import Types exposing (..)


-- TODO: configurable base url
baseUrl : String
baseUrl = "http://localhost:8080"


addNode node =
  let url = baseUrl ++ "/.statser/control/nodes"
      body = Http.jsonBody <| Encode.object [("node", Encode.string node)]
      request = Http.post url body Types.mkSuccess
  in  Http.send (BoolResult AddNodeCommand) request


fetch route =
  case route of
    Dashboard -> [ fetchMetrics ]
    Control -> [ fetchNodes ]
    _ -> []


fetchMetrics =
  let url = baseUrl ++ "/.statser/metrics"
      request = Http.get url Types.mkStats
  in  Http.send StatsUpdate request


fetchNodes =
  let url = baseUrl ++ "/.statser/control/nodes"
      request = Http.get url Types.mkNodes
  in  Http.send NodesUpdate request

-- vim: et sw=2 sts=2

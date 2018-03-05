module Api exposing ( fetch, fetchMetrics, fetchNodes )

import Http

import Types exposing (..)


-- TODO: configurable base url
baseUrl : String
baseUrl = "http://localhost:8080"


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

module Routing exposing ( parse, dashboard, control, login, routeToPath )

import Navigation exposing ( Location )
import UrlParser exposing (..)

import Types


dashboard : String
dashboard = "#"


control : String
control = "#control"


login : String
login = "#login"


routeToPath : Types.Route -> String
routeToPath route =
  case route of
    Types.Dashboard -> dashboard
    Types.Control -> control
    Types.Login -> login


parse : Location -> Types.Route
parse location =
  case parseHash matchers location of
    Just route -> route
    Nothing    -> Types.Dashboard


matchers : Parser (Types.Route -> a) a
matchers =
  oneOf
    [ map Types.Dashboard top
    , map Types.Control (s "control")
    ]

-- vim: et sw=2 sts=2

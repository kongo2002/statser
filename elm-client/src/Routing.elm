module Routing exposing ( parse, dashboard, control, login )

import Navigation exposing ( Location )
import UrlParser exposing (..)

import Types


dashboard : String
dashboard = "#"


control : String
control = "#control"


login : String
login = "#login"


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

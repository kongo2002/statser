module Routing exposing ( parse, dashboard, control, login, routeToPath )

import Url
import Url.Parser exposing (..)

import Types


dashboard : String
dashboard = "#"


control : String
control = "#control"


login : String
login = "#login"


settings : String
settings = "#settings"


routeToPath : Types.Route -> String
routeToPath route =
  case route of
    Types.Dashboard -> dashboard
    Types.Control -> control
    Types.Login -> login
    Types.Settings -> settings


parse : Url.Url -> Types.Route
parse location =
  case Url.Parser.parse matchers location of
    Just route -> route
    Nothing    -> Types.Dashboard


matchers : Parser (Types.Route -> a) a
matchers =
  let frag = string </> string </> fragment identity
      toRoute _ _ arg =
        case arg of
          Just "" -> Types.Dashboard
          Just "control" -> Types.Control
          Just "settings" -> Types.Settings
          Just "login" -> Types.Login
          _ -> Types.Dashboard
  in map toRoute frag

-- vim: et sw=2 sts=2

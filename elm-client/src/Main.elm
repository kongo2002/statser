module Main exposing (..)

import Dict
import Navigation
import Time exposing ( second )

import Api
import Fields
import Ports
import Routing
import Types exposing (..)
import View


defaultLiveMetric : String
defaultLiveMetric = "committed-points"


historyEntries : Int
historyEntries = 20


emptyModel : Route -> Model
emptyModel route =
  let history = StatHistory 0 Dict.empty
  in  Model route history Dict.empty [] defaultLiveMetric [] [] [] [] Dict.empty


init : Navigation.Location -> ( Model, Cmd Msg )
init location =
  let route = Routing.parse location
  in  (emptyModel route ! Api.fetch route)


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
  case msg of
    NoOp ->
      model ! []

    Tick time ->
      model ! [ Api.fetchMetrics ]

    NavigationChange location ->
      let newRoute = Routing.parse location
          newModel = { model | route = newRoute }
      in  newModel ! Api.fetch newRoute

    StatsUpdate (Ok (newStats, newHealths, ts)) ->
      let newModel = { model |
              stats = newStats,
              healths = newHealths,
              history = toHistory model.history ts newStats }

          update = getEntries newModel
      in  newModel ! [ update ]

    StatsUpdate _ ->
      model ! []

    NodesUpdate (Ok nodes) ->
      let newModel = { model | nodes = nodes }
      in  newModel ! []

    NodesUpdate _ ->
      model ! []

    AggregationsUpdate (Ok aggregations) ->
      let newModel = { model | aggregations = aggregations }
      in  newModel ! []

    AggregationsUpdate _ ->
      model ! []

    StoragesUpdate (Ok storages) ->
      let newModel = { model | storages = storages }
      in  newModel ! []

    StoragesUpdate _ ->
      model ! []

    RateLimitsUpdate (Ok rateLimits) ->
      let newModel = { model | rateLimits = rateLimits }
      in  newModel ! []

    RateLimitsUpdate _ ->
      model ! []

    -- TODO: this 'BoolResult' pattern is an abstraction candidate for sure

    BoolResult AddNodeCommand (Ok True) ->
      model ! [ Ports.notification ("successfully connected", "primary"), Api.fetchNodes ]

    BoolResult AddNodeCommand res ->
      model ! [ Ports.notification ("failed to connect to node", "danger") ]

    BoolResult RemoveNodeCommand (Ok True) ->
      model ! [ Ports.notification ("successfully disconnected", "primary"), Api.fetchNodes ]

    BoolResult RemoveNodeCommand res ->
      model ! [ Ports.notification ("failed to disconnect from node", "danger") ]

    BoolResult AddAggregationCommand (Ok True) ->
      model ! [ Ports.notification ("successfully added new aggregation rule", "primary")
              , Api.fetchAggregations ]

    BoolResult AddAggregationCommand res ->
      model ! [ Ports.notification ("failed to add new aggregation rule", "danger") ]

    BoolResult AddStorageCommand (Ok True) ->
      model ! [ Ports.notification ("successfully added new storage rule", "primary")
              , Api.fetchStorages ]

    BoolResult AddStorageCommand res ->
      model ! [ Ports.notification ("failed to add new storage rule", "danger") ]

    PickLiveMetric metric ->
      let newModel = { model | liveMetric = metric }
          update = getEntries newModel
      in  newModel ! [ update ]

    AddNode ->
      let nodeName = Fields.getFieldEmpty NodeNameKey model
      in  model ! [ Api.addNode nodeName ]

    RemoveNode node ->
      model ! [ Api.removeNode node ]

    AddAggregation ->
      let agg = Fields.getAggregation model
      in  model ! [ Api.addAggregation agg ]

    AddStorage ->
      let storage = Fields.getStorage model
      in  model ! [ Api.addStorage storage ]

    UpdateRateLimits ->
      let rateLimits = Fields.getRateLimits model
      in  model ! [ Api.updateRateLimits rateLimits ]

    SetField key value ->
      let newModel = Fields.setField key model value
      in  newModel ! []


getEntries : Model -> Cmd Msg
getEntries model =
  let liveMetric = model.liveMetric
      entries = Maybe.withDefault [] (Dict.get liveMetric model.history.entries)
  in  Ports.liveUpdate (liveMetric, entries)


toHistory : StatHistory -> Int -> (Dict.Dict String Stat) -> StatHistory
toHistory history ts stats =
  let inits = List.repeat (historyEntries - 1) (ts, 0.0)
      empties =
        List.foldl (\_ (last, acc) -> (last - 10, (last - 10, 0.0) :: acc)) (ts, []) inits
        |> Tuple.second
        |> List.reverse

      update k stat hist =
        case Dict.get k hist.entries of
          Just stats ->
            let stats0 = (ts, stat.value) :: stats
                stats1 = List.take historyEntries stats0
                es = Dict.insert k stats1 hist.entries
            in  { hist | entries = es }
          Nothing ->
            let stats = (ts, stat.value) :: empties
                es    = Dict.insert k stats hist.entries
            in  { hist | entries = es }

  in  Dict.foldr update history stats


subTicks : Model -> Sub Msg
subTicks model =
  Time.every (10 * second) Tick


main : Program Never Model Msg
main =
  Navigation.program NavigationChange
    { init = init
    , view = View.view
    , update = update
    , subscriptions = subTicks
    }

-- vim: et sw=2 sts=2

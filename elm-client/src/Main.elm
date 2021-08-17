module Main exposing (..)

import Dict
import Browser
import Browser.Navigation
import Time
import Url

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


emptyModel : Browser.Navigation.Key -> Route -> Model
emptyModel key route =
  let history = StatHistory 0 Dict.empty
  in  Model route key history Dict.empty [] defaultLiveMetric [] [] [] [] Dict.empty


init : Flags -> Url.Url -> Browser.Navigation.Key -> ( Model, Cmd Msg )
init flags location key =
  let route = Routing.parse location
  in  (emptyModel key route, Cmd.batch (Api.fetch route))


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
  case msg of
    NoOp ->
      (model , Cmd.none)

    Tick time ->
      (model , Cmd.batch [ Api.fetchMetrics ])

    NavigationChange location ->
      let newRoute = Routing.parse location
          newModel = { model | route = newRoute }
      in  (newModel , Cmd.batch (Api.fetch newRoute))

    NavigationRequest request ->
      case request of
        Browser.Internal url ->
          ( model
          , Browser.Navigation.pushUrl model.key (Url.toString url)
          )
        Browser.External url ->
          ( model, Browser.Navigation.load url )

    StatsUpdate (Ok (newStats, newHealths, ts)) ->
      let newModel = { model |
              stats = newStats,
              healths = newHealths,
              history = toHistory model.history ts newStats }

          update0 = getEntries newModel
      in  (newModel , Cmd.batch [ update0 ])

    StatsUpdate _ ->
      (model , Cmd.none)

    NodesUpdate (Ok nodes) ->
      let newModel = { model | nodes = nodes }
      in  (newModel , Cmd.none)

    NodesUpdate _ ->
      (model , Cmd.none)

    AggregationsUpdate (Ok aggregations) ->
      let newModel = { model | aggregations = aggregations }
      in  (newModel , Cmd.none)

    AggregationsUpdate _ ->
      (model , Cmd.none)

    StoragesUpdate (Ok storages) ->
      let newModel = { model | storages = storages }
      in  (newModel , Cmd.none)

    StoragesUpdate _ ->
      (model , Cmd.none)

    RateLimitsUpdate (Ok rateLimits) ->
      let newModel = { model | rateLimits = rateLimits }
      in  (newModel , Cmd.none)

    RateLimitsUpdate _ ->
      (model , Cmd.none)

    -- TODO: this 'BoolResult' pattern is an abstraction candidate for sure

    BoolResult AddNodeCommand (Ok True) ->
      (model , Cmd.batch [ Ports.notification ("successfully connected", "primary"), Api.fetchNodes ])

    BoolResult AddNodeCommand res ->
      (model , Cmd.batch [ Ports.notification ("failed to connect to node", "danger") ])

    BoolResult RemoveNodeCommand (Ok True) ->
      (model , Cmd.batch [ Ports.notification ("successfully disconnected", "primary"), Api.fetchNodes ])

    BoolResult RemoveNodeCommand res ->
      (model , Cmd.batch [ Ports.notification ("failed to disconnect from node", "danger") ])

    BoolResult AddAggregationCommand (Ok True) ->
      (model , Cmd.batch [ Ports.notification ("successfully added new aggregation rule", "primary")
      , Api.fetchAggregations ])

    BoolResult AddAggregationCommand res ->
      (model , Cmd.batch [ Ports.notification ("failed to add new aggregation rule", "danger") ])

    BoolResult AddStorageCommand (Ok True) ->
      (model , Cmd.batch [ Ports.notification ("successfully added new storage rule", "primary")
      , Api.fetchStorages ])

    BoolResult AddStorageCommand res ->
      (model , Cmd.batch [ Ports.notification ("failed to add new storage rule", "danger") ])

    PickLiveMetric metric ->
      let newModel = { model | liveMetric = metric }
          update0 = getEntries newModel
      in  (newModel , Cmd.batch [ update0 ])

    AddNode ->
      let nodeName = Fields.getFieldEmpty NodeNameKey model
      in  (model , Cmd.batch [ Api.addNode nodeName ])

    RemoveNode node ->
      (model , Cmd.batch [ Api.removeNode node ])

    AddAggregation ->
      let agg = Fields.getAggregation model
      in  (model , Cmd.batch [ Api.addAggregation agg ])

    AddStorage ->
      let storage = Fields.getStorage model
      in  (model , Cmd.batch [ Api.addStorage storage ])

    UpdateRateLimits ->
      let rateLimits = Fields.getRateLimits model
      in  (model , Cmd.batch [ Api.updateRateLimits rateLimits ])

    SetField key value ->
      let newModel = Fields.setField key model value
      in  (newModel , Cmd.none)


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

      update0 k stat hist =
        case Dict.get k hist.entries of
          Just stats_ ->
            let stats0 = (ts, stat.value) :: stats_
                stats1 = List.take historyEntries stats0
                es = Dict.insert k stats1 hist.entries
            in  { hist | entries = es }
          Nothing ->
            let stats_ = (ts, stat.value) :: empties
                es    = Dict.insert k stats_ hist.entries
            in  { hist | entries = es }

  in  Dict.foldr update0 history stats


subTicks : Model -> Sub Msg
subTicks model =
  Time.every 10000.0 Tick


main =
  Browser.application
    { init = init
    , view = View.document
    , update = update
    , onUrlChange = NavigationChange
    , onUrlRequest = NavigationRequest
    , subscriptions = subTicks
    }

-- vim: et sw=2 sts=2

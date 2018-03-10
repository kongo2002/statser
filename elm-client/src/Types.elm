module Types exposing (..)

import Array
import Date
import Dict
import Http
import Navigation exposing ( Location )
import Json.Decode exposing (..)
import Time exposing ( Time )

-- TYPES

type Msg
  = NoOp
  -- internals
  | NavigationChange Location
  | Tick Time
  -- response types
  | StatsUpdate (Result Http.Error (Dict.Dict String Stat, List Health, Int))
  | NodesUpdate (Result Http.Error (List Node))
  | AggregationsUpdate (Result Http.Error (List Aggregation))
  | StoragesUpdate (Result Http.Error (List Storage))
  | BoolResult Command (Result Http.Error Bool)
  -- commands
  | PickLiveMetric String
  | UpdateNode String
  | AddNode
  | RemoveNode String


type Command
  = AddNodeCommand
  | RemoveNodeCommand


type Route
  = Dashboard
  | Control
  | Login


type alias Model =
  { route : Route
  , history : StatHistory
  , stats : Dict.Dict String Stat
  , healths : List Health
  , liveMetric : String
  , nodes : List Node
  , addNode : String
  , aggregations : List Aggregation
  , storages : List Storage
  }


type alias StatHistory =
  { lastTimestamp : Int
  , entries : Dict.Dict String (List ( Int, Float ))
  }


type alias Stat =
  { name : String
  , value : Float
  , typ : StatType
  }


type alias Health =
  { name : String
  , good : Bool
  , timestamp : Date.Date
  }


type StatType
  = Average
  | Counter


type alias Aggregation =
  { name : String
  , aggregation : String
  , pattern : String
  , factor : Float
  }


type alias Storage =
  { name : String
  , pattern : String
  -- TODO: this might be an explicit type 'Retention'
  , retentions : List String
  }


type alias Node =
  { node : String
  , state : NodeState
  , self : Bool
  }


type NodeState
  = Connected
  | Disconnected


-- DECODERS

mkSuccess : Decoder Bool
mkSuccess =
  (field "success" bool)


mkStats : Decoder (Dict.Dict String Stat, List Health, Int)
mkStats =
  let stats    = field "stats" (list mkStat)
      keyed vs = Dict.fromList (List.map (\x -> (x.name, x)) vs)
      healths  = field "health" (list mkHealth)
      ts       = field "timestamp" int
  in  map3 (,,) (map keyed stats) healths ts


mkHealth : Decoder Health
mkHealth =
  map3 Health
    (field "name" string)
    (field "good" bool)
    (field "timestamp" decodeDate)


decodeDate : Decoder Date.Date
decodeDate =
  let toDate epoch = Date.fromTime (epoch * 1000)
  in  map toDate float


mkStat : Decoder Stat
mkStat =
  map3 Stat
    (field "name" string)
    (field "value" float)
    (field "type" mkStatType)


mkStatType : Decoder StatType
mkStatType =
  string |> map statType


statType : String -> StatType
statType stat =
  case stat of
    "average" -> Average
    _ -> Counter


mkAggregations : Decoder (List Aggregation)
mkAggregations =
  list mkAggregation


mkAggregation : Decoder Aggregation
mkAggregation =
  map4 Aggregation
    (field "name" string)
    (field "aggregation" string)
    (field "pattern" string)
    (field "factor" float)


mkStorages : Decoder (List Storage)
mkStorages =
  list mkStorage


mkStorage : Decoder Storage
mkStorage =
  map3 Storage
    (field "name" string)
    (field "pattern" string)
    (field "retentions" (list string))


mkNodes : Decoder (List Node)
mkNodes =
  list mkNode


mkNode : Decoder Node
mkNode =
  map3 Node
    (field "node" string)
    (field "state" mkNodeState)
    (field "self" bool)


mkNodeState : Decoder NodeState
mkNodeState =
  string |> map nodeState


nodeState : String -> NodeState
nodeState state =
  case state of
    "connected" -> Connected
    _ -> Disconnected


-- vim: et sw=2 sts=2

module Types exposing (..)

import Browser
import Browser.Navigation
import Dict
import Http
import Json.Decode exposing (..)
import Time exposing ( Posix )
import Url exposing ( Url )

-- TYPES

type Msg
  = NoOp
  -- internals
  | NavigationChange Url
  | NavigationRequest Browser.UrlRequest
  | Tick Posix
  -- response types
  | StatsUpdate (Result Http.Error (Dict.Dict String Stat, List Health, Int))
  | NodesUpdate (Result Http.Error (List Node))
  | AggregationsUpdate (Result Http.Error (List Aggregation))
  | StoragesUpdate (Result Http.Error (List Storage))
  | RateLimitsUpdate (Result Http.Error (List RateLimit))
  | BoolResult Command (Result Http.Error Bool)
  -- commands
  | PickLiveMetric String
  | AddNode
  | RemoveNode String
  | AddAggregation
  | AddStorage
  | UpdateRateLimits
  -- fields
  | SetField FieldKey String


type Command
  = AddNodeCommand
  | RemoveNodeCommand
  | AddAggregationCommand
  | AddStorageCommand


type Route
  = Dashboard
  | Control
  | Settings
  | Login


type FieldKey
  -- add node form
  = NodeNameKey
  -- add aggregation form
  | AggregationNameKey
  | AggregationPatternKey
  | AggregationAggregationKey
  | AggregationFactorKey
  -- add storage form
  | StorageNameKey
  | StoragePatternKey
  | StorageRetentionsKey
  -- update rate limits form
  | RateLimitsKey String


type alias Flags = {}


type alias Model =
  { route : Route
  , key : Browser.Navigation.Key
  , history : StatHistory
  , stats : Dict.Dict String Stat
  , healths : List Health
  , liveMetric : String
  , nodes : List Node
  , aggregations : List Aggregation
  , storages : List Storage
  , rateLimits : List RateLimit
  , fields : Dict.Dict Int String
  }


type alias StatHistory =
  { lastTimestamp : Int
  , entries : Dict.Dict String (List ( Int, Float ))
  }


type alias RateLimit =
  { typ : String
  , limit : Int
  }


type alias Stat =
  { name : String
  , value : Float
  , typ : StatType
  }


type alias Health =
  { name : String
  , good : Bool
  , timestamp : Posix
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


-- FIELDS

fieldKey : FieldKey -> Int
fieldKey key =
  -- I would love to use the FieldKey as Dict key but it's not comparable...
  case key of
    NodeNameKey               -> 1
    AggregationNameKey        -> 2
    AggregationPatternKey     -> 3
    AggregationAggregationKey -> 4
    AggregationFactorKey      -> 5
    StorageNameKey            -> 6
    StoragePatternKey         -> 7
    StorageRetentionsKey      -> 8
    RateLimitsKey "create"    -> 9
    RateLimitsKey _           -> 10


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
      toTuple a b c = (a, b, c)
  in  map3 toTuple (map keyed stats) healths ts


mkHealth : Decoder Health
mkHealth =
  map3 Health
    (field "name" string)
    (field "good" bool)
    (field "timestamp" decodeDate)


decodeDate : Decoder Posix
decodeDate =
  let toDate epoch = Time.millisToPosix (epoch * 1000)
  in  map toDate int


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


mkRateLimits : Decoder (List RateLimit)
mkRateLimits =
  list mkRateLimit


mkRateLimit : Decoder RateLimit
mkRateLimit =
  map2 RateLimit
    (field "type" string)
    (field "limit" int)


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

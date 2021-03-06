module Fields exposing
  ( getField
  , getFieldEmpty
  , setField
  , getAggregation
  , getStorage
  , getRateLimits
  )

import Dict

import Types exposing (..)
import Utils


getAggregation : Model -> Aggregation
getAggregation model =
  -- TODO: this feels pretty 'boiler-platey'...
  let name = getFieldEmpty AggregationNameKey model
      pattern = getFieldEmpty AggregationPatternKey model
      agg = getFieldEmpty AggregationAggregationKey model
      factor =
        getField AggregationFactorKey model
        |> Maybe.andThen Utils.tryFloat
        |> Maybe.withDefault 0.5
  in  Aggregation name pattern agg factor


getStorage : Model -> Storage
getStorage model =
  -- TODO: this feels pretty 'boiler-platey'...
  let name = getFieldEmpty StorageNameKey model
      pattern = getFieldEmpty StoragePatternKey model
      rets = getFieldEmpty StorageRetentionsKey model
      retentions = Utils.splitByCommas rets
  in  Storage name pattern retentions


getRateLimits : Model -> List RateLimit
getRateLimits model =
  let getLimit limit =
        getField (RateLimitsKey limit.typ) model
        |> Maybe.andThen Utils.tryInt
        |> Maybe.map (\l -> RateLimit limit.typ l)
      limits =
        model.rateLimits
        |> List.filterMap getLimit
  in  limits


setField : FieldKey -> Model -> String -> Model
setField key model value =
  let comp = fieldKey key
      fields = Dict.insert comp value model.fields
  in  { model | fields = fields }


getField : FieldKey -> Model -> Maybe String
getField key model =
  let comp = fieldKey key
  in  Dict.get comp model.fields


getFieldEmpty : FieldKey -> Model -> String
getFieldEmpty key model =
  getField key model
  |> Maybe.withDefault ""


-- vim: et sw=2 sts=2

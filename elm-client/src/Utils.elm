module Utils exposing (..)

import Regex


tryInt : String -> Maybe Int
tryInt input =
  String.toInt input
  |> Result.toMaybe


tryFloat : String -> Maybe Float
tryFloat input =
  String.toFloat input
  |> Result.toMaybe


tryFloatDefault : Float -> String -> Float
tryFloatDefault default =
  tryFloat >> Maybe.withDefault default


splitByCommas : String -> List String
splitByCommas input =
  let rgx = Regex.regex "[ \\t,]+"
      parts = Regex.split Regex.All rgx input
  in  List.filter (String.isEmpty >> not) parts


-- vim: et sw=2 sts=2

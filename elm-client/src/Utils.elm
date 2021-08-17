module Utils exposing (..)

import Regex


tryInt : String -> Maybe Int
tryInt input =
  String.toInt input


tryFloat : String -> Maybe Float
tryFloat input =
  String.toFloat input


tryFloatDefault : Float -> String -> Float
tryFloatDefault default =
  tryFloat >> Maybe.withDefault default


splitByCommas : String -> List String
splitByCommas input =
  let rgx = Regex.fromString "[ \\t,]+" |> Maybe.withDefault Regex.never
      parts = Regex.split rgx input
  in  List.filter (String.isEmpty >> not) parts


-- vim: et sw=2 sts=2

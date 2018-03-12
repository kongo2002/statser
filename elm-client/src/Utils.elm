module Utils exposing (..)

import Regex


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
  in  Regex.split Regex.All rgx input


-- vim: et sw=2 sts=2

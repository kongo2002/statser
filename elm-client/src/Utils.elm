module Utils exposing (..)


tryFloat : String -> Maybe Float
tryFloat input =
  String.toFloat input
  |> Result.toMaybe


tryFloatDefault : Float -> String -> Float
tryFloatDefault default =
  tryFloat >> Maybe.withDefault default


-- vim: et sw=2 sts=2

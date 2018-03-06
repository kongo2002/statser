port module Ports exposing ( liveUpdate, notification )

port liveUpdate : (String, List (Int, Float)) -> Cmd msg

port notification : (String, String) -> Cmd msg

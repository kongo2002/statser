port module Ports exposing ( liveUpdate )

port liveUpdate : (String, List (Int, Float)) -> Cmd msg

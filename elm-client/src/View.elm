module View exposing ( view )

import Dict
import Html exposing (..)
import Html.Attributes exposing (..)
import Html.Events exposing (..)
import List
import Svg
import Svg.Attributes

import Routing
import Types exposing (..)


-- SVG related constants

svgHeight = 240
svgWidth  = 560
svgMargin = 50


view : Model -> Html Msg
view model =
  let body = div [ class "uk-container" ] <| content model
  in  div [] [ viewNavigation model, body ]


content : Model -> List (Html Msg)
content model =
  case model.route of
    Dashboard ->
      viewDashboard model
    Control ->
      viewControl model
    Login ->
      -- TODO
      viewDashboard model


viewLead =
  p [ class "uk-text-lead" ]
    [ text "Find the most important operation metrics, statistics and health checkpoints on statser below." ]


viewMetrics model =
  let stat x = tr []
        [ td [ onClick (PickLiveMetric x.name) ] [ text x.name ]
        , td [] [ text (toString x.value), val x ]
        ]
      val x =
        case x.typ of
          Counter -> small [] [ text " /sec" ]
          Average -> small [] [ text " average" ]
  in
    [ h2 [] [ text "Metrics" ]
    , p [] [ text "Statistics on some of the most important performance characteristics of this statser instance." ]
    , table
      [ id "metrics"
      , class "uk-table"
      , class "uk-table-divider"
      , class "uk-table-responsive"
      , class "uk-table-hover"
      ]
      [ thead [] [ th [] [ text "Statistic" ], th [] [ text "Metric" ] ]
      , tbody [] (List.map stat (Dict.values model.stats))
      ]
    ]


viewHealths model =
  let health x =
        [ dt [] [ text x.name ]
        , dd []
          [ icon x
          , small [] [ text " last seen at ", text (toString x.timestamp) ]
          ]
        ]
      icon x =
        case x.good of
          True -> span [ class "uk-label", class "uk-label-success", class "uk-icon", attribute "uk-icon" "icon: check" ] []
          False -> span [ class "uk-label", class "uk-label-danger", class "uk-icon", attribute "uk-icon" "icon: close" ] []
  in
    [ h2 [] [ text "Healths" ]
    , p [] [ text "Health checks on statser's main operational components." ]
    , div [ id "health" ]
      [ dl [ class "uk-description-list" ]
        (List.concatMap health model.healths)
      ]
    ]


viewLive =
  let width = svgWidth - 2 * svgMargin
      height = svgHeight - 2 * svgMargin
      translate = "translate(" ++ toString svgMargin ++ "," ++ toString svgMargin ++ ")"
  in
    [ h2 [] [ text "Live" ]
    , div [ id "live", class "graph" ]
      [Svg.svg
        [ Svg.Attributes.width (toString svgWidth)
        , Svg.Attributes.height (toString svgHeight) ]
        [ Svg.g [ attribute "transform" translate ]
          [ Svg.path [ Svg.Attributes.class "line" ] []
          , Svg.text_ [ Svg.Attributes.class "legend", Svg.Attributes.x "10", Svg.Attributes.y "20" ] []
          , Svg.g [ Svg.Attributes.class "xaxis", attribute "transform" ("translate(0," ++ toString height ++ ")") ] []
          , Svg.g [ Svg.Attributes.class "yaxis" ] []
          ]
        ]
      ]
    ]


viewControl model =
  let header name = th [] [ text name ]
      node elem =
        let self =
            case elem.self of
              True -> [ text elem.node, small [] [ text " (self)" ] ]
              False -> []
            state =
              case elem.state of
                Connected -> [icon "happy", text " connected"]
                Disconnected -> [text "disconnected"]
            actions =
              case elem.self of
                False -> [ a [ class "uk-icon-button", attribute "uk-icon" "ban" ] [] ]
                True -> []
        in tr [] [ td [] self, td [] state, td [] actions ]
  in
    [ h2 [] [ text "Nodes" ]
    , table
      [ id "nodes"
      , class "uk-table"
      , class "uk-table-divider"
      , class "uk-table-responsive"
      ]
      [ thead [] [ header "Node", header "Status", header "Actions" ]
      , tbody [] (List.map node model.nodes)
      ]
    , div [ class "uk-inline", class "uk-align-right" ]
      [ input
        [ class "uk-input"
        , attribute "placeholder" "statser@node"
        , onInput UpdateNode
        ] []
      , a [ class "uk-form-icon uk-form-icon-flip"
          , attribute "uk-icon" "plus-circle"
          , onClick AddNode
          ] []
      ]
    ]


icon name =
  span [ class "uk-icon", attribute "uk-icon" name ] []


viewDashboard model =
  [ viewLead , viewMeta ] ++ viewHealths model ++ viewLive ++ viewMetrics model


viewMeta =
  div [ class "uk-text-meta" ]
    [ text "This page's content is updated every 10 seconds. If this does not work for you, please try the "
    , a [ href "/.statser/health" ] [ text "JSON endpoint" ]
    , text " instead"
    ]


viewNavigation model =
  let
    link name route =
      let ref = [ attribute "href" (Routing.routeToPath route) ]
          active = route == model.route
      in  li [ classList [ ( "uk-active", active ) ] ] [ a ref [ text name ] ]
  in
    node "nav"
    [ class "uk-navbar-container", class "uk-navbar", attribute "uk-navbar" "" ]
    [ div [ class "uk-navbar-left" ]
      [ ul [ class "uk-navbar-nav" ]
        [ link "statser" Dashboard
        , link "dashboard" Dashboard
        , link "control" Control
        ]
      ]
    , div [ class "uk-navbar-right" ]
      [ ul [ class "uk-navbar-nav" ]
        [ link "login" Login ]
      ]
    ]

-- vim: et sw=2 sts=2
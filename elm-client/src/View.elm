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

svgHeight : Int
svgHeight = 240

svgWidth : Int
svgWidth  = 560

svgMargin : Int
svgMargin = 50


view : Model -> Html Msg
view model =
  let body = div [ class "uk-container" ] <| content model
      section = div [ class "uk-section" ] [ body ]
  in  div [] [ viewNavigation model, section ]


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


viewLead : Html Msg
viewLead =
  p [ class "uk-text-lead" ]
    [ text "Find the most important operation metrics, statistics and health checkpoints on statser below." ]


viewMetrics : Model -> List (Html Msg)
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


viewHealths : Model -> List (Html Msg)
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


viewLive : List (Html Msg)
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


viewControl : Model -> List (Html Msg)
viewControl model =
  let nodes = viewNodes model
      aggregations = viewAggregations model
      storages = viewStorages model
  in  storages ++ aggregations ++ nodes


header : String -> Html Msg
header name = th [] [ text name ]


viewNodes : Model -> List (Html Msg)
viewNodes model =
  let node elem =
        let self =
            case elem.self of
              True -> [ text elem.node, small [] [ text " (self)" ] ]
              False -> [ text elem.node ]
            state =
              case elem.state of
                Connected -> [icon "happy", text " connected"]
                Disconnected -> [text "disconnected"]
            actions =
              case elem.self of
                False -> [ a [ class "uk-icon-button", attribute "uk-icon" "ban", onClick (RemoveNode elem.node) ] [] ]
                True -> []
        in tr [] [ td [ class "uk-text-bold" ] self, td [] state, td [] actions ]
  in
    [ h2 [] [ text "Nodes" ]
    , p [] [ text "Find all currently connected and known, formerly connected statser nodes below." ]
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
        , onInput (SetField NodeNameKey)
        ] []
      , a [ class "uk-form-icon uk-form-icon-flip"
          , attribute "uk-icon" "plus-circle"
          , onClick AddNode
          ] []
      ]
    ]


setFieldInput : FieldKey -> String -> (Html Msg)
setFieldInput key placeholder =
  input
    [ class "uk-input"
    , attribute "placeholder" placeholder
    , onInput (SetField key)
    ] []


viewAggregations : Model -> List (Html Msg)
viewAggregations model =
  let row agg =
        tr []
          [ td [ class "uk-text-bold" ] [ text agg.name ]
          , td [] [ text agg.pattern ]
          , td [] [ text agg.aggregation ]
          , td [] [ text (toString agg.factor) ]
          ]
      inputRow =
        tr []
          [ td [ class "uk-text-bold" ] [ setFieldInput AggregationNameKey "name" ]
          , td [] [ setFieldInput AggregationPatternKey "pattern" ]
          , td [] [ setFieldInput AggregationAggregationKey "average" ]
          , td []
            [  div [ class "uk-inline" ]
              [ setFieldInput AggregationFactorKey "0.5"
              , a [ class "uk-form-icon uk-form-icon-flip"
                  , attribute "uk-icon" "plus-circle"
                  , onClick AddAggregation
                  ] []
              ]
            ]
          ]
      aggregations = List.map row model.aggregations
      rows = aggregations ++ [inputRow]
  in
    [ h2 [] [ text "Aggregation rules" ]
    , p [] [ text "Please note that changes to these aggregation rules don't change the persisted configuration in your statser.yaml and therefore won't survive a server restart. Moreover configuration changes usually won't affect any already existing archives' aggregation rules."]
    , table
      [ id "aggregations"
      , class "uk-table"
      , class "uk-table-divider"
      , class "uk-table-responsive"
      ]
      [ thead [] [ header "Name", header "Pattern", header "Aggregation", header "Factor" ]
      , tbody [] rows
      ]
    ]


viewStorages : Model -> List (Html Msg)
viewStorages model =
  let row storage =
        tr []
          [ td [ class "uk-text-bold" ] [ text storage.name ]
          , td [] [ text storage.pattern ]
          , td [] [ text (String.join ", " storage.retentions) ]
          ]
      inputRow =
        tr []
          [ td [ class "uk-text-bold" ] [ setFieldInput StorageNameKey "name" ]
          , td [] [ setFieldInput StoragePatternKey "pattern" ]
          , td []
            [  div [ class "uk-inline" ]
              [ setFieldInput StorageRetentionsKey "60:1d"
              , a [ class "uk-form-icon uk-form-icon-flip"
                  , attribute "uk-icon" "plus-circle"
                  , onClick AddStorage
                  ] []
              ]
            ]
          ]
      storages = List.map row model.storages
      rows = storages ++ [inputRow]
  in
    [ h2 [] [ text "Storage rules" ]
    , p [] [ text "Please note that changes to these storage rules don't change the persisted configuration in your statser.yaml and therefore won't survive a server restart. Moreover configuration changes usually won't affect any already existing archives' storage rules."]
    , table
      [ id "storages"
      , class "uk-table"
      , class "uk-table-divider"
      , class "uk-table-responsive"
      ]
      [ thead [] [ header "Name", header "Pattern", header "Retentions" ]
      , tbody [] rows
      ]
    ]


icon : String -> Html Msg
icon name =
  span [ class "uk-icon", attribute "uk-icon" name ] []


viewDashboard : Model -> List (Html Msg)
viewDashboard model =
  [ viewLead , viewMeta ] ++ viewHealths model ++ viewLive ++ viewMetrics model


viewMeta : Html Msg
viewMeta =
  div [ class "uk-text-meta" ]
    [ text "This page's content is updated every 10 seconds. If this does not work for you, please try the "
    , a [ href "/.statser/health" ] [ text "JSON endpoint" ]
    , text " instead"
    ]


viewNavigation : Model -> Html Msg
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

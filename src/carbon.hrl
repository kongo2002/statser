%% -*- coding: utf-8 -*-
%% Automatically generated, do not edit
%% Generated by gpb_compile version 4.1.1

-ifndef(carbon).
-define(carbon, true).

-define(carbon_gpb_version, "4.1.1").

-ifndef('POINT_PB_H').
-define('POINT_PB_H', true).
-record('Point',
        {timestamp = 0          :: non_neg_integer() | undefined, % = 1, 32 bits
         value = 0.0            :: float() | integer() | infinity | '-infinity' | nan | undefined % = 2
        }).
-endif.

-ifndef('METRIC_PB_H').
-define('METRIC_PB_H', true).
-record('Metric',
        {metric = []            :: iolist() | undefined, % = 1
         points = []            :: [#'Point'{}] | undefined % = 2
        }).
-endif.

-ifndef('PAYLOAD_PB_H').
-define('PAYLOAD_PB_H', true).
-record('Payload',
        {metrics = []           :: [#'Metric'{}] | undefined % = 1
        }).
-endif.

-endif.

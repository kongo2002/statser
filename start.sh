#!/bin/sh

exec erl -env ERL_LIBS _build/default/lib -eval 'application:ensure_all_started(statser).' -noshell

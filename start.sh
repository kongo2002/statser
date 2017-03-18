#!/bin/sh

exec erl -env ERL_LIBS _build/default/lib -config app.config -eval 'application:ensure_all_started(statser).' -noshell

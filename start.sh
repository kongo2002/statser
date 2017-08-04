#!/bin/sh

exec erl +B -env ERL_LIBS _build/default/lib -config config/sys.config -setcookie statser -sname statser -eval 'application:ensure_all_started(statser).' -noinput

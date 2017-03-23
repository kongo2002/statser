
# statser

This project is supposed to *become* a more-or-less drop-in replacement for
[graphite][graphite].

> The project is still in pre-alpha state - work in progress!


## Motivation

[Graphite][graphite] is a fantastic piece or rather collection of software(s)
and I don't want to really replace any of it. Nonetheless there are a few points
that motivate me to tackle some parts of [graphite][graphite]'s feature set to
be solved in a distributed erlang-powered application.

- the fact that the "graphite stack" is spread across several applications
  (`whisper`, `carbon`, `graphite-web`) has some advantages for sure but
  complicates maintainability significantly

- as soon as you want/have to distribute graphite across multiple nodes there
  come more application parts into play (`carbon-relay`, `carbon-aggregator`)

- all of these applications are configured independently from each other and
  might get difficult to tune into sync at times

- *statser* will be built to replace basically all of the "server parts" into
  one single application which is supposed to be distributed more easily (erlang
  is basically built for this kind of thing)

- my goal is that you only need one or more `statser` instances that expose an
  interface that can be used with common metrics dashboards like
  [grafana][grafana] - and nothing more!

- last but not least I will use this project to improve my grip on erlang for
  sure


## Build

### Compile

The project is powered by [rebar3][rebar3], so expect the common commands to
work as expected:

    $ rebar3 compile


### Tests

    $ rebar3 eunit


## Status

Right now you can start `statser` which will listen on port `2003` (by default)
and for example start your `collectd` instance(s) (or whatever graphite
compatible metrics source you use), point those to your `statser` node and it
will go ahead with recording and aggregating your metrics just like
`carbon-cache` did. Moreover you can configure the storage and aggregation rules
in a simple YAML configuration file.

Please find a roughly up-to-date list of features that are either finished or on
my roadmap (somewhere) below.


### Stuff that should work

* full whisper database support
* carbon plain text interface (known to listen on port `2003`)
* configurable storage rules
* configurable aggregation schemes
* throttling of archive creation


### TODO

* throttling of metrics updates (flushed to disk)
* JSON API of `graphite-web`
    - I want to support the pure data point API (I don't know what it's called
      for real) only: **no image generation**
    - this is a *huge* list for sure - I will try to start with the most
      common/basic ones and go from there I guess
* configurable amount of caching (see above)
* investigate into more elaborate archive write optimization


[rebar3]: https://www.rebar3.org/
[graphite]: https://graphiteapp.org/
[grafana]: https://grafana.com/

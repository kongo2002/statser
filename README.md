
# statser

[![build status](https://api.travis-ci.org/kongo2002/statser.svg)][travis]

This project is supposed to *become* a more-or-less drop-in replacement for
[graphite][graphite].

> The project is still in beta state - work in progress but feel free to
> give it a shot!


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

### Requirements

* erlang OTP >= 19


### Compile

The project is powered by [rebar3][rebar3], so expect the common commands to
work as expected:

    $ rebar3 compile


### Quickstart

After successful compilation you can quickly start a development *statser*
instance with the sample `start.sh` script. The script uses default options that
spawn port `2003` (carbon plain text interface) and `8080` (metrics API):

    $ rebar3 compile && ./start.sh


### Tests

    $ rebar3 eunit


## Status

Right now you can start `statser` which will listen on port `2003` (by default)
and for example start your `collectd` instance(s) (or whatever graphite
compatible metrics source you use), point those to your `statser` node and it
will go ahead with recording and aggregating your metrics just like
`carbon-cache` did. Moreover you can configure the storage and aggregation rules
in a simple YAML configuration file.

Moreover you can already point your [grafana][grafana] dashboard to your statser
instance just like you did with a graphite backend. Even though
[grafana][grafana] has no particular statser-support it does work because
statser implements most parts of graphite-web's API already.

Find a roughly up-to-date list of features that are either finished or somewhere
on my roadmap below.


### Stuff that works already

* full whisper database support
* carbon plain text interface (known to listen on port `2003`)
* configurable storage rules
* configurable aggregation schemes
* throttling of archive creation
* throttling of metrics updates (writes to disk)
* configurable blacklist/whitelist of metrics
* basic [StatsD][statsd] interface (known to listen on UDP port `8125`)
    - support for counters, timers, gauges and sets
* metrics API
* render API (JSON output only!)
    - `absolute`
    - `aliasByMetric`
    - `aliasByNode`
    - `alias`
    - `averageAbove`
    - `averageBelow`
    - `averageOutsidePercentile`
    - `derivative`
    - `diffSeries`
    - `integral`
    - `invert`
    - `limit`
    - `mostDeviant`
    - `movingAverage`
    - `multiplySeries`
    - `nPercentile`
    - `offsetToZero`
    - `offset`
    - `removeAboveValue`
    - `removeBelowValue`
    - `squareRoot`
    - `sumSeries`


### Configuration

Statser ships with sane defaults out-of-the-box so you might not need a
configuration at all. However there are a few settings that you may configure
via a YAML file (`statser.yaml` in the working directory):

```yaml
# storage directory of the whisper files
# defaults to the current working directory
data_dir: /opt/whisper/storage

# configuration of storage rules and archive retentions
#
# this contains basically what you might know of `storage-schemas.conf`
# from the 'carbon-cache' configuration
storage:

  stats:
    pattern: ^stats\.
    retentions:
      - 10:1m
      - 60:1d

  carbon:
    pattern: ^carbon\.
    retentions:
      - 60:30d

# storage aggregation rules
#
# may look familiar to `storage-aggregation.conf` of carbon-cache
aggregation:

  min:
    pattern: \.min$
    aggregation: min
    factor: 0.1

  max:
    pattern: \.max$
    aggregation: max
    factor: 0.1

  count:
    pattern: \.count$
    aggregation: sum
    factor: 0

# StatsD compatible adapter (disabled by default)
udp:
  port: 8125
  interval: 10000

# you may configure a blacklist and/or whitelist to configure
# what metrics you really care about
# every metric that enters the system will be checked against
# the regexes you defined
blacklist:
  - \.sum_squares$
  - \.localhost\.

whitelist:
  - ^stats\.
```


### TODO

* configurable amount of caching
* support even more functions of `graphite-web` render API
* investigate into more elaborate archive write optimization


[rebar3]: https://www.rebar3.org/
[graphite]: https://graphiteapp.org/
[grafana]: https://grafana.com/
[travis]: https://travis-ci.org/kongo2002/statser/
[statsd]: https://github.com/etsy/statsd/


# statser

[![build status](https://api.travis-ci.org/kongo2002/statser.svg)][travis]

This project is a rework of the [graphite][graphite] stack in shape of an erlang
application.

> The project is still in beta state - work in progress but feel free to
> give it a shot!


## Motivation

[Graphite][graphite] is a fantastic piece or rather collection of software(s)
that's tough to 'replace' at all. Nonetheless there are a few points
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

* erlang OTP >= 20
  * xmerl
* node-js *(for client only)*


### Compile

You can build the project's components by using the `Makefile`:

    $ make

The server and client parts can also be built separately:

    $ make server
    $ make client

However since the project is powered by [rebar3][rebar3], you may also directly
invoke all common commands directly:

    $ rebar3 compile


### Docker

You can also use the pre-built [docker
image](https://hub.docker.com/r/kongo2002/statser/) in order to quickly get up
and running:

    $ docker pull kongo2002/statser

On your local machine you may want to start the container with `-P` or expose
the container's ports on your host machine:

    $ docker run -d -p 8080:8080 -p 2003:2003 -p 8125:8125/udp kongo2002/statser


### Quickstart

After successful compilation you can quickly start a development *statser*
instance with the sample `start.sh` script. The script uses default options that
spawn port `2003` (carbon plain text interface), port `8080` (metrics API) and
`8125/udp` (StatsD interface):

    $ ./start.sh

Now you can already ingest metrics, e.g. by using `netcat`:

```bash
# push some test metrics
$ echo "test.foo 100.1" | nc --send-only localhost 2003
$ echo "test.bar 200.2" | nc --send-only localhost 2003

# fetch metrics
$ curl localhost:8080 -XPOST -d 'target=test.*' -d 'maxDataPoints=5'
[{"target":"test.bar",
  "datapoints":[[null,1520891400],[null,1520908680],[null,1520925960],[null,1520943240],[200.2,1520960520]]},
 {"target":"test.foo",
  "datapoints":[[null,1520891400],[null,1520908680],[null,1520925960],[null,1520943240],[100.1,1520960520]]}]
```


#### Web dashboard

Moreover you can browse to the web dashboard that displays some of statser's
health endpoints, internal metrics and serves a basic administration UI:

    $ firefox http://localhost:8080/.statser/


### Tests

    # basically the same as `rebar3 eunit`
    $ make test


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
statser implements (or mimics) most parts of graphite-web's API already.

Find a list of features that are either finished or on my roadmap below.


### Features

* full whisper database support
* carbon plain text interface (known to listen on port `2003`)
* configurable storage rules
* configurable aggregation schemes
* throttling of archive creation
* throttling of metrics updates (writes to disk)
* configurable blacklist/whitelist of metrics
* basic [StatsD][statsd] interface (known to listen on UDP port `8125`)
    - support for counters, timers, gauges and sets
* support for querying metrics of multiple connected statser instances
* metrics API
* render API (JSON output only!)
    - `absolute`
    - `aliasByMetric`
    - `aliasByNode`
    - `aliasSub`
    - `alias`
    - `asPercent`
    - `averageAbove`
    - `averageBelow`
    - `averageOutsidePercentile`
    - `averageSeries` (short alias: `avg`)
    - `changed`
    - `constantLine`
    - `currentAbove`
    - `currentBelow`
    - `derivative`
    - `diffSeries`
    - `divideSeries`
    - `exclude`
    - `grep`
    - `highestAverage`
    - `highestCurrent`
    - `highestMax`
    - `integralByInterval`
    - `integral`
    - `invert`
    - `isNonNull`
    - `keepLastValue`
    - `limit`
    - `lowestAverage`
    - `lowestCurrent`
    - `maxSeries`
    - `maximumAbove`
    - `maximumBelow`
    - `minSeries`
    - `minimumAbove`
    - `minimumBelow`
    - `mostDeviant`
    - `movingAverage`
    - `multiplySeries`
    - `nPercentile`
    - `nonNegativeDerivative`
    - `offsetToZero`
    - `offset`
    - `perSecond`
    - `powSeries`
    - `pow`
    - `randomWalk`
    - `rangeOfSeries`
    - `removeAboveValue`
    - `removeBelowValue`
    - `scaleToSeconds`
    - `scale`
    - `squareRoot`
    - `stddevSeries`
    - `sumSeries`


### Configuration

Statser ships with sane defaults out-of-the-box so you might not need a
configuration at all. However there are a few settings that you may configure
via a YAML file (`statser.yaml` in the working directory):

```yaml
# storage directory of the whisper files
# defaults to the current working directory
data_dir: /opt/whisper/storage

# IO consuming operations like updates and archive creations
# are by default (and should be) rate limited to some extent:
rate_limits:

  # max. archive creations per second (default: 25)
  creates: 25

  # max. archive updates per second (default: 500)
  updates: 500

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

# you may configure a blacklist and/or whitelist to configure
# what metrics you really care about
# every metric that enters the system will be checked against
# the regexes you defined
blacklist:
  - \.sum_squares$
  - \.localhost\.

whitelist:
  - ^stats\.

# metrics and render API
api:
  port: 8080

# TCP listener
# this listener is basically equivalent to the carbon
# plaintext listener you might know from graphite
tcp:
  port: 2003

# StatsD compatible adapter
udp:
  # UDP port to listen on
  # set this to something =< 0 to disable
  port: 8125

  # metrics flush interval (in seconds)
  interval: 10

  # prune inactive metrics keys after x seconds
  # 0 to never prune metrics at all
  prune_after: 300

# protobuf listener (disabled by default)
#
# the protobuf interface was introduced in graphite 1.x
# you can enable this by using the following options
protobuf:
  port: 2005
```


### TODO

* graphite 1.1 features
  - tag support
  - [open metrics format][openmetrics]
* support even more functions of `graphite-web` render API
* investigate into more elaborate archive write optimization


## License

*statser* is licensed under the [Apache license][apache], Version 2.0

> Unless required by applicable law or agreed to in writing, software
> distributed under the License is distributed on an "AS IS" BASIS,
> WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
> See the License for the specific language governing permissions and
> limitations under the License.


[apache]: http://www.apache.org/licenses/LICENSE-2.0
[docker]: https://www.docker.com/
[grafana]: https://grafana.com/
[graphite]: https://graphiteapp.org/
[openmetrics]: https://github.com/RichiH/OpenMetrics/blob/master/metric_exposition_format.md
[rebar3]: https://www.rebar3.org/
[statsd]: https://github.com/etsy/statsd/
[travis]: https://travis-ci.org/kongo2002/statser/

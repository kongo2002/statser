-module(statser_whisper).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-export([read_metadata/1,
         aggregation_type/1,
         aggregation_value/1,
         update_point/3]).


% 16 bytes = 4 metadata fields x 4 bytes
-define(METADATA_HEADER_SIZE, 16).

% 12 bytes = 3 archive header fields x 4 bytes
-define(METADATA_ARCHIVE_HEADER_SIZE, 12).

% 12 bytes = 4 bytes (timestamp) + 8 bytes (value)
-define(POINT_SIZE, 12).


-record(metadata, {aggregation, retention, xff, archives}).

-record(archive_header, {offset, seconds, points, retention, size}).


aggregation_type(1) -> average;
aggregation_type(2) -> sum;
aggregation_type(3) -> last;
aggregation_type(4) -> max;
aggregation_type(5) -> min;
aggregation_type(6) -> average_zero.


aggregation_value(average) -> 1;
aggregation_value(sum) -> 2;
aggregation_value(last) -> 3;
aggregation_value(max) -> 4;
aggregation_value(min) -> 5;
aggregation_value(average_zero) -> 6.


read_metadata(File) ->
    {ok, IO} = file:open(File, [read, binary]),
    try read_metadata_inner(IO)
        after file:close(IO)
    end.


read_metadata_inner(IO) ->
    {ok, Header} = file:read(IO, ?METADATA_HEADER_SIZE),
    case read_header(Header) of
        {ok, AggType, MaxRet, XFF, Archives} ->
            case read_archive_info(IO, Archives) of
                error -> error;
                As ->
                    Metadata = #metadata{aggregation=AggType,
                                        retention=MaxRet,
                                        xff=XFF,
                                        archives=As},
                    {ok, Metadata}
            end;
        error -> error
    end.


make_archive_header(Offset, Seconds, Points) ->
    #archive_header{offset=Offset,
                   seconds=Seconds,
                   points=Points,
                   retention=Seconds * Points,
                   size=Points * ?POINT_SIZE}.


read_archive_info(IO, Archives) ->
    ByOffset = fun(A, B) -> A#archive_header.offset =< B#archive_header.offset end,
    case read_archive_info(IO, [], Archives) of
        error -> error;
        As -> lists:sort(ByOffset, As)
    end.

read_archive_info(_IO, As, 0) -> As;
read_archive_info(IO, As, Archives) ->
    case file:read(IO, ?METADATA_ARCHIVE_HEADER_SIZE) of
        {ok, <<Offset:32/integer-unsigned-big, Secs:32/integer-unsigned-big, Points:32/integer-unsigned-big>>} ->
            Archive = make_archive_header(Offset, Secs, Points),
            read_archive_info(IO, [Archive | As], Archives - 1);
        _Error -> error
    end.


read_header(<<AggType:32/unsigned-integer-big,
              MaxRetention:32/unsigned-integer-big,
              XFF:32/float-big,
              NumArchives:32/integer-unsigned-big>>) ->
    {ok, aggregation_type(AggType), MaxRetention, XFF, NumArchives};
read_header(_) -> error.


write_header(AggType, MaxRetention, XFF, NumArchives) ->
    % aggregation type: 4 bytes, unsigned
    % max retention:    4 bytes, unsigned
    % x-files-factor:   4 bytes, float
    % archives:         4 bytes, unsigned
    AggValue = aggregation_value(AggType),
    <<AggValue:32/unsigned-integer-big,
      MaxRetention:32/unsigned-integer-big,
      XFF:32/float-big,
      NumArchives:32/integer-unsigned-big>>.


highest_precision_archive(TimeDiff, [#archive_header{retention=Ret} | As]) when Ret < TimeDiff ->
    highest_precision_archive(TimeDiff, As);
highest_precision_archive(_TimeDiff, [A | As]) -> {A, As};
highest_precision_archive(_TimeDiff, []) -> error.


data_point(Interval, Value) ->
    <<Interval:32/integer-unsigned-big, Value:64/float-big>>.


read_point(IO) ->
    case file:read(IO, ?POINT_SIZE) of
        {ok, <<Interval:32/integer-unsigned-big, Value:64/float-big>>} ->
            {Interval, Value};
        Unexpected ->
            lager:error("read unexpected data point: ~p", [Unexpected]),
            error
    end.


point_at(IO, Offset) ->
    {ok, _Position} = file:position(IO, {bof, Offset}),
    read_point(IO).


write_at(IO, Point, Offset) ->
    {ok, _Position} = file:position(IO, {bof, Offset}),
    file:write(IO, Point).


mod(A, B) when A < 0 -> erlang:abs(A) rem B;
mod(A, B) -> A rem B.


% this method determines the offset where a data point with the
% given timestamp (Interval) is supposed to start at
get_data_point_offset(Archive, _Interval, 0) ->
    % no data point was written to this archive yet
    % start at the initial offset for that reason
    Archive#archive_header.offset;
get_data_point_offset(Archive, Interval, BaseInterval) ->
    % the archive contains data already, that's why we
    % calculate the relative offset/distance to this 'BaseInterval'
    Distance = Interval - BaseInterval,
    PointDistance = Distance div Archive#archive_header.seconds,
    ByteDistance = PointDistance * ?POINT_SIZE,
    Archive#archive_header.offset + (mod(ByteDistance, Archive#archive_header.size)).


update_point(File, Value, TimeStamp) ->
    {ok, IO} = file:open(File, [write, read, binary]),
    try do_update(IO, Value, TimeStamp)
        after file:close(IO)
    end.


do_update(IO, Value, TimeStamp) ->
    {ok, Metadata} = read_metadata_inner(IO),
    write_point(IO, Metadata, Value, TimeStamp).


interval_start(Archive, TimeStamp) ->
    TimeStamp - (TimeStamp rem Archive#archive_header.seconds).


collect_series_values(#archive_header{seconds=Step}, Interval, Values) ->
    collect_series_values(Step, Interval, Values, [], 0).

collect_series_values(Step, Interval, <<TS:32/integer-unsigned-big, Value:64/float-big, Rst/binary>>, Acc, Seen) ->
    if
        % we compare the timestamp of the data point we just read (TS) with
        % the timestamp value we are expecting (Interval)
        % only if these timestamps match up we consider that data point a valid one
        % NOTE: this is *not* an error - the archive may very well contain sparse values
        Interval == TS ->
            collect_series_values(Step, Interval + Step, Rst, [Value | Acc], Seen + 1);
        true ->
            collect_series_values(Step, Interval + Step, Rst, Acc, Seen + 1)
    end;
collect_series_values(_Step, _Interval, <<>>, Res, Seen) -> {Res, Seen}.


propagate_lower_archives(IO, Header, TimeStamp, Higher, [Lower | Ls]) ->
    LowerSeconds = Lower#archive_header.seconds,
    LowerStart = interval_start(Lower, TimeStamp),

    % read higher point
    % XXX: might be passed in already?
    HighOffset = Higher#archive_header.offset,
    {HighInterval, _} = point_at(IO, HighOffset),
    HighFirstOffset = get_data_point_offset(Higher, LowerStart, HighInterval),

    HigherSeconds = Higher#archive_header.seconds,
    HigherPointsPerLower = LowerSeconds div HigherSeconds,
    HigherSize = HigherPointsPerLower * ?POINT_SIZE,
    RelativeFirstOffset = HighFirstOffset - HighOffset,
    RelativeLastOffset = mod(RelativeFirstOffset + HigherSize, Higher#archive_header.size),
    HigherLastOffset = RelativeLastOffset + HighOffset,

    {ok, _} = file:position(IO, {bof, HighFirstOffset}),

    {ok, Series} =
    if
        % the amount of higher points that make up one lower point (HigherPointsPerLower)
        % do fit into the higher archive (starting from the current interval/timestamp).
        % this means we can read all required points straight up
        HighFirstOffset < HigherLastOffset ->
            file:read(IO, HigherLastOffset - HighFirstOffset);
        % otherwise we are now basically at the end of the higher archive so that we
        % cannot read the required aggregate points (HigherPointsPerLower) without exceeding
        % the archive's size.
        % that's why we read until the end of the archive first, followed by the
        % remaining number of required aggregate points from the beginning of the archive
        true ->
            HigherEnd = HighOffset + Higher#archive_header.size,
            {ok, FstSeries} = file:read(IO, HigherEnd - HighFirstOffset),
            {ok, _} = file:position(IO, {bof, HighOffset}),
            {ok, LstSeries} = file:read(IO, HigherLastOffset - HighOffset),
            {ok, <<FstSeries/binary, LstSeries/binary>>}
    end,

    {CollectedValues, NumPoints} = collect_series_values(Higher, LowerStart, Series),

    lager:info("read series: ~p [~p points]", [CollectedValues, NumPoints]),

    WroteAggregate = write_propagated_values(IO, Header, Lower, LowerStart, CollectedValues, NumPoints),
    if
        % continue to write into even lower archives only if we actually
        % wrote an aggregate value in this archive
        WroteAggregate == true ->
            propagate_lower_archives(IO, Header, TimeStamp, Lower, Ls);
        true ->
            ok
    end;
propagate_lower_archives(_, _, _, _, []) -> ok.


write_propagated_values(IO, Header, Lower, LowerInterval, Values, NumPoints) ->
    AggType = Header#metadata.aggregation,
    XFF = Header#metadata.xff,
    NumValues = length(Values),
    KnownPercentage = NumValues / NumPoints,
    if
        % we do have enough data points to calculate an aggregation
        KnownPercentage >= XFF ->
            AggValue = aggregate(AggType, Values, NumValues, NumPoints),
            lager:info("calculated aggregate [~p] ~p [values ~p]", [AggType, AggValue, Values]),
            Point = data_point(LowerInterval, AggValue),
            {BaseInterval, _} = point_at(IO, Lower#archive_header.offset),
            Offset = get_data_point_offset(Lower, LowerInterval, BaseInterval),
            ok = write_at(IO, Point, Offset),
            true;
        true ->
            false
    end.


aggregate(average, Values, NumValues, _) -> lists:sum(Values) / NumValues;
aggregate(sum, Values, _, _) -> lists:sum(Values);
aggregate(last, Values, _, _) -> lists:last(Values);
aggregate(max, Values, _, _) -> lists:max(Values);
aggregate(min, Values, _, _) -> lists:min(Values);
aggregate(average_zero, Values, _, NumPoints) -> lists:sum(Values) / NumPoints.


write_point(IO, Header, Value, TimeStamp) ->
    Now = erlang:system_time(second),
    TimeDiff = Now - TimeStamp,
    MaxRetention = Header#metadata.retention,
    if
        TimeDiff >= MaxRetention, TimeDiff < 0 ->
            lager:error("timestamp ~p is not covered by any archive of ~p", [TimeStamp, Header]),
            error;
        true ->
            % find highest precision and lower archives to update
            {Archive, LowerArchives} = highest_precision_archive(TimeDiff, Header#metadata.archives),

            % read base data point
            {BaseInterval, _Value} = point_at(IO, Archive#archive_header.offset),
            Position = get_data_point_offset(Archive, TimeStamp, BaseInterval),

            % write data point based on initial data point
            Interval = interval_start(Archive, TimeStamp),
            Point = data_point(Interval, Value),
            ok = write_at(IO, Point, Position),

            propagate_lower_archives(IO, Header, Interval, Archive, LowerArchives)
    end.

%%
%% TESTS
%%

-ifdef(TEST).

highest_precision_archive_test_() ->
    Archive = make_archive_header(28, 60, 1440),
    Archive2 = make_archive_header(28, 300, 1000),

    [?_assertEqual(highest_precision_archive(100, []), error),
     ?_assertEqual(highest_precision_archive(100, [Archive]), {Archive, []}),
     ?_assertEqual(highest_precision_archive(86400, [Archive]), {Archive, []}),
     ?_assertEqual(highest_precision_archive(100, [Archive, Archive2]), {Archive, [Archive2]}),
     ?_assertEqual(highest_precision_archive(86400, [Archive, Archive2]), {Archive, [Archive2]}),
     ?_assertEqual(highest_precision_archive(86401, [Archive, Archive2]), {Archive2, []}),
     ?_assertEqual(highest_precision_archive(300 * 1000 + 1, [Archive, Archive2]), error)].

get_data_point_offset_test_() ->
    Now = erlang:system_time(second),
    Offset = 28,
    Archive = make_archive_header(Offset, 60, 1440),

    [?_assertEqual(get_data_point_offset(Archive, Now, 0), Offset),
     ?_assertEqual(get_data_point_offset(Archive, Now+60, Now), Offset + ?POINT_SIZE),
     ?_assertEqual(get_data_point_offset(Archive, Now+119, Now), Offset + ?POINT_SIZE),
     ?_assertEqual(get_data_point_offset(Archive, Now+120, Now), Offset + ?POINT_SIZE + ?POINT_SIZE)].

-endif. % TEST

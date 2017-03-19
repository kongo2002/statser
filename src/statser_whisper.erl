-module(statser_whisper).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([create/2,
         create/4,
         read_metadata/1,
         aggregation_type/1,
         aggregation_value/1,
         update_point/3]).


-spec aggregation_type(integer()) -> aggregation().
aggregation_type(1) -> average;
aggregation_type(2) -> sum;
aggregation_type(3) -> last;
aggregation_type(4) -> max;
aggregation_type(5) -> min;
aggregation_type(6) -> average_zero.


-spec aggregation_value(aggregation()) -> integer().
aggregation_value(average) -> 1;
aggregation_value(sum) -> 2;
aggregation_value(last) -> 3;
aggregation_value(max) -> 4;
aggregation_value(min) -> 5;
aggregation_value(average_zero) -> 6.


-spec read_metadata(binary()) -> tuple().
read_metadata(File) ->
    case file:open(File, [read, binary]) of
        {ok, IO} ->
            try read_metadata_inner(IO)
                after file:close(IO)
            end;
        Error -> Error
    end.


read_metadata_inner(IO) ->
    Read = file:read(IO, ?METADATA_HEADER_SIZE),
    case read_header(Read) of
        {ok, AggType, MaxRet, XFF, Archives} ->
            case read_archive_info(IO, Archives) of
                error -> error;
                As -> {ok, #whisper_metadata{aggregation=AggType,
                                     retention=MaxRet,
                                     xff=XFF,
                                     archives=As}}
            end;
        error -> error
    end.


read_header({ok, <<AggType:32/unsigned-integer-big,
                   MaxRetention:32/unsigned-integer-big,
                   XFF:32/float-big,
                   NumArchives:32/integer-unsigned-big>>}) ->
    {ok, aggregation_type(AggType), MaxRetention, XFF, NumArchives};
read_header(_) -> error.


read_archive_info(IO, Archives) ->
    ByOffset = fun(A, B) -> A#whisper_archive.offset =< B#whisper_archive.offset end,
    case read_archive_info(IO, [], Archives) of
        error -> error;
        As -> lists:sort(ByOffset, As)
    end.

read_archive_info(_IO, As, 0) -> As;
read_archive_info(IO, As, Archives) ->
    case file:read(IO, ?METADATA_ARCHIVE_HEADER_SIZE) of
        {ok, <<Offset:32/integer-unsigned-big,
               Secs:32/integer-unsigned-big,
               Points:32/integer-unsigned-big>>} ->
            Archive = make_archive_header(Offset, Secs, Points),
            read_archive_info(IO, [Archive | As], Archives - 1);
        _Error -> error
    end.


-spec create(binary(), #whisper_metadata{}) -> {ok, #whisper_metadata{}}.
create(File, #whisper_metadata{archives=As, aggregation=Agg, xff=XFF}) ->
    Archives = lists:map(fun(A) -> {A#whisper_archive.seconds, A#whisper_archive.points} end, As),
    create(File, Archives, Agg, XFF).


-spec create(binary(), [{integer(), integer()}], aggregation(), float()) -> {ok, #whisper_metadata{}}.
create(File, Archives, Aggregation, XFF) ->
    {ok, IO} = file:open(File, [write, binary]),
    try create_inner(IO, Archives, Aggregation, XFF)
        after file:close(IO)
    end.


create_inner(IO, Archives, Aggregation, XFF) ->
    GetDefault = fun([], Def) -> Def;
                    (Val, _) -> Val end,
    AggValue = GetDefault(Aggregation, average),
    XFFValue = GetDefault(XFF, 0.5),

    case validate_archives(Archives) of
        {ok, ValidArchives} ->
            % archives are valid; write the header now
            NumArchives = length(ValidArchives),
            InitialOffset = NumArchives * ?METADATA_ARCHIVE_HEADER_SIZE + ?METADATA_HEADER_SIZE,
            {LastOffset, UpdArchives} = with_offsets(InitialOffset, ValidArchives),
            MaxRetention = max_retention_archive(UpdArchives),
            ok = file:write(IO, write_header(AggValue, MaxRetention, XFFValue, NumArchives)),
            ok = write_archive_info(IO, UpdArchives),
            ok = write_empty_archives(IO, LastOffset - InitialOffset),

            {ok, #whisper_metadata{aggregation=AggValue,
                           retention=MaxRetention,
                           xff=XFFValue,
                           archives=UpdArchives}};
        error -> error
    end.


write_empty_archives(_IO, 0) -> ok;
write_empty_archives(IO, Bytes) ->
    WriteBytes = min(Bytes, ?WRITE_CHUNK_SIZE),
    WriteBits = WriteBytes * 8,
    ok = file:write(IO, <<0:WriteBits>>),
    write_empty_archives(IO, Bytes - WriteBytes).


with_offsets(Offset, Archives) -> with_offsets(Offset, Archives, []).

with_offsets(Offset, [], Res) -> {Offset, lists:reverse(Res)};
with_offsets(Offset, [A | As], Res) ->
    NewOffset = Offset + A#whisper_archive.size,
    with_offsets(NewOffset, As, [A#whisper_archive{offset=Offset} | Res]).


max_retention_archive(Archives) ->
    lists:max(lists:map(fun(A) -> A#whisper_archive.retention end, Archives)).


% validate specified archives against the following rules:
%
% - at least one archive required
% - no duplicate archives (precision)
% - higher precision archives' precision must evenly divide all lower precision archives' precisions
% - lower precision archives must cover larger time intervals than higher precision archives
% - each archive must have at least enough points to consolidate to the next lower archive
validate_archives(Archives) ->
    case validate_archives(Archives, []) of
        error -> error;
        [] ->
            lager:error("invalid archives: no valid archive format found"),
            error;
        As ->
            ByPrecision = fun(A, B) -> A#whisper_archive.seconds =< B#whisper_archive.seconds end,
            [S | Ss] = Sorted = lists:sort(ByPrecision, As),
            case validate(S, Ss) of
                true -> {ok, Sorted};
                false -> error
            end
    end.

validate(Higher, [Lower | Ls]) ->
    EvenlyDivide = Lower#whisper_archive.seconds rem Higher#whisper_archive.seconds == 0,

    HighRetention = Higher#whisper_archive.retention,
    LowerRetention = Lower#whisper_archive.retention,
    RetentionIsCovered = LowerRetention > HighRetention,

    PointsPerConsolidation = Lower#whisper_archive.seconds div Higher#whisper_archive.seconds,
    EnoughPointsToConsolidate = Higher#whisper_archive.points >= PointsPerConsolidation,

    case EvenlyDivide and RetentionIsCovered and EnoughPointsToConsolidate of
        true -> validate(Lower, Ls);
        false -> false
    end;
validate(_, []) -> true.


validate_archives([{Seconds, Points} | As], Res) when Seconds > 0 andalso Points > 0 ->
    Archive = make_archive_header(0, Seconds, Points),
    PrecisionExists = fun(A) -> A#whisper_archive.seconds == Seconds end,
    case lists:any(PrecisionExists, Res) of
        true ->
            lager:error("invalid archive: archive with precision of ~p seconds already exists", [Seconds]),
            error;
        false ->
            validate_archives(As, [Archive | Res])
    end;
validate_archives([], Res) -> Res;
validate_archives(_, _) ->
    lager:error("invalid archive format: interval and points must be greater than 0"),
    error.


make_archive_header(Offset, Seconds, Points) ->
    #whisper_archive{offset=Offset,
                     seconds=Seconds,
                     points=Points,
                     retention=Seconds * Points,
                     size=Points * ?POINT_SIZE}.


write_archive_info(_IO, []) -> ok;
write_archive_info(IO, [#whisper_archive{offset=Offset,seconds=Secs,points=Points} | As]) ->
    Bytes = <<Offset:32/integer-unsigned-big, Secs:32/integer-unsigned-big, Points:32/integer-unsigned-big>>,
    ok = file:write(IO, Bytes),
    write_archive_info(IO, As).


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


highest_precision_archive(TimeDiff, [#whisper_archive{retention=Ret} | As]) when Ret < TimeDiff ->
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
    Archive#whisper_archive.offset;
get_data_point_offset(Archive, Interval, BaseInterval) ->
    % the archive contains data already, that's why we
    % calculate the relative offset/distance to this 'BaseInterval'
    Distance = Interval - BaseInterval,
    PointDistance = Distance div Archive#whisper_archive.seconds,
    ByteDistance = PointDistance * ?POINT_SIZE,
    Archive#whisper_archive.offset + (mod(ByteDistance, Archive#whisper_archive.size)).


-spec update_point(binary(), number(), integer()) -> tuple().
update_point(File, Value, TimeStamp) ->
    case file:open(File, [write, read, binary]) of
        {ok, IO} ->
            try do_update(IO, Value, TimeStamp)
                after file:close(IO)
            end;
        Error -> Error
    end.


do_update(IO, Value, TimeStamp) ->
    {ok, Metadata} = read_metadata_inner(IO),
    write_point(IO, Metadata, Value, TimeStamp).


interval_start(Archive, TimeStamp) ->
    TimeStamp - (TimeStamp rem Archive#whisper_archive.seconds).


collect_series_values(#whisper_archive{seconds=Step}, Interval, Values) ->
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
    LowerSeconds = Lower#whisper_archive.seconds,
    LowerStart = interval_start(Lower, TimeStamp),

    % read higher point
    % XXX: might be passed in already?
    HighOffset = Higher#whisper_archive.offset,
    {HighInterval, _} = point_at(IO, HighOffset),
    HighFirstOffset = get_data_point_offset(Higher, LowerStart, HighInterval),

    HigherSeconds = Higher#whisper_archive.seconds,
    HigherPointsPerLower = LowerSeconds div HigherSeconds,
    HigherSize = HigherPointsPerLower * ?POINT_SIZE,
    RelativeFirstOffset = HighFirstOffset - HighOffset,
    RelativeLastOffset = mod(RelativeFirstOffset + HigherSize, Higher#whisper_archive.size),
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
            HigherEnd = HighOffset + Higher#whisper_archive.size,
            {ok, FstSeries} = file:read(IO, HigherEnd - HighFirstOffset),
            {ok, _} = file:position(IO, {bof, HighOffset}),
            {ok, LstSeries} = file:read(IO, HigherLastOffset - HighOffset),
            {ok, <<FstSeries/binary, LstSeries/binary>>}
    end,

    {CollectedValues, NumPoints} = collect_series_values(Higher, LowerStart, Series),

    lager:debug("read series: ~p [~p points]", [CollectedValues, NumPoints]),

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
    AggType = Header#whisper_metadata.aggregation,
    XFF = Header#whisper_metadata.xff,
    NumValues = length(Values),
    KnownPercentage = NumValues / NumPoints,
    if
        % we do have enough data points to calculate an aggregation
        KnownPercentage >= XFF ->
            AggValue = aggregate(AggType, Values, NumValues, NumPoints),
            lager:debug("calculated aggregate [~p] ~p [values ~p]", [AggType, AggValue, Values]),
            Point = data_point(LowerInterval, AggValue),
            {BaseInterval, _} = point_at(IO, Lower#whisper_archive.offset),
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
    MaxRetention = Header#whisper_metadata.retention,
    if
        TimeDiff >= MaxRetention, TimeDiff < 0 ->
            lager:error("timestamp ~p is not covered by any archive of ~p", [TimeStamp, Header]),
            error;
        true ->
            % find highest precision and lower archives to update
            {Archive, LowerArchives} = highest_precision_archive(TimeDiff, Header#whisper_metadata.archives),

            % read base data point
            {BaseInterval, _Value} = point_at(IO, Archive#whisper_archive.offset),
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

    [?_assertEqual(error, highest_precision_archive(100, [])),
     ?_assertEqual({Archive, []}, highest_precision_archive(100, [Archive])),
     ?_assertEqual({Archive, []}, highest_precision_archive(86400, [Archive])),
     ?_assertEqual({Archive, [Archive2]}, highest_precision_archive(100, [Archive, Archive2])),
     ?_assertEqual({Archive, [Archive2]}, highest_precision_archive(86400, [Archive, Archive2])),
     ?_assertEqual({Archive2, []}, highest_precision_archive(86401, [Archive, Archive2])),
     ?_assertEqual(error, highest_precision_archive(300 * 1000 + 1, [Archive, Archive2]))
    ].

get_data_point_offset_test_() ->
    Now = erlang:system_time(second),
    Offset = 28,
    Archive = make_archive_header(Offset, 60, 1440),

    [?_assertEqual(Offset, get_data_point_offset(Archive, Now, 0)),
     ?_assertEqual(Offset + ?POINT_SIZE, get_data_point_offset(Archive, Now+60, Now)),
     ?_assertEqual(Offset + ?POINT_SIZE, get_data_point_offset(Archive, Now+119, Now)),
     ?_assertEqual(Offset + ?POINT_SIZE + ?POINT_SIZE, get_data_point_offset(Archive, Now+120, Now))
    ].

validate_archives_test_() ->
    Archive0 = {60, 1440},
    ArchiveHeader0 = make_archive_header(0, 60, 1440),
    Archive1 = {10, 360},
    ArchiveHeader1 = make_archive_header(0, 10, 360),

    [?_assertEqual(error, validate_archives([])),
     ?_assertEqual({ok, [ArchiveHeader0]}, validate_archives([Archive0])),
     ?_assertEqual({ok, [ArchiveHeader1, ArchiveHeader0]}, validate_archives([Archive0, Archive1])),
     ?_assertEqual(error, validate_archives([Archive0, Archive0])),
     ?_assertEqual(error, validate_archives([Archive0, {60, 3600}])),
     % unevenly divisible precisions
     ?_assertEqual(error, validate_archives([{60, 3600}, {61, 3600}])),
     % lower precision archive with lesser retention
     ?_assertEqual(error, validate_archives([{60, 3600}, {120, 100}])),
     ?_assertEqual(error, validate_archives([{120, 100}, {60, 3600}])),
     % not enough points for consolidation
     ?_assertEqual(error, validate_archives([{10, 5}, {60, 3600}]))
    ].

create_and_read_test_() ->
    RunTest = fun(As) ->
                      Fun = fun(File) ->
                                    {ok, M1} = create(File, As, average, 0.5),
                                    {ok, M2} = read_metadata(File),
                                    {ok, M1, M2}
                            end,
                      {ok, CreateM, ReadM} = with_tempfile(Fun),
                      Extract = fun(A) ->
                                        {A#whisper_archive.seconds, A#whisper_archive.points}
                                end,
                      CArchives = lists:map(Extract, CreateM#whisper_metadata.archives),
                      RArchives = lists:map(Extract, ReadM#whisper_metadata.archives),

                      [?_assertEqual(As, CArchives),
                       ?_assertEqual(As, RArchives),
                       ?_assertEqual(CreateM, ReadM)
                      ]
              end,

    lists:flatten([RunTest([{10, 60}]),
     RunTest([{10, 60}, {60, 600}]),
     RunTest([{10, 60}, {60, 600}, {3600, 168}])
    ]).

with_tempfile(Fun) ->
    TempFile = lib:nonl(os:cmd("mktemp")),
    try Fun(TempFile)
        after file:delete(TempFile)
    end.

-endif. % TEST

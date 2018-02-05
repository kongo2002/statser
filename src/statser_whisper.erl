-module(statser_whisper).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([create/2,
         create/4,
         read_metadata/1,
         aggregate/4,
         aggregation_type/1,
         aggregation_value/1,
         prepare_metadata/3,
         fetch/3,
         fetch/4,
         update_point/3,
         update_points/2]).


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
    case file:open(File, [read, binary, raw]) of
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


-spec fetch(binary(), integer(), integer()) -> #series{}.
fetch(File, From, Until) ->
    fetch(File, From, Until, statser_util:seconds()).


-spec fetch(binary(), integer(), integer(), integer()) -> #series{}.
fetch(File, From, Until, Now) ->
    case file:open(File, [read, binary, raw]) of
        {ok, IO} ->
            try fetch_inner(IO, From, Until, Now)
                after file:close(IO)
            end;
        Error -> Error
    end.


fetch_inner(IO, From, Until, Now) ->
    case read_metadata_inner(IO) of
        {ok, Metadata} ->
            Oldest = Now - Metadata#whisper_metadata.retention,

            if From > Now orelse Until < Oldest ->
                   % XXX: is this the right return value?
                   #series{values=[]};
               true ->
                   FromAdjusted = adjust_from(From, Oldest),
                   UntilAdjusted = adjust_until(Until, Now),
                   fetch_with_metadata(IO, Metadata, Now, FromAdjusted, UntilAdjusted)
            end;
        Error -> Error
    end.


adjust_from(From, OldestRetention) when From < OldestRetention -> OldestRetention;
adjust_from(From, _Oldest) -> From.


adjust_until(Until, Now) when Until > Now -> Now;
adjust_until(Until, _Now) -> Until.


fetch_with_metadata(IO, Metadata, Now, From, Until) ->
    Distance = Now - From,
    Archive = target_archive(Distance, Metadata#whisper_metadata.archives),

    % normalize range
    ArchiveSeconds = Archive#whisper_archive.seconds,
    FromInterval = interval_start(Archive, From) + ArchiveSeconds,
    UntilInterval0 = interval_start(Archive, Until) + ArchiveSeconds,

    UntilInterval = if UntilInterval0 == FromInterval ->
                           UntilInterval0 + ArchiveSeconds;
                       true ->
                           UntilInterval0
                    end,

    % read base data point
    {BaseInterval, _Value} = point_at(IO, Archive#whisper_archive.offset),

    FromOffset = get_data_point_offset(Archive, FromInterval, BaseInterval),
    UntilOffset = get_data_point_offset(Archive, UntilInterval, BaseInterval),

    % read series data points
    Series = fetch_series(IO, Archive, FromOffset, UntilOffset),
    Values = fetch_series_values(Archive, FromInterval, Series),
    Aggregation = Metadata#whisper_metadata.aggregation,
    #series{values=Values,start=FromInterval,until=UntilInterval,step=ArchiveSeconds,aggregation=Aggregation}.


fetch_series(IO, _Archive, FromOffset, UntilOffset) when FromOffset < UntilOffset ->
    {ok, _} = file:position(IO, {bof, FromOffset}),
    Distance = UntilOffset - FromOffset,
    {ok, Content} = file:read(IO, Distance),
    Content;

fetch_series(IO, Archive, FromOffset, UntilOffset) ->
    % we have to wrap around the archive's end
    {ok, _} = file:position(IO, {bof, FromOffset}),
    ArchiveOffset = Archive#whisper_archive.offset,
    ArchiveSize = Archive#whisper_archive.size,
    ArchiveEnd = ArchiveOffset + ArchiveSize,

    {ok, FstContent} = file:read(IO, ArchiveEnd - FromOffset),
    {ok, _} = file:position(IO, {bof, ArchiveOffset}),
    {ok, LstContent} = file:read(IO, UntilOffset - ArchiveOffset),
    <<FstContent/binary, LstContent/binary>>.


fetch_fold_func({TS, Value}, List) -> [{TS, Value} | List].


fetch_series_values(Archive, Interval, Values) ->
    {Result, _Seen} = fold_series_values(fun fetch_fold_func/2, Archive, Interval, Values),
    % XXX: probably we don't have to sort the values in here
    lists:reverse(Result).


target_archive(Distance, [#whisper_archive{retention=Retention} | As]) when Distance > Retention ->
    target_archive(Distance, As);
target_archive(_Distance, [#whisper_archive{}=Archive | _As]) ->
    Archive;
target_archive(_Distance, _Archives) ->
    none.


-spec create(binary(), #whisper_metadata{}) -> {ok, #whisper_metadata{}}.
create(File, #whisper_metadata{archives=As, aggregation=Agg, xff=XFF}) ->
    Archives = lists:map(fun(A) -> {A#whisper_archive.seconds, A#whisper_archive.points} end, As),
    create(File, Archives, Agg, XFF).


-spec create(binary(), [{integer(), integer()}], aggregation(), float()) -> {ok, #whisper_metadata{}}.
create(File, Archives, Aggregation, XFF) ->
    case file:open(File, [write, binary, raw]) of
        {ok, IO} ->
            try create_inner(IO, Archives, Aggregation, XFF)
                after file:close(IO)
            end;
        Error -> Error
    end.


-spec prepare_metadata([{integer(), integer()}], aggregation(), float()) -> {ok, #whisper_metadata{}}.
prepare_metadata(Archives, Aggregation, XFF) ->
    GetDefault = fun([], Def) -> Def;
                    (Val, _) -> Val end,
    AggValue = GetDefault(Aggregation, average),
    XFFValue = GetDefault(XFF, 0.5),

    case validate_archives(Archives) of
        {ok, ValidArchives} ->
            NumArchives = length(ValidArchives),
            InitialOffset = NumArchives * ?METADATA_ARCHIVE_HEADER_SIZE + ?METADATA_HEADER_SIZE,
            {_, UpdArchives} = with_offsets(InitialOffset, ValidArchives),
            MaxRetention = max_retention_archive(UpdArchives),
            {ok, #whisper_metadata{aggregation=AggValue,
                                   retention=MaxRetention,
                                   xff=XFFValue,
                                   archives=UpdArchives}};
        error -> error
    end.


create_inner(IO, Archives, Aggregation, XFF) ->
    case prepare_metadata(Archives, Aggregation, XFF) of
        {ok, M} ->
            % archives are valid; write the header now
            As = M#whisper_metadata.archives,
            NumArchives = length(As),
            InitialOffset = NumArchives * ?METADATA_ARCHIVE_HEADER_SIZE + ?METADATA_HEADER_SIZE,
            {LastOffset, _} = with_offsets(InitialOffset, As),
            ok = file:write(IO, write_header(M#whisper_metadata.aggregation,
                                             M#whisper_metadata.retention,
                                             M#whisper_metadata.xff,
                                             NumArchives)),
            ok = write_archive_info(IO, As),
            ok = write_empty_archives(IO, LastOffset - InitialOffset),

            {ok, M};
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
    case file:position(IO, {bof, Offset}) of
        {ok, _Position} -> file:write(IO, Point);
        Error ->
            lager:error("write_at failed at offset ~w [~p]", [Offset, Point]),
            Error
    end.


mod(X, Y) when X > 0 -> X rem Y;
mod(X, Y) when X < 0 -> Y + X rem Y;
mod(0, _Y) -> 0.


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


sort_by_newest(Points) ->
    ByTimeStamp = fun ({TA, _}, {TB, _}) -> TA > TB end,
    lists:sort(ByTimeStamp, Points).


update_points(_File, []) -> ok;
update_points(File, Points) ->
    Now = erlang:system_time(second),

    % order points by timestamp (newest first)
    OrderedPoints = sort_by_newest(Points),

    % open file handle
    case file:open(File, [write, read, binary, raw]) of
        {ok, IO} ->
            try do_update_points(IO, OrderedPoints, Now)
                after file:close(IO)
            end;
        Error -> Error
    end.


do_update_points(IO, Points, Now) ->
    case read_metadata_inner(IO) of
        {ok, Metadata} ->
            do_update_points(IO, Points, Metadata, Now);
        error -> error
    end.

do_update_points(IO, Points, Metadata, Now) ->
    % first we distribute all points over the available retentions
    Archives = Metadata#whisper_metadata.archives,
    Distributed = distribute_points(Now, Points, Archives),

    lists:foreach(fun ({As, Ps}) ->
                          do_update_retention_points(IO, Metadata, As, Ps)
                  end, Distributed).


do_update_retention_points(_IO, _Metadata, _Archives, []) -> ok;
do_update_retention_points(IO, Metadata, [Archive | Lower], Points) ->
    Step = Archive#whisper_archive.seconds,

    % we combine all consecutive points (current-interval == last-interval + step)
    Consecutive = build_consecutive_points(Step, Points),

    % all those combined points are seeked + written to the archive at once
    % while respecting possible archive overlaps
    {BaseInterval, _Value} = point_at(IO, Archive#whisper_archive.offset),

    lists:foreach(fun(Ps) ->
                          write_consecutive_points(IO, Archive, BaseInterval, Ps)
                  end, Consecutive),

    % propagate to lower archives (if necessary)
    grouped_propagate_to_lowers(IO, Metadata, Archive, Lower, Points).


grouped_propagate_to_lowers(_IO, _Metadata, _Higher, [], _Points) -> ok;
grouped_propagate_to_lowers(IO, Metadata, Higher, [Lower | Ls], Points) ->
    GroupedPoints = group_points_to_lower_archive(Lower, Points),

    Propagate = lists:foldl(fun({TS, _}, DidPropagate) ->
                                    Changed = propagate_lower_archives(IO, Metadata, TS, Higher, [Lower]),
                                    DidPropagate or Changed
                            end, false, GroupedPoints),

    if Propagate == true ->
           grouped_propagate_to_lowers(IO, Metadata, Lower, Ls, Points);
       true -> ok
    end.


group_points_to_lower_archive(Archive, Ps) ->
    group_points_to_lower_archive(Archive, Ps, []).

group_points_to_lower_archive(_Archive, [], Acc) -> Acc;
group_points_to_lower_archive(Archive, [{TS, V} | Ps], []) ->
    group_points_to_lower_archive(Archive, Ps, [{interval_start(Archive, TS), V}]);
group_points_to_lower_archive(Archive, [{TS0, V} | Ps], [{TS1, _} | _]=Vs) ->
    TS = interval_start(Archive, TS0),
    if TS == TS1 ->
           group_points_to_lower_archive(Archive, Ps, Vs);
       true ->
           group_points_to_lower_archive(Archive, Ps, [{TS, V} | Vs])
    end.


safe_last([{TS, _}]) -> TS;
safe_last([_ | Xs]) -> safe_last(Xs).


write_consecutive_points(IO, Archive, BaseInterval, Points) ->
    Position = get_data_point_offset(Archive, safe_last(Points), BaseInterval),
    {Bytes, Length} = lists:foldl(fun ({T, Value}, {BS, Len0}) ->
                                          PointBytes = data_point(T, Value),
                                          Len = Len0 + ?POINT_SIZE,
                                          case Len0 of
                                              0 -> {<<PointBytes/binary>>, Len};
                                              _ -> {<<PointBytes/binary, BS/binary>>, Len}
                                          end
                                  end, {<<>>, 0}, Points),

    % we have to determine if this point sequence overlaps the archive's end
    ArchiveEnd = Archive#whisper_archive.offset + Archive#whisper_archive.size,
    BytesOverlap = (Position + Length) - ArchiveEnd,
    if BytesOverlap > 0 ->
           lager:debug("point-seq of len ~w overlaps by ~w bytes: ~p", [Length, BytesOverlap, Points]),
           FirstPart = binary:part(Bytes, 0, Length - BytesOverlap),
           ok = write_at(IO, FirstPart, Position),
           LastPart = binary:part(Bytes, byte_size(Bytes), -BytesOverlap),
           ok = write_at(IO, LastPart, Archive#whisper_archive.offset);
       true ->
           lager:debug("writing point-seq of len ~w: ~p", [Length, Points]),
           ok = write_at(IO, Bytes, Position)
    end.


build_consecutive_points(Step, Points) ->
    build_consecutive_points(Step, Points, [], []).

build_consecutive_points(_Step, [], [], Acc) -> Acc;
build_consecutive_points(_Step, [], Pss, Acc) -> [Pss | Acc];
build_consecutive_points(Step, [{T, V} | Ps], [], Acc) ->
    Aligned = T - (T rem Step),
    build_consecutive_points(Step, Ps, [{Aligned, V}], Acc);
build_consecutive_points(Step, [{T1, V} | Ps], [{T0, _} | _]=Pss, Acc) ->
    Aligned = T1 - (T1 rem Step),
    ExpectedNextTimeStamp = T0 + Step,

    % if the current timestamp is the next consecutive timestamp
    % according to the last timestamp and the archive's step we
    % keep accumulating this point sequence
    if ExpectedNextTimeStamp == Aligned ->
           build_consecutive_points(Step, Ps, [{Aligned, V} | Pss], Acc);
       true ->
           build_consecutive_points(Step, Ps, [{Aligned, V}], [Pss | Acc])
    end.


distribute_points(Now, Points, Archives) ->
    Result = distribute_points(Now, Points, Archives, [], []),
    lists:reverse(Result).

distribute_points(_Now, _Ps, [], _, Acc) -> Acc;
distribute_points(_Now, [], Archives, APs, Acc) ->
    [{Archives, APs} | Acc];
distribute_points(Now, [{TS, _}=P | Ps]=Pss, [Archive | As]=Ass, APs, Acc) ->
    Age = Now - TS,
    if Age >= Archive#whisper_archive.retention ->
           % point exceeds current archive's retention
           % -> pack up current archive with its points and
           % continue with next archive
           distribute_points(Now, Pss, As, [], [{Ass, APs} | Acc]);
       true ->
           distribute_points(Now, Ps, Ass, [P | APs], Acc)
    end.


-spec update_point(binary(), number(), integer()) -> tuple().
update_point(File, Value, TimeStamp) ->
    case file:open(File, [write, read, binary, raw]) of
        {ok, IO} ->
            try do_update(IO, Value, TimeStamp)
                after file:close(IO)
            end;
        Error -> Error
    end.


do_update(IO, Value, TimeStamp) ->
    case read_metadata_inner(IO) of
        {ok, Metadata} ->
            write_point(IO, Metadata, Value, TimeStamp);
        error -> error
    end.


interval_start(Archive, TimeStamp) ->
    TimeStamp - (TimeStamp rem Archive#whisper_archive.seconds).


collect_fold_func({_TS, null}, List) -> List;
collect_fold_func({_TS, Value}, List) -> [Value | List].


collect_series_values(Archive, Interval, Values) ->
    fold_series_values(fun collect_fold_func/2, Archive, Interval, Values).


fold_series_values(Fun, #whisper_archive{seconds=Step}, Interval, Values) ->
    fold_series_values(Fun, Step, Interval, Values, [], 0).

fold_series_values(Fun, Step, Interval, <<TS:32/integer-unsigned-big, Value:64/float-big, Rst/binary>>, Acc, Seen) ->
    Result = if
                 % we compare the timestamp of the data point we just read (TS) with
                 % the timestamp value we are expecting (Interval)
                 % only if these timestamps match up we consider that data point a valid one
                 % NOTE: this is *not* an error - the archive may very well contain sparse values
                 Interval == TS ->
                     Fun({Interval, Value}, Acc);
                 true ->
                     Fun({Interval, null}, Acc)
             end,
    fold_series_values(Fun, Step, Interval + Step, Rst, Result, Seen + 1);
fold_series_values(_Fun, _Step, _Interval, <<>>, Result, Seen) -> {Result, Seen}.


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
            false
    end;
propagate_lower_archives(_, _, _, _, []) -> true.


write_propagated_values(IO, Header, Lower, LowerInterval, Values, NumPoints) ->
    AggType = Header#whisper_metadata.aggregation,
    XFF = Header#whisper_metadata.xff,
    NumValues = length(Values),
    KnownPercentage = NumValues / NumPoints,
    if
        % we do have enough data points to calculate an aggregation
        KnownPercentage >= XFF ->
            % usually the file factor (XFF) should make sure we
            % have at least 'some' values to aggregate but you never
            % know what configuration we have at hand
            % e.g. average with XFF == 0.0
            case aggregate(AggType, Values, NumValues, NumPoints) of
                null ->
                    false;
                AggValue ->
                    lager:debug("calculated aggregate [~p] ~p [values ~p]", [AggType, AggValue, Values]),
                    Point = data_point(LowerInterval, AggValue),
                    {BaseInterval, _} = point_at(IO, Lower#whisper_archive.offset),
                    Offset = get_data_point_offset(Lower, LowerInterval, BaseInterval),
                    ok = write_at(IO, Point, Offset),
                    true
            end;
        true ->
            false
    end.


aggregate(sum, Values, _, _) -> lists:sum(Values);
aggregate(_, [], _, _) -> null;
aggregate(average, Values, NumValues, _) -> lists:sum(Values) / NumValues;
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

            propagate_lower_archives(IO, Header, Interval, Archive, LowerArchives),
            ok
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

distribute_points_test_() ->
    Archive0 = make_archive_header(0, 10, 360),
    Archive1 = make_archive_header(0, 60, 1440),
    Archives = [Archive0, Archive1],

    [?_assertEqual([{Archives, [{100, 3}, {110, 2}, {120, 1}]}],
                   distribute_points(150, [{120, 1}, {110, 2}, {100, 3}], Archives)),

     ?_assertEqual([{Archives, []}, {[Archive1], [{100, 3}, {110, 2}, {120, 1}]}],
                   distribute_points(4000, [{120, 1}, {110, 2}, {100, 3}], Archives)),

     ?_assertEqual([{Archives, [{1000, 2}, {1010, 1}]}, {[Archive1], [{100, 3}]}],
                   distribute_points(4000, [{1010, 1}, {1000, 2}, {100, 3}], Archives)),

     ?_assertEqual([{Archives, []}, {[Archive1], []}],
                   distribute_points(100000, [{1010, 1}, {1000, 2}, {100, 3}], Archives))
    ].

sort_by_newest_test_() ->
    [?_assertEqual([{120, 3}, {110, 235}, {100, -3.34}],
                  sort_by_newest([{110, 235}, {120, 3}, {100, -3.34}]))
    ].

build_consecutive_points_test_() ->
    [?_assertEqual([[{130, 0}, {120, 34623}], [{100, 225}]],
                   build_consecutive_points(10, [{100, 225}, {120, 34623}, {130, 0}])),

     ?_assertEqual([[{130, 0}, {120, 34623}, {110, 225}]],
                   build_consecutive_points(10, [{110, 225}, {120, 34623}, {130, 0}]))
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
                      {ok, CreateM, ReadM} = test_util:with_tempfile(Fun),
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

update_points_test_() ->
    Archives1 = [{10, 60}],
    Archives2 = [{10, 60}, {60, 600}],
    Check = fun(Archives, Points, From, To) ->
                    WithF = fun(File) ->
                                    {ok, _} = create(File, Archives, average, 0.5),
                                    lists:foreach(fun({T, P}) -> ok = update_point(File, P, T) end, Points),
                                    Series1 = fetch(File, From, To),
                                    ok = update_points(File, Points),
                                    Series2 = fetch(File, From, To),
                                    {ok, Series1, Series2}
                            end,
                    {ok, S1, S2} = test_util:with_tempfile(WithF),
                    [?_assertEqual(S1, S2)]
            end,
    Now = erlang:system_time(second),
    lists:flatten([Check(Archives1, [{Now, 100}], Now, Now+60),
                   Check(Archives1, [{Now, 100}], Now-20, Now),
                   Check(Archives1, [{Now, 100}, {Now+10, 110}, {Now+20, 120}, {Now+30, 130}], Now, Now+60),
                   Check(Archives1, [{Now, 100}, {Now+10, 110}, {Now+20, 120}, {Now+40, 140}], Now, Now+60),
                   Check(Archives2, [{Now, 100}], Now, Now+60),
                   Check(Archives2, [{Now, 100}, {Now+10, 110}, {Now+20, 120}, {Now+30, 130}], Now, Now+60),
                   Check(Archives2, [{Now, 100}, {Now+10, 110}, {Now+20, 120}, {Now+40, 140}], Now, Now+60)
                  ]).

-endif. % TEST

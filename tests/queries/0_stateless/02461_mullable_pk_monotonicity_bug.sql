create table tab (x Nullable(UInt8)) engine = MergeTree order by x settings allow_nullable_key = 1, index_granularity = 2;
insert into tab select number from numbers(4);

set max_rows_to_read = 2;
SELECT x + 1 FROM tab where plus(x, 1) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1, x) <= 2 order by x;

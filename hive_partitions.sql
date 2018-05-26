CREATE TABLE IF NOT EXISTS stocks_partition (
     exch string,
     symbol string,
     ymd string,
     price_open float,
     price_high float,
     price_low float,
     price_close float,
     volume int,
     price_adj_close float)
     partitioned by (sym STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
/*
OK
Time taken: 0.033 seconds
*/

hive> INSERT into stocks_partition
    > PARTITION (sym='B7J')
    > SELECT * FROM stocks s WHERE s.symbol = 'B7J';

hive> describe forrmatted stocks_partition;


hive> !hadoop fs -ls /user/hive/warehouse/demo.db/stocks_partition;
Found 1 items
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-25 11:01 /user/hive/warehouse/demo.db/stocks_partition/sym=B7J

hive> INSERT OVERWRITE TABLE stocks_partition
    > PARTITION (sym='BB3')
    > SELECT * FROM stocks s
    > WHERE s.symbol = 'BB3';

!hadoop fs  -ls /user/hive/warehouse/demo.db/stocks_partition ;
/*
Found 2 items
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-25 11:01 /user/hive/warehouse/demo.db/stocks_partition/sym=B7J
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-25 11:08 /user/hive/warehouse/demo.db/stocks_partition/sym=BB3
*/

SELECT * FROM stocks_partition
    > WHERE sym='B7J';
/*
Extracts records from the partition (i.e: FASTER WAY)
*/

hive> INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/stocks_partition/stocks-ZUU' SELECT * FROM stocks WHERE symbol = 'ZUU';

hive> !hadoop fs -ls  /user/hive/warehouse/stocks_partition;
/*
Found 5 items
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-25 11:23 /user/hive/warehouse/stocks_partition/stocks-ZUU
drwxrwxrwt   - hirwuser697    hive          0 2018-05-21 13:11 /user/hive/warehouse/stocks_partition/sym=APPL
drwxrwxrwt   - hirwuser697    hive          0 2018-05-21 13:07 /user/hive/warehouse/stocks_partition/sym=B7J
drwxrwxrwt   - hirwuser697    hive          0 2018-05-21 13:08 /user/hive/warehouse/stocks_partition/sym=BB3
drwxrwxrwt   - hirwuser697    hive          0 2018-05-21 13:10 /user/hive/warehouse/stocks_partition/sym=GEK
*/

hive> ALTER TABLE stocks_partition ADD IF NOT EXISTS
    > PARTITION (sym='ZUU')
    > LOCATION '/user/hive/warehouse/demo.db/stocks_partition'
    > ;
/*
OK
Time taken: 0.288 seconds
*/

hive> SHOW PARTITIONS stocks_partition;
/*
OK
sym=B7J
sym=BB3
sym=ZUU
Time taken: 0.253 seconds, Fetched: 3 row(s)
*/
hive> FROM stocks s
    > INSERT OVERWRITE TABLE stocks_partition
    > PARTITION (sym='GEL')
    > SELECT * WHERE s.symbol = 'GEL'
    > INSERT OVERWRITE TABLE stocks_partition
    > PARTITION (sym='GEK')
    > SELECT * WHERE s.symbol = 'GEK';

hive> SHOW PARTITIONS stocks_partition;
/*
OK
sym=B7J
sym=BB3
sym=GEK
sym=GEL
sym=ZUU
Time taken: 0.02 seconds, Fetched: 5 row(s)
*/

hive> SHOW PARTITIONS stocks_partition;
/*
OK
sym=B7J
sym=BB3
sym=GEK
sym=GEL
sym=ZUU
Time taken: 0.234 seconds, Fetched: 5 row(s)
*/

hive> ALTER TABLE stocks_partition DROP IF EXISTS PARTITION (sym = 'GEL');
/*
Dropped the partition sym=GEL
OK
Time taken: 0.478 seconds
*/
hive> SHOW PARTITIONS stocks_partition;
/*
OK
sym=B7J
sym=BB3
sym=GEK
sym=ZUU
Time taken: 0.025 seconds, Fetched: 4 row(s)
*/

/* ENABLING DYNAMIC PARTITION */
SET hive.exec.dynamic.partition=true;

hive> CREATE TABLE IF NOT EXISTS stocks_dynamic_partition (
    > exch string,
    > symbol string,
    > ymd string,
    > price_open float,
    > price_high float,
    > price_low float,
    > price_close float,
    > volume int,
    > price_adj_close float)
    > PARTITIONED BY (exch_name STRING,yr STRING,sym STRING)
    > ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
/*
OK
Time taken: 0.11 seconds
*/
hive> insert overwrite table stocks_dynamic_partition
    > partition (exch_name='ABCSE' ,yr,sym)
    > select *,year(ymd),symbol
    > from stocks where year(ymd)='2001';

hive> SET hive.exec.max.dynamic.partitions=1000;
hive> SET hive.exec.max.dynamic.partitions.pernode=500;
hive> INSERT OVERWRITE TABLE stocks_dynamic_partition
    > PARTITION (exch_name='ABCSE',yr,sym)
    > SELECT * ,year(ymd),symbol
    > FROM stocks WHERE year(ymd) IN ('2001','2002','2003') and
    > symbol like 'B%';

hive> !hadoop fs -ls /user/hive/warehouse/demo.db/stocks_dynamic_partition;
/*
Found 1 items
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-25 14:21 /user/hive/warehouse/demo.db/stocks_dynamic_partition/exch_name=ABCSE

*/
hive> !hadoop fs -ls /user/hive/warehouse/demo.db/stocks_dynamic_partition/exch_name=ABCSE;
/*
Found 3 items
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-25 14:21 /user/hive/warehouse/demo.db/stocks_dynamic_partition/exch_name=ABCSE/yr=2001
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-25 14:21 /user/hive/warehouse/demo.db/stocks_dynamic_partition/exch_name=ABCSE/yr=2002
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-25 14:21 /user/hive/warehouse/demo.db/stocks_dynamic_partition/exch_name=ABCSE/yr=2003
*/
hive> !hadoop fs -ls /user/hive/warehouse/demo.db/stocks_dynamic_partition/exch_name=ABCSE/yr=2001;
/*
Found 118 items
*/

hive> SELECT * FROM stocks_dynamic_partition
    > WHERE yr==2003 and volume>50000;

hive> SET hive.mapred.mode=strict;
hive> SELECT * FROM stocks_dynamic_partition
    > WHERE volume>500000;
/*
ERROR OCCURED BECAUSE ,IT'S A RISKY QUERY ,IT WILL NOT WORK IN STRICT MODE
FAILED: SemanticException [Error 10041]: No partition predicate found for Alias "stocks_dynamic_partition" Table "stocks_dynamic_partition"

*/
hive> SET hive.mapred.mode=null;
/* hive is no more in strict mode */
hive> SELECT * FROM stocks_dynamic_partition
    > WHERE volume>500000;
/*
OK
ABCSE	B3	2001-07-13	57.5	58.07	56.6	58.0	596600	18.56	ABCSE	2001	B3
ABCSE	B33	2001-11-02	7.85	7.9	7.65	7.75	2684400	3.45	ABCSE	2001	B33
...
*/



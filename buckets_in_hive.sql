hive> CREATE TABLE IF NOT EXISTS stocks_bucket (
    >      exch string,
    >      symbol string,
    >      ymd string,
    >      price_open float,
    >      price_high float,
    >      price_low float,
    >      price_close float,
    >      volume int,
    >      price_adj_close float)
    >      partitioned by (exch_name STRING,yr STRING)
    >      CLUSTERED BY (symbol) INTO 5 BUCKETS 
    >      ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
/*OK
Time taken: 0.245 seconds
hive> show tables;
OK
stocks
stocks_bucket
stocks_dynamic_partition
stocks_partition
Time taken: 0.081 seconds, Fetched: 4 row(s)
*/

hive> SET hive.exec.max.dynamic.partitions=15000;
hive> SET hive.exec.max.dynamic.partitions.pernode=5000;
hive> SET hive.enforce.bucketing =true;
hive> SET hive.exec.max.dynamic.partition=true;

hive> INSERT OVERWRITE TABLE stocks_bucket
    > PARTITION (exch_name='ABCSE',yr)
    > SELECT *,year(ymd)
    > FROM stocks WHERE year(ymd) IN('2001','2002','2003') and symbol like 'B%'; 

hive> DESCRIBE FORMATTED stocks_bucket;
/*
To get the location of the table
*/

hive> !hadoop fs -ls /user/hive/warehouse/demo.db/stocks_bucket;
/*
Found 1 items
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-26 02:52 /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE
*/

hive> !hadoop fs -ls /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE;
/*
Found 3 items
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-26 02:52 /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE/yr=2001
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-26 02:52 /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE/yr=2002
drwxrwxrwt   - hirwuser150430 hive          0 2018-05-26 02:52 /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE/yr=2003
*/
hive> !hadoop fs -ls /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE/yr=2001;
/*
Found 5 items
-rwxrwxrwt   3 hirwuser150430 hive     582826 2018-05-26 02:52 /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE/yr=2001/000000_0
-rwxrwxrwt   3 hirwuser150430 hive     869067 2018-05-26 02:52 /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE/yr=2001/000001_0
-rwxrwxrwt   3 hirwuser150430 hive     991279 2018-05-26 02:52 /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE/yr=2001/000002_0
-rwxrwxrwt   3 hirwuser150430 hive     835486 2018-05-26 02:52 /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE/yr=2001/000003_0
-rwxrwxrwt   3 hirwuser150430 hive     699163 2018-05-26 02:52 /user/hive/warehouse/demo.db/stocks_bucket/exch_name=ABCSE/yr=2001/000004_0
*/

hive> SELECT * FROM stocks TABLESAMPLE(BUCKET 3 OUT OF 5 ON SYMBOL) s;
/*
OK
..

Bucketing from a non bucket table
TIME CONSUMING BECAUSE BUCKETS ARE RANDOMLY ORGANISED THEN ACCESSED..
*/

hive> SELECT * FROM stocks_bucket TABLESAMPLE(BUCKET 3 OUT OF 5 ON SYMBOL) s;
/*
OK
..
FASTER
Bucketing from a bucket table
*/




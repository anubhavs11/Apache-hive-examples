CREATE DATABASE stocks_db;

USE stocks_db
/*  METHOD 1 */
CREATE EXTERNAL TABLE IF NOT EXISTS stocks_tb (
     exchg STRING,
     symbol STRING,
     ymd STRING,
     open FLOAT,
     high FLOAT,
     low FLOAT,
     close FLOAT,
     volume int,
     adj_close FLOAT)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
     LOCATION '/user/hirw/input/stocks';


SELECT * FROM stocks LIMIT 100;

!hadoop fs -cp /user/hirw/input/stocks/stocks anubhav;

!hadoop fs -ls anubhav;
/*
Found 1 items
-rw-r--r--   3 hirwuser150430 hirwuser150430  428223209 2018-05-25 01:37 anubhav/stocks
*/

/*  METHOD 2 */
LOAD DATA INPATH 'anubhav/stocks' INTO TABLE stocks_tb;
/*
Loading data to table demo.stocks_tb
Table demo.stocks_tb stats: [numFiles=1, totalSize=428223209]
OK
Time taken: 0.281 seconds
*/

!hadoop fs -ls anubhav;
/*
file MOVED
*/

/*  METHOD 3 */
CREATE TABLE stocks_ctas AS SELECT * FROM stocks LIMIT 100;
/* COPY data from another table */

!hadoop fs -ls /user/hive/warehouse/demo.db/stocks_tb;
/*
Found 1 items
-rw-r--r--   3 hirwuser150430 hirwuser150430  428223209 2018-05-25 01:37 /user/hive/warehouse/demo.db/stocks_tb/stocks
*/

/*  METHOD 4 */
INSERT INTO TABLE stocks_tb SELECT * FROM stocks;

!hadoop fs -ls /user/hive/warehouse/demo.db/stocks_tb;
/*
Found 2 items
-rwxrwxrwt   3 hirwuser150430 hive  259093446 2018-05-25 02:21 /user/hive/warehouse/demo.db/stocks_tb/000000_0
-rwxrwxrwt   3 hirwuser150430 hive  154220192 2018-05-25 02:21 /user/hive/warehouse/demo.db/stocks_tb/000001_0
*/

INSERT OVERWRITE TABLE stocks_tb SELECT * FROM stocks;

!hadoop fs -ls /user/hive/warehouse/demo.db/stocks_tb;
/*
Found 2 items
-rwxrwxrwt   3 hirwuser150430 hive  259093446 2018-05-25 02:23 /user/hive/warehouse/demo.db/stocks_tb/000000_0
-rwxrwxrwt   3 hirwuser150430 hive  154220192 2018-05-25 02:22 /user/hive/warehouse/demo.db/stocks_tb/000001_0
*/ 


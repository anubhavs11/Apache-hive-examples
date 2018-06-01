hive> use stocks_db;

hive>  CREATE EXTERNAL TABLE IF NOT EXISTS dividends(
          exch STRING,
          symbol STRING,
          ymd STRING,
          divident FLOAT
          )
          ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
          LOCATION '/user/hirw/input/dividends';

hive> describe stocks;
/*
OK
exch                	string              	                    
symbol              	string              	                    
ymd                 	string              	                    
price_open          	float               	                    
price_high          	float               	                    
price_low           	float               	                    
price_close         	float               	                    
volume              	int                 	                    
price_adj_close     	float               	                    
Time taken: 0.048 seconds, Fetched: 9 row(s)
*/

hive> SELECT s.exch,s.symbol,s.ymd,s.price_close,d.divident
     FROM stocks s INNER JOIN dividends d
     ON s.symbol=d.symbol AND s.ymd = d.ymd;
--Time taken: 36.167 seconds, Fetched: 3322 row(s)


--INNER JOIN WITH BETTER TABLE OPTIMIZATION
hive> SELECT s.exch,s.symbol,s.ymd,s.price_close,d.divident
     FROM dividends d INNER JOIN stocks s
     ON s.symbol=d.symbol AND s.ymd = d.ymd;
--Time taken: 34.307 seconds, Fetched: 3322 row(s)


hive> SELECT /*+ STREAMTABLE(s) */ 				    s.exch,s.symbol,s.ymd,s.price_close,d.divident
     FROM dividends d INNER JOIN stocks s
     ON s.symbol=d.symbol AND s.ymd = d.ymd;
--Time taken: 32.28 seconds, Fetched: 3322 row(s)

hive> SET hive.auto.convert.join = true;

hive> SELECT /*+ MAPJOIN(d) */ 				       		  	s.exch,s.symbol,s.ymd,s.price_close,d.divident
      FROM dividends d INNER JOIN stocks s
      ON s.symbol=d.symbol AND s.ymd = d.ymd;
--Time taken: 34.329 seconds, Fetched: 3322 row(s)


--SMB JOIN
--ENABLE BUCKETING
SET hive.enforce.bucketing = true;

CREATE TABLE IF NOT EXISTS stocks_smb (
          exch string,
          symbol string,
          ymd string,
          price_open float,
         price_high float,
          price_low float,
          price_close float,
          volume int,
          price_adj_close float)
          CLUSTERED BY (symbol) SORTED BY (symbol) INTO 10 BUCKETS 
          ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE stocks_smb
SELECT * FROM stocks

CREATE EXTERNAL TABLE IF NOT EXISTS dividends_smb(
          exch STRING,
          symbol STRING,
          ymd STRING,
          divident FLOAT
          )
          CLUSTERED BY (symbol) SORTED BY (symbol) INTO 5 BUCKETS 
          ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


INSERT OVERWRITE TABLE dividends_smb
SELECT * FROM dividends;

SET hive.auto.convert.sortmerge.join=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;
SET hive.auto/convert.sortmerge.join.noconditionaltask=true;


SELECT s.exch,s.symbol,s.ymd,s.price_close,d.divident
     FROM dividends_smb d INNER JOIN stocks_smb s
     ON s.symbol=d.symbol AND s.ymd = d.ymd;
--Time taken: 29.308 seconds, Fetched: 3322 row(s)



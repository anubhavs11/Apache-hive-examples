SET hive.exec.compress.intermediate = true;
SET mapred.map.output.compression.codec=org.apache.hadoop .io.compress.GZipCodec;
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
CREATE TABLE stocks_comp_on_gz
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
     AS SELECT * FROM stocks;


!hadoop fs -ls /user/hive/warehouse/demo.db/stocks_comp_on_gz;
/*
Found 2 items
-rwxrwxrwt   3 hirwuser150430 hive   64525019 2018-05-31 14:53 /user/hive/warehouse/demo.db/stocks_comp_on_gz/000000_0.gz
-rwxrwxrwt   3 hirwuser150430 hive   38777279 2018-05-31 14:53 /user/hive/warehouse/demo.db/stocks_comp_on_gz/000001_0.gz
*/
SELECT * FROM stocks_comp_on_gz  LIMIT 10;
/*
OK
ABCSE	B7J	2010-02-08	8.63	8.7	8.57	8.64	78900	8.64
ABCSE	B7J	2010-02-05	8.63	8.71	8.31	8.58	218700	8.58
ABCSE	B7J	2010-02-04	8.88	8.88	8.59	8.66	89900	8.66
ABCSE	B7J	2010-02-03	8.83	8.92	8.8	8.89	119000	8.89
ABCSE	B7J	2010-02-02	8.77	8.9	8.73	8.87	51900	8.87
ABCSE	B7J	2010-02-01	8.69	8.77	8.66	8.75	38600	8.75
ABCSE	B7J	2010-01-29	8.81	8.81	8.56	8.57	91700	8.57
ABCSE	B7J	2010-01-28	8.9	8.9	8.6	8.69	92100	8.69
ABCSE	B7J	2010-01-27	8.87	8.87	8.68	8.79	82400	8.79
ABCSE	B7J	2010-01-26	8.83	8.92	8.71	8.82	106000	8.82
Time taken: 0.052 seconds, Fetched: 10 row(s)
*/



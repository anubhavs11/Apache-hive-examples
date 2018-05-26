hive> CREATE EXTERNAL TABLE IF NOT EXISTS dividends(
     exch STRING,
     symbol STRING,
     ymd STRING,
     divident FLOAT
     )
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
     LOCATION '/user/hirw/input/dividends';
/*
OK
Time taken: 0.043 seconds
hive> 
*/

hive> SELECT * FROM dividends LIMIT 10;
/*
OK
..
Time taken: 0.337 seconds, Fetched: 10 row(s)
*/

/* INNER JOIN */
SELECT s.exchg,s.symbol,s.ymd,s.close,d.divident
FROM stocks s INNER JOIN dividends d
ON s.symbol=d.symbol AND s.ymd = d.ymd;

/* LEFT OUTER JOIN */
SELECT s.exchg,s.symbol,s.ymd,s.close,d.divident
FROM stocks s LEFT JOIN dividends d
ON s.symbol=d.symbol AND s.ymd = d.ymd;

SELECT s.exchg,s.symbol,s.ymd,s.close,d.divident
FROM stocks s LEFT OUTER JOIN dividends d
ON s.symbol=d.symbol AND s.ymd = d.ymd
WHERE D.divident IS NOT NULL LIMIT 10;


/* RIGHT OUTER JOIN */
SELECT s.exchg,s.symbol,s.ymd,s.close,d.divident
FROM stocks s RIGHT OUTER JOIN dividends d
ON s.symbol=d.symbol AND s.ymd = d.ymd;


/* FULL OUTER JOIN */
SELECT s.exchg,s.symbol,s.ymd,s.close,d.divident
FROM stocks s FULL OUTER JOIN dividends d
ON s.symbol=d.symbol AND s.ymd = d.ymd;


/* LEFT SEMI JOIN */
SELECT s.symbol,s.ymd,s.close
FROM stocks s LEFT SEMI JOIN dividends d
ON s.symbol=d.symbol AND s.ymd = d.ymd;


/* INEQUALITY JOIN */
SELECT s.symbol,s.ymd,s.close
FROM stocks s LEFT SEMI JOIN dividends d
ON s.ymd > d.ymd;
/*
FAILED: SemanticException [Error 10017]: Line 3:3 Both left and right aliases encountered in JOIN 'ymd'
*/

/* CROSS INEQUALITY JOIN */
SELECT s.symbol,s.ymd,s.close
FROM stocks s CROSS JOIN dividends d
ON s.symbol=d.symbol AND s.ymd = d.ymd;


/*  (Imp**) MULTITABLE JOINS AND NUMBER OF MAP/REDUCE JOBS */
--same key ..only one MAP/REDUCE JOB
SELECT a.val,b.val,c.val FROM a JOIN b ON (a.key=b.key1) JOIN c ON(C.key = b.key1)

/*2 MAP/REDUCE JOB ..first MAP/REDUCE JOB joins a with b and the 
result are then joined with c in the 2nd MAP/REDUCE JOB*/
SELECT a.val,b.val,c.val FROM a JOIN b ON (a.key=b.key1) JOIN c ON(C.key = b.key2)
 




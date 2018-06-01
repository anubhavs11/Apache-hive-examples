
!hadoop fs -cat avro/stocks.avro.schema;
--to display the format of the schema

CREATE TABLE IF NOT EXISTS stocks_avro
	ROW FORMAT
	SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
	STORED AS
	INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
	OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
	TBLPROPERTIES ('avro.schema.url'='hdfs:///user/hirw/avro/stocks.avro.schema');

--Insert in to stocks_avro from stocks table. This will result in files under stocks_avro stored in Avro format.
INSERT OVERWRITE TABLE stocks_avro
	SELECT * FROM stocks;

!hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_avro;

rm 000000_0

hadoop fs -copyToLocal /user/hive/warehouse/stocks_db.db/stocks_avro/000000_0 .

vi 000000_0

SELECT * FROM stocks_avro LIMIT 10;



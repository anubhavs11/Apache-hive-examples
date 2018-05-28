/*


City Of Chicago Employee Dataset

Input (HDFS) - /user/hirw/input/employee-pig/employee_dataset_chicago

Column Delimiter Comma (,)

Position
	

Column Value
	

Data Type

1
	

Employee Id
	

INT

2
	

Employee Name

(First Name <space> Last Name)
	

STRING

3
	

Position Title
	

STRING

4
	

Department Id
	

INT

5
	

Employee Annual Salary
	

FLOAT

Sample Record

1512,ACCURSO MARY K,ASST ADMINISTRATIVE SECRETARY I,43,21548.8

City of Chicago Department Dataset

Input (HDFS) - /user/hirw/input/employee-pig/department_dataset_chicago

Column Delimiter Semicolon (;)

Position
	

Column Value
	

Data Type

1
	

Department Id
	

INT

2
	

Department Name
	

CHARARRAY

3
	

Address
	

MAP[]

3 keys street, city, state

Sample Record

56;BOARD OF ETHICS;[street#226 S Wabash Ave,city#Chicago,state#IL]

City of Chicago Bonus Dataset

Input Location - /user/hirw /input/employee-pig/employee_bonus_chicago

Column Delimiter Comma (,)

Position
	

Column Value
	

Data Type

1
	

Employee Id
	

INT

2
	

Bonus
	

FLOAT

Sample Record

2281,25000.50

    Copy the file /hirw-workshop/input/employee-pig/employee_dataset_chicago to a folder of your choice in HDFS
    Create a database with name as you cluster username HDFS
    Switch to use the database created above HDFS
    Create a managed table and name it employees from the file from (insert assignment # 17) HDFS
    Load the employees table from the file created from insert assignment # 17
    Show the HDFS location where the table is copied to in HDFS
    Explain what you see when you look up for the file in HDFS created from insert assignment #17
    Describe (to show formatted output) the table employees
    Alter table and database to add DBPROPERTIES and TBLPROPERTIES to include created by, creation time and comments
    Display all employees table with salary more than $100,000
    Alter table employees and add a column with name and data type of your choice
    Describe the table employees
    Create a table with name employees_ptn which is partitioned by Department Id
    Describe the table employees_ptn
    Create 4 partitions in employees_ptn table to store 4 departments in it appropriate partition. For example, all data for Department Id 43 should be stored in Department Id 43 partition.
    Show the partitions in employees_ptn table
    Display only one partition details from employees_ptn. Use the partition column for filtering.
    Drop one of the 4 partitions from employees_ptn table
    Select all the rows from employees_ptn table
    Select all the rows from employees_dynamic_ptn table with the same structure as employees_ptn
    Do a dynamic insert in to employees_dynamic_ptn table
    Create an external table named employees_external
    Load the data in to employees_external and display all the records from employees_external
    What is the significance of using LOCATION clause when creating a table
    Drop employees, employees_ptn and employees_external tables




*/

hive> !hadoop fs -ls /user/hirw/input/employee-pig;
/*Found 3 items
-rw-r--r--   3 hirw hirw       1026 2017-02-15 02:16 /user/hirw/input/employee-pig/department_dataset_chicago
-rw-r--r--   3 hirw hirw        493 2017-02-15 02:16 /user/hirw/input/employee-pig/employee_bonus_chicago
-rw-r--r--   3 hirw hirw     535233 2017-01-26 00:10 /user/hirw/input/employee-pig/employee_dataset_chicago
*/
hive> !hadoop fs -cp /user/hirw/input/employee-pig/ anubhav;
hive> !hadoop fs -ls anubhav;
/*
Found 3 items
-rw-r--r--   3 hirwuser150430 hirwuser150430       1026 2018-05-27 05:23 anubhav/department_dataset_chicago
-rw-r--r--   3 hirwuser150430 hirwuser150430        493 2018-05-27 05:23 anubhav/employee_bonus_chicago
-rw-r--r--   3 hirwuser150430 hirwuser150430     535233 2018-05-27 05:23 anubhav/employee_dataset_chicago
*/
hive> CREATE DATABASE anubhav;

/*OK
Time taken: 0.022 seconds
*/
hive> USE anubhav;
/*
OK
Time taken: 0.011 seconds
*/

hive> CREATE TABLE IF NOT EXISTS employee(
     EID int,
     ENAME string,
     PTITLE string,
     DID int,
     ESAL float)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
/*
OK
Time taken: 0.03 seconds
*/
hive> LOAD DATA INPATH '/user/hirwuser150430/anubhav/employee_dataset_chicago' INTO TABLE employee;
/*
Loading data to table anubhav.employee
Table anubhav.employee stats: [numFiles=1, totalSize=535233]
OK
Time taken: 0.329 seconds
*/
hive> DESCRIBE FORMATTED employee;
/*OK
# col_name            	data_type           	comment             
	 	 
eid                 	int                 	                    
ename               	string              	                    
ptitle              	string              	                    
did                 	int                 	                    
esal                	float               	                    
	 	 
# Detailed Table Information	 	 
Database:           	anubhav             	 
Owner:              	hirwuser150430      	 
CreateTime:         	Sun May 27 05:33:12 UTC 2018	 
LastAccessTime:     	UNKNOWN             	 
Protect Mode:       	None                	 
Retention:          	0                   	 
Location:           	hdfs://ip-172-31-45-216.ec2.internal:8020/user/hive/warehouse/anubhav.db/employee	 
Table Type:         	MANAGED_TABLE       	 
Table Parameters:	 	 
	COLUMN_STATS_ACCURATE	true                
	numFiles            	1                   
	totalSize           	535233              
	transient_lastDdlTime	1527399564          
	 	 
# Storage Information	 	 
SerDe Library:      	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe	 
InputFormat:        	org.apache.hadoop.mapred.TextInputFormat	 
OutputFormat:       	org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat	 
Compressed:         	No                  	 
Num Buckets:        	-1                  	 
Bucket Columns:     	[]                  	 
Sort Columns:       	[]                  	 
Storage Desc Params:	 	 
	field.delim         	,                   
	serialization.format	,                   
Time taken: 0.086 seconds, Fetched: 34 row(s)
*/
hive> !hadoop fs -ls /user/hive/warehouse/anubhav.db/employee;
/*
Found 1 items
-rw-r--r--   3 hirwuser150430 hirwuser150430     535233 2018-05-27 05:23 /user/hive/warehouse/anubhav.db/employee/employee_dataset_chicago
*/

hive> !hadoop fs -ls anubhav;
/*
Found 2 items
-rw-r--r--   3 hirwuser150430 hirwuser150430       1026 2018-05-27 05:23 anubhav/department_dataset_chicago
-rw-r--r--   3 hirwuser150430 hirwuser150430        493 2018-05-27 05:23 anubhav/employee_bonus_chicago
*/
hive> ALTER TABLE employee SET TBLPROPERTIES('creater'='Anubhav');
--OK
--Time taken: 0.035 seconds
hive> ALTER DATABASE anubhav SET DBPROPERTIES('Date'= '27-05-2018');
--OK
--Time taken: 0.024 seconds

hive> SELECT * FROM employee WHERE ESAL>100000 LIMIT 10;
/*
OK
1474	ABDOLLAHZADEH ALI	PARAMEDIC I/C	328	111996.0
1475	ABDUL-KARIM MUHAMMAD A	ENGINEERING TECHNICIAN VI	328	156420.0
1491	ABRAMS SAMUEL	POOL MOTOR TRUCK DRIVER	328	129108.0
1501	ABSTON KATHY A	POLICE OFFICER	43	134124.0
1561	ADAMOW JEFFREY W	FIREFIGHTER-EMT	43	115980.0
1573	ADAMS EULA M	DATA ENTRY OPERATOR	210	115224.0
1580	ADAMS JR JERRY M	POLICE OFFICER	210	120444.0
1602	ADAMS VICKY M	POLICE OFFICER	210	186576.0
1606	ADCOCK MARK	FOREMAN OF WINDOW WASHERS	210	109032.0
1610	ADDISON LEWIS	LIBRARY ASSOCIATE	210	103043.2
Time taken: 0.052 seconds, Fetched: 10 row(s)
*/

hive> ALTER TABLE employee ADD COLUMNS (experience float);
--OK
--Time taken: 0.031 seconds

hive> DESCRIBE employee;
/*
OK
eid                 	int                 	                    
ename               	string              	                    
ptitle              	string              	                    
did                 	int                 	                    
esal                	float               	                    
experience          	float               	                    
Time taken: 0.025 seconds, Fetched: 6 row(s)
*/

hive> CREATE TABLE IF NOT EXISTS employees_ptn(
          EID int,
          ENAME string,
          PTITLE string,
          DID int,
          ESAL float)
          PARTITIONED BY (DepID int)
          ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
--OK
--Time taken: 0.009 seconds
hive> DESCRIBE employees_ptn;
/*
OK
eid                 	int                 	                    
ename               	string              	                    
ptitle              	string              	                    
did                 	int                 	                    
esal                	float               	                    
depid               	int                 	                    
	 	 
# Partition Information	 	 
# col_name            	data_type           	comment             
	 	 
depid               	int                 	                    
Time taken: 0.027 seconds, Fetched: 11 row(s)
*/
hive> SHOW PARTITIONS employees_ptn;
--OK
--Time taken: 0.162 seconds
hive> DESCRIBE employee;
/*
OK
eid                 	int                 	                    
ename               	string              	                    
ptitle              	string              	                    
did                 	int                 	                    
esal                	float               	                    
Time taken: 0.021 seconds, Fetched: 5 row(s)
*/


hive> FROM employee e
     INSERT OVERWRITE TABLE employees_ptn
     PARTITION (depid=43)
     SELECT * WHERE e.DID = 43
     INSERT OVERWRITE TABLE employees_ptn
     PARTITION (depid=328)
     SELECT * WHERE e.DID = 328
     INSERT OVERWRITE TABLE employees_ptn
     PARTITION (depid=210)
     SELECT * WHERE e.DID = 210
     INSERT OVERWRITE TABLE employees_ptn
     PARTITION (depid=743)
     SELECT * WHERE e.DID = 743;

hive SHOW PARTITIONS employees_ptn;
/*
OK
depid=210
depid=328
depid=43
depid=743
Time taken: 0.019 seconds, Fetched: 4 row(s)
*/
hive> SELECT * FROM employees_ptn WHERE depid=43 LIMIT 10;;
/*
OK
1499	ABRON FLOYD	POLICE OFFICER	43	57828.0	43
1500	ABRONS KENNETH L	ELECTRICAL MECHANIC	43	59748.0	43
1501	ABSTON KATHY A	POLICE OFFICER	43	134124.0	43
1502	ABUBAKER MOHAMMED G	SENIOR ENVIRONMENTAL INSPECTOR	43	66552.0	43
1503	ABUTALEB AHMAD H	CIVIL ENGINEER II	43	57900.0	43
1504	ABUZANAT ABDALLA H	POLICE OFFICER (ASSIGNED AS EVIDENCE TECHNICIAN)	43	66552.0	43
1505	ACCARDI JOHNNA L	MOTOR TRUCK DRIVER	43	57828.0	43
1506	ACCARDO ANTHONY D	CONSTRUCTION LABORER	43	45372.0	43
1507	ACCARDO JENNIFER A	POLICE OFFICER	43	57900.0	43
1508	ACCARDO ROSEMARY	POLICE OFFICER	43	57900.0	43
Time taken: 0.044 seconds, Fetched: 10 row(s)
*/
hive> ALTER TABLE employees_ptn DROP PARTITION (depid=43);
/*
Dropped the partition depid=43
OKINSERT OVERWRITE TABLE employees_dynamic_ptn
     PARTITION (depid=43,empid)
     SELECT *
     FROM employee;
Time taken: 0.161 seconds
*/

hive> CREATE TABLE IF NOT EXISTS employees_dynamic_ptn(
  EID int,
     ENAME string,
     PTITLE string,
     DID int,
     ESAL float)
    PARTITIONED BY (depid int,empid int,sal float)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


hive> INSERT OVERWRITE TABLE employees_dynamic_ptn
     PARTITION (depid=328,empid,sal)
     SELECT *,EID,ESAL
     FROM employee;

/*... Caused by: org.apache.hadoop.hive.ql.metadata.HiveFatalException: [Error 20004]: Fatal error occurred when node tried to create too many dynamic partitions. The maximum number of dynamic partitions is controlled by hive.exec.max.dynamic.partitions and hive.exec.max.dynamic.partitions.pernode. Maximum was set to: 100...
*/
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=2000;





CREATE EXTERNAL TABLE IF NOT EXISTS employee_external(
     EID int,
     ENAME string,
     PTITLE string,
     DID int,
     ESAL float)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
     LOCATION '/home/hirwuser150430/anubhav/employee-pig/employee_bonus_chicago';


# hadoop migrator compator tool

Creating Hive Table Creation and Insert Scripts Automatically & Comparing Hadoop table data with other RDBMS data table



Objective:
a) Creating Hive tables is really an easy task. But when you really want to migrate from existing RDMS to Hadoop based on the Source RDBMS tables and it’s data types. The Development Scripts Creation and Execution take a huge toll. 
b) Once the tables and data is migrated from the existing RDBMS , we need to validate the data move to Hadoop. Creating a tool that compares the data of the existing source RDBMS with migrated data to Hadoop.

Motivation:
Many project are getting migrated from existing RDBMS to  Hadoop that consists of offloading a data , creating Hive tables similar to the existing in RDBMS and then validation of the data which is migrated from existing RDBMS to Hadoop. 
That involves rewriting all the (many) DDL in to Hive. Queries to compare both the environmental data.
Some criteria of success for such project are:
•	All the exiting tables needs to be created in Hadoop
•	Offloading the data 
•	Validating the data by comparing.
Hence the need of a validation as well as script generating tool.
As, no validation tool existed and we were doing all the validation "manually": looking at the data in the RDBMS table and checking that they were the same in the generated Hive tables. Due to the high number of columns in each table and the number of rows this approach was quite limited.
There is Need of a process/method/tool which is easy to use, allows to compare data across the platforms and comes as cost effective in terms of all perspective. And can handle  
1.	Web Scale data
2.	Efficient algorithms
3.	Small memory footprint
4.	Approximate answers  suffice
5.	Duplicate/change detection 

Need of compare & DDL generator tool is required for one of the Data Testing solution built specifically to automate the migration testing of Data Warehouses & Big Data, ensuring that the data extracted or migrated to target dBs , findings bad data and provides a holistic view of your data differences.

Challenges
a)	Huge volume of Data 
b)	Data types of RDBMS not always same as of Hive table
c)	No current comparison tool for RDMBS and Hadoop data
d)	Problem with datatypes with float, double and decimals . If some columns use numerical values that are not integer (or bigInteger), then you might have a problem because the representation of the float

Brief Description of Tool:
a)	This whole tool have three main parts all built on python 2.7 and PyQT4 for front end.
a.	Comparing Hadoop data with RDBMS data
b.	Creation of Hive table DDL’s from RDBMS tables
c.	Offloading data from the tables
 
b)	This is Hadoop compare tool is one of the Data Testing solution built specifically to automate the migration testing of Data Warehouses & Big Data, ensuring that the data extracted or migrated to target dBs , findings bad data and provides a holistic view of your data differences.
c)	This part of Creation of scripts for the Hive tables.

Current Capabilities:
1. Compare data from two different data server /databases on same or different data servers (including Hadoop , Greenplum , db2 etc.), irrespective of data server engines , can compare the data and  provide a holistic  difference.  Best part is the tool is not restricted to any dataserver engine. It allows user to have the ODBC connector. So any ODBC connections will be consumed to connect to    dataserver and compare. There by making it more flexible to use.
2. Can be utilized on Hadoop Green plum data comparison & validation.
3. Has a flexibility to create DDL’s for the hive from the existing tables for the whole RDBMS data source.
Pre-requisites & Details:
1.	Must have all the RDBMS as well as Hadoop ODBC setup. 
2.	Execute the Python scripts (must have Python 2.7 as well as PyQT4)
When the tool is invoked. User should provide the Username /password. After that we must select the Source Data sources , if is Hadoop or Postgres SQL, check the box next to Source Datasouces.
3.User can select specific columns or whole table.

![Image one](https://github.com/Payel007/hadoopmigratorcompatortool/blob/master/one.png)
![Image two](https://github.com/Payel007/hadoopmigratorcompatortool/blob/master/two.png)

4.Press button Done with source as well as Done with Target respectively when done .So that you can move to Hit Compare for comparing.

![Image three](https://github.com/Payel007/hadoopmigratorcompatortool/blob/master/three.png)
![Image four](https://github.com/Payel007/hadoopmigratorcompatortool/blob/master/four.png)
![Image five](https://github.com/Payel007/hadoopmigratorcompatortool/blob/master/five.png)



5.Once the processing of comparison is done . The Screen display interactively as well as graphically. The details will be like 
a)	Non Matching rows
b)	Matching rows
c)	Total number of rows processed by source 
d)	Total number of rows processed by target 
e)	Allow user to save the difference data .

![Image six](https://github.com/Payel007/hadoopmigratorcompatortool/blob/master/six.png)

6.This is for the Hive-Table Generator . This creates the DDL for all the table or selected by the user . The User should select the data sources . once the data source is selected. User can either go with single selection of the table or the check the box of all tables.

7.Once the All is done , Hit the button Proceed to Hive Creation. This create two files and will be prompted to user. 
1-	Hive DDL 
2-	Hive insert statements

8. This let user know that the scripts are generated and the number of table for which the tool has created the script.

Conclusion:

This tools come quite handy as its uses python. You can customize this tool as per you need as it flexible and easy to install . 


Data Loader and Writer example
========================================================
This demo creates the sstable files and loads them through jmx to a cassandra cluster. It then loads transactions into a file of a certain size. The default is to load 1 million records and create files of 100,000 transactions in each file. 

The files are routed to different files based on the transaction type (Debit/Credit). This could be changed to be source feed or bank provider for example. 

## Running the demo 

To run this code, you need to have your cluster 'cassandra.yaml' and 'log4j-tools.properties' in the 'src/main/resources' directory.

You will need a java 7 runtime along with maven 3 to run this demo. Start DSE 4.0.X or a cassandra 2.0.5 or greater instance on your local machine. This demo just runs as a standalone process on the localhost.

This demo uses quite a lot of memory so it is worth setting the MAVEN_OPTS to run maven with more memory

    export MAVEN_OPTS=-Xmx512M


## Schema Setup
Note : This will drop the keyspace "datastax_bulkload_writer_demo" and create a new one. All existing data will be lost. 

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To run the bulk loader, this defaults to 1 million rows.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.bulkloader.Main" 
    
To run it other settings for no of rows, jmx host and port

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.bulkloader.Main"  -DnoOfRows=5000000 -Djmxhost=cassandra1 -Djmxport=7191    
	
To run the report writer to fill a file with a batchSize of transactions

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.reports.ReportWriterImpl"

The default is 100,000 transactions per file, to change this use the batchSize property

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.reports.ReportWriterImpl" -DbatchSize=15000
		
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
	

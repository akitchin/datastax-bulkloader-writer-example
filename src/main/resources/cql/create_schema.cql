create keyspace IF NOT EXISTS datastax_bulkload_writer_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

use datastax_bulkload_writer_demo; 

create table IF NOT EXISTS transactions (
	accid text,
	txtnid uuid,
	txtntime timestamp,
	amount double,
	type text,
	reason text,
	PRIMARY KEY((accid), txtntime)
);

create table IF NOT EXISTS report_counters (
	rundate text,
	type text,
	filename text,
	rec_num counter,
	PRIMARY KEY((rundate, type), filename)
);

create table IF NOT EXISTS report_locks(
	filename text PRIMARY KEY,
	lock text,
);

truncate transactions;
truncate report_counters;
truncate report_locks;

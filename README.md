# sqoopx project - The sqoop extension project.

Usually, we use the sqoop to export the data in hadoop cluster to relational database, or import relational database data to hadoop cluster.
The cluster data storages contains hdfs file，hive databse or hbase, etc.
Sqoop tools are base on mapreduce. They general would create an orm jar to submit to hadoop cluster.

There are some questions:
1. Can we extend some operation before the export/import job, or some operations after the export/import job? Simply put, can we enhance the export/import tools?
2. Can we use the tools on spark?
3. Can we export to a data source other than relational database? Such as MQ, or Redis.

So, we want to extend it or resolve the three questions.

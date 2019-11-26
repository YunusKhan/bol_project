# bol_project
```bash
.
. |bol   : - folder project 
. |bol |constants.py : contains all constants enumerations  
. |bol |kafka_client.py : kafka client code 
. |bol |kafka_consumer.py : kafka client code 
. |bol |kafka_producer.py : kafka client code 
. |bol |root.py : start root of execution code 
. |config_files : contains kakfa server config for running multiple brokers
. |data_src : contains data sources, csv
. |schema : contains avro schema file in json format 

```

from main calls , important calls are below 

df = stream_whole_data_set() - 
	1. takes whole data set
	2. serialise using dill 
	3. pushes in kafka producer
	4. consume in kafka consumer
	5. insert document in mongo

station_wise = stream_station_wise_data_set(df)
	1. takes whole data set
	2. serialise using pickle 
	3. pushes in kafka producer
	4. consume in kafka consumer
	5. split station wise into different queues per station
	6. push into new station specific kafka producer
	7. insert flat file row wise in hdfs

year_wise = year_wise_data_set(df)
	1. takes whole data set
	2. serialise using avro 
	3. pushes in kafka producer
	4. consume in kafka consumer
	5. split year wise into different queues per year
	6. push into new year wise specific kafka producer
	7. write output as parquet file

year_wise_station_wise = year_wise_station_wise_data_set(df)
# not implemented but similar logic fundamentally 

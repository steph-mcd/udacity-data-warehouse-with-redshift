## Project 3: Data Warehouse

#### The purpose of this redshift database is to provide an optimal data warehousing solution in the cloud. This soluton will allow Sparkify's analytics teams to efficiently and effectively work with their big data. This data was initially stored in an s3 bucket in the form of JSON logs - making it extremely difficult to analyse and query said data. The solution I have provided will allow Sparkify's analytics team to move faster and be able to extract information from their data to help inform the business' decisions.

### Database Schema Design

The schema is made up of 5 dimension tables and 1 fact table. The 5 dimension tables are as below:
1. users - contains information relating to the users in the app
2. songs - contains information relating to the songs in Sparkify's music database
3. artists - contains information relating to the artists in Sparkify's music database
4. time - containes timestamps of records in songplays whcih has then been broken down into specific units of time to create ease for Sparkify's anlaytics team

The 1 fact table is songplays which contains records in event data that are associated with songplays. 

The choice of the above schema has been made to suit the needs adn requriements of Sparkify's analytical teams. 

### ETL Pipeline
The solution I have provided picks up data in the json log format from an s3 bucket and loads it into staging tables inside a Redshift cluster. This data is then transformed adn inserted into the above described tables and once there is ready to be queried by the analytics team. Redshift provides the perfect cloud based data warehouse solution for Sparkify's needs.

### Running the scripts
To set up the redshift datatwarehouse solution a redshift cluster must first be created in AWS and the details relating to that database should be added into the config - including AWS access keys,DB host and IAM role ARN. Once this is done the following steps can be followed to create the tables and isnert data:
1. Run the create_tables.py file by running the following command in the terminal:
```python create_tables.py```
2. After you have succesfully created the tables you need to run the etl.py file by entering the follwoing command in the terminal:
```python etl.py```

You should now have succesfully created the tables and inserted data. You should also eb able to now connect to the redshift cluster and run queries to analyse the data against the newly craeted tables.


# Data Pipelines with Apache Airflow (S3 to Redshift with Airflow)

This is an Apache Airflow ETL data pipelines project that copies datasets from AWS S3, stages them in Redshift, and then transforms the datasets to power analytics teams.

## Project Overview
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring in a data engineer into the project, whi is expected to create high grade data pipelines that are dynamic and built from reusable tasks, which can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

To complete the project, the data engineer will need to create custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

## Project Implementation
The architecture of the ETL data pipelines with airflow is shown below:

![Airflow Data Pipeline](https://user-images.githubusercontent.com/24963911/220166114-a55b0668-859d-4d55-925f-180ad0df1958.png)

The Airflow dags leverage exisitng Airflow operators, e.g., PostgresOperator, and Airflow hooks, e.g., PostgresHook, to facilitate the development of the data pipelines. In addition, the user defined resusable operators as well as the SQL statements modularizes the implemtation of the data pipelines to reduce complexity and promotes reusability of the sofdtware components, which can be reused in other Airflow data pipelines.

Below is an image of the Aiflow UI after a succesful Dag run. Note the create table tasks creates a table if the table doesn't exist.
![Airflow UI](https://user-images.githubusercontent.com/24963911/220166569-a5b01978-5a4f-4732-8b1b-1f9d07e9fb9a.png)


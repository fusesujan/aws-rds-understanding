# Assignment on aws understanding

This repository demonstrates a simple data processing pipeline (ETL) for NBA player data. The pipeline involves fetching raw data from an API, simple cleaning and storing it in raw AWS S3 buckets, retrieving it again from that raw and cleaning and transforming it and finally inserting it into a PostgreSQL database connected with RDS in aws.

1. **Fetching Raw Data from API:** Raw NBA player data is fetched from the API endpoint: https://free-nba.p.rapidapi.com/players. The data includes player information such as names, positions, teams, and physical attributes. some cleaning are done such as renaming the column name, triming the data etc.

2. **Storing Raw Data:** The fetched raw data is stored in an AWS S3 bucket named `apprentice-training-sujan-ml-raw` with s3.put_object(....) functionality.

3. **Retriving raw Data:** Those raw data are retrived from the bucket with s3.get_object(...) functionality.

4. **Cleaning and Transforming Data:** The raw data is retrieved from the S3 bucket, cleaned, and transformed. It's then stored in a new S3 bucket named `apprentice-training-sujan-ml-cleaned`. Unnecessary columns may be dropped, and the data may be rearranged or modified as needed.

5. **Storing Cleaned Data:** The cleaned data is stored in the `apprentice-training-sujan-ml-cleaned` S3 bucket with s3.put_object(....) functionality.

6. **Inserting into PostgreSQL Database:** The cleaned data is further processed to match the schema of a PostgreSQL database table named `Suzan_player_info`. The data is inserted into the database using a psycopg2 connection.

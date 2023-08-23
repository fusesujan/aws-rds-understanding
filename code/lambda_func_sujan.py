import json
import urllib3
import pandas as pd
import boto3
import psycopg2
import logging
import os



logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

def lambda_handler(event, context):
    # TODO implement
    http = urllib3.PoolManager()
    
    # url = "https://free-nba.p.rapidapi.com/players"
    
    # querystring = {"page": "0", "per_page": "100"}
    
    # headers = {
    #     "X-RapidAPI-Key": "3e37d26c72mshdc81f3c9da834b1p1ad847jsn396ec640d849",
    #     "X-RapidAPI-Host": "free-nba.p.rapidapi.com"
    # }
    
    
    # # Make the API request
    # response = http.request('GET', url, headers=headers, fields=querystring)
    
    # if response.status == 200:
        
    #     # Convert the response data to a Python dictionary
    #     data = json.loads(response.data.decode('utf-8'))
        
    #     # Extract the 'data' list from the API response
    #     new_data = data['data']

    #     required_data = []
    #     for player in new_data:
    #         required_data.append({
    #             'player_id': player['id'],
    #             'first_name': player['first_name'],
    #             'last_name': player['last_name'],
    #             'position': player['position'],
    #             'team': player['team']['full_name'] if 'team' in player and 'full_name' in player['team'] else None,
    #             'city': player['team']['city'] if 'team' in player and 'city' in player['team'] else None,
    #             'height_feet': player['height_feet'],
    #             'height_inches': player['height_inches'],
    #             'weight_pounds': player['weight_pounds']
    #         })
        
    #     # converting it to dataframe
    #     df = pd.DataFrame(required_data)
    #     # selecting first 20 rows of data, 
    #     smalldf= df.head(20)
    #     # sorting data according to id
    #     sorted_df = smalldf.sort_values(by="player_id")
        
    #     # converting it back to json 
    #     final_json_data = sorted_df.to_json(orient="records")

    #     s3.put_object(Body=final_json_data, Bucket='apprentice-training-sujan-ml-raw',Key='new_raw.json')

    #     return {
    #             'statusCode': 200,
    #                 'body': json.dumps('Data fetched, transformed, and stored successfully!')
    #             }
                    
    # else:
    #     logger.error(f"Error: HTTP status code {response.status}")
        
    #     return {
    #         'statusCode': 500,
    #         'body': json.dumps('Error fetching data from the API')
    #     }        
        
       

    # Get clean json from source bucket
    response = s3.get_object(Bucket='apprentice-training-sujan-ml-raw', Key='new_raw.json')
    json_content = response['Body'].read()
    
    data = json.loads(json_content.decode('utf-8'))
    sorted_df = pd.DataFrame(data)
    # reseting index
    sorted_df.reset_index()
    #merging the first and last name
    sorted_df["full_name"] = sorted_df["first_name"] + " " + sorted_df["last_name"]
    
    # # deleting first and last name
    modified_df = sorted_df.drop(columns=['first_name','last_name'])
    # #reordering the data column
    reordered_columns = ['player_id', 'full_name', 'position', 'team', 'city', 'height_feet', 'height_inches', 'weight_pounds']
    sorted_data = modified_df[reordered_columns]
    print(sorted_data)
    #deleting some rows whose  most of the values are null
    sorted_data = sorted_data.drop(columns=['height_feet','height_inches','weight_pounds'])

    # # converting it back to json 
    final_json_data = sorted_data.to_json(orient="records")
    
    #putting this clean data to the clean bucket
    # s3.put_object(Body=final_json_data, Bucket='apprentice-training-sujan-ml-cleaned',Key='new_cleaned.json')
    
    # RDS
    try:
        conn = psycopg2.connect(
            host = os.environ['HOST'],
            database = os.environ['DBNAME'],
            user = os.environ['USER'],
            password = os.environ['PASSWORD']
            )
        print('Connecting to database........Connected')
    except Exception as e:
        print("Connection Failed")
        print(e)
    
    cursor = conn.cursor()
    
    print(sorted_data.info())

    data_to_insert = [tuple(row) for row in sorted_data.values]
    insert_query = f"""
    INSERT INTO Suzan_player_info
    (player_id,full_name,position,team,city)
    VALUES (%s, %s, %s, %s, %s)
    
    """
    
    
    cursor.executemany(insert_query, data_to_insert)

    # Commit the transaction
    conn.commit()

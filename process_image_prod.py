# Now, you can import the Python file from the directory
from function_lib import get_char_list,read_image,get_win_lose,get_df,delete_data,insert_data,preprocess_image
from function_lib import job_monitoring
import os 
import json
import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery.client import Client
import pandas as pd
import pandas_gbq
import os
import shutil
import io
import time
import traceback
from google.cloud import storage



def move_blob(bucket, blob_name, new_location):
    new_blob = bucket.copy_blob(
        bucket.blob(blob_name), bucket, new_location
    )
    bucket.delete_blob(blob_name)
    return new_blob


def process_image():
    read_image_counter = 0  # Initialize the counter at the beginning of your script
    stop_processing = False  # Flag to indicate when to stop processing

    #initialize storage bucket to read images from
    storage_client = storage.Client()
    bucket_name = "discord_bot_pvp"
    bucket = storage_client.bucket(bucket_name)


    # Define the "parent directory" as the root folder of discord_bot_pvp in GCS
    parent_directory = ""
    job_path = r'C:\Anaconda3\Projects\discord_bot\Python_Scripts\main.py'

    # Define subdirectory names for ATK and DEF
    atk_subdirectory = 'Fileholder_atk'
    def_subdirectory = 'Fileholder_def'

    try:

        # Iterate over subdirectories (ATK and DEF)
        # for subdirectory, is_attack in [(atk_subdirectory, True), (def_subdirectory, False)]:
        for subdirectory, is_attack in [(atk_subdirectory, True)]:
            
            # Adjust directory paths to use forward slashes and exclude bucket name
            directory_path = subdirectory
            processed_directory = f'Processed_{"atk" if is_attack else "def"}'
            fail_directory = f'Fail_{"atk" if is_attack else "def"}'
            
            current_datetime = datetime.datetime.now()
            current_date = current_datetime.strftime("%Y%m%d")
            current_time = current_datetime.strftime("%H%M")
            
            new_processed_directory = f"{processed_directory}/{current_date}_{current_time}"
            new_fail_directory = f"{fail_directory}/{current_date}_{current_time}"
            
            #reading image_mapping.json from gcs
            json_blob_name = f"{atk_subdirectory}/image_mapping.json"
            json_blob = bucket.blob(json_blob_name)
            
            json_data = json_blob.download_as_text()
            image_mapping = json.loads(json_data)
            
        
            # Initialize empty lists to store column values
            message_author = []
            filename = []
            message_timestamp = []
        
            # Iterate through the image mapping dictionary
            for key, value in image_mapping.items():
                filename.append(key)
                message_author.append(value[1])
        
                # Parse the timestamp string to a datetime object
                timestamp_str = value[0]
                message_timestamp.append(datetime.datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S'))
                
            # Define the column names for ATK and DEF
            atk_columns = [f'ATK_{i}' for i in range(1, 7)]
            def_columns = [f'DEF_{i}' for i in range(1, 7)]
        
            # Define the column order based on is_attack
            column_order = atk_columns + def_columns + ['WIN_LOSE'] if is_attack else def_columns + atk_columns + ['WIN_LOSE']
        
            # Create an empty main_df with all columns in the specified order
            info_df = pd.DataFrame(columns=[
                *column_order
            ])
            
            # Create a DataFrame for message_author, filename, and message_timestamp
            main_df = pd.DataFrame({
                'MESSAGE_AUTHOR': message_author,
                'FILENAME': filename,
                'MESSAGE_TIMESTAMP': message_timestamp,
            })
        
            # Concatenate info_df with main_df
            main_df = pd.concat([main_df,info_df], axis=1)
        
            name_list = get_char_list()
            
            #local read
            # Iterate over all files in the directory
            # for filename in os.listdir(directory_path):
                
            prefix_path = f"{subdirectory}/"
            
            for blob in bucket.list_blobs(prefix=prefix_path):
                
                # Make sure it's an image and not the JSON mapping file
                if blob.name.endswith(('.jpg', '.png')) and blob.name != json_blob_name:
                    filename = os.path.basename(blob.name)
                    # Later, when you're handling the blobs
                    source_path = f"{directory_path}/{filename}"
                    destination_path = f"{new_processed_directory}/{filename}"
                    fail_path = f"{new_fail_directory}/{filename}"
                    
                    
                    if stop_processing:  # remove all other images from main_df
                        file_base = os.path.splitext(filename)[0]
                        match_index = main_df[main_df['FILENAME'] == file_base].index
                        if not match_index.empty:
                            main_df = main_df.drop(index=match_index)
                        break  # Break out of the inner loop
        
                    try:
                        print(f'processing {filename}')
                        # Construct the full image file path
                        image_bytes = io.BytesIO(blob.download_as_bytes())
                        
                        scales_to_try = [4, 4.5, 5, 5.5, 3.5, 6]
                        success = False
                        
                        
                        for scale in scales_to_try:
                            if stop_processing:  # Check if we need to stop processing scales
                                break  # Break out of the inner loop
                            
                            print(f'trying scale: {scale}')
                            # Call the read_image function to process the image
                            processed_img = preprocess_image(image_bytes, scale)  # enhance and make b/w
                        
                            # Create an in-memory binary stream to hold the processed image
                            image_stream = io.BytesIO()
                            processed_img.save(image_stream, format='PNG')
                            image_stream.seek(0)
                            
                            #see if image extraction is successful, if not try diff scales
                            try:
                        
                                if read_image_counter >= 180:
                                    print('read_image function was called 180 times. Stopping further image processing.')
                                    stop_processing = True  # Set the flag to true
                                    break  # Break out of the inner loop
                
                                parsed_response = read_image(image_stream)
                                read_image_counter += 1  # Increment the counter each time read_image is called

                                pos_df, success,fail_list = get_df(parsed_response, is_attack, name_list)
                                
                                if not fail_list:
                                    print(f"pos_df:{pos_df}\n")
                                    print(f"success:{success}\n")
                                    print(f"fail_list:{fail_list}\n")
            
                            
                                if success:
                                    break
                            except:
                                continue 
                            
        
                            
                        
                        # Check if all attempts failed
                        if not success:
                            job_monitoring('IMAGE_CHECK','Fail',job_path,'Please Check: '+str(fail_path),', '.join(fail_list),'')
                        if not parsed_response:
                            job_monitoring('IMAGE_CHECK','Fail',job_path,'Parsed response empty: '+str(fail_path),', '.join(fail_list),'')
                            continue
                        
                        win_lose = get_win_lose(parsed_response)
                        
                        # Extract the filename without extension (e.g., 'image_1')
                        file_base = os.path.splitext(filename)[0]
        
                        # Find the index of the matching filename in main_df['FILENAME']
                        match_index = main_df[main_df['FILENAME'] == file_base].index
        
                        # If a match is found, append the pos_df horizontally to main_df
                        if not match_index.empty:
                            match_index = match_index[0]  # Get the first index if multiple matches
                            main_df.loc[match_index, pos_df.columns] = pos_df.values[0]
                            main_df.loc[match_index, 'WIN_LOSE'] = win_lose
                            
                        if success:
                            move_blob(bucket, source_path, destination_path)
                        else:
                            move_blob(bucket, source_path, fail_path)
        
                    except Exception as e:
                        # Later, when you're handling the blobs
                        
                        # Handle the exception (e.g., log or print error message)
                        print(f"Error processing file {filename}: {str(e)}\n{traceback.format_exc()}")
                        job_monitoring('IMAGE_CHECK','Fail',job_path,str(e)[:1000],str(traceback.format_exc()[:1000]),fail_path)
                        
                        # Extract the filename without extension (e.g., 'image_1')
                        file_base = os.path.splitext(filename)[0]
            
                        # Find the index of the matching filename in main_df['FILENAME']
                        match_index = main_df[main_df['FILENAME'] == file_base].index
            
                        # If a match was found and an exception occurred, remove the record from main_df
                        if not match_index.empty:
                            main_df = main_df.drop(index=match_index)
                            
                        # Move the file to the processed directory
                        move_blob(bucket, source_path, fail_path)
        
            # Display main_df
            main_df = main_df[['MESSAGE_AUTHOR', 'FILENAME', 'MESSAGE_TIMESTAMP', 'WIN_LOSE', 'ATK_1', 'ATK_2',
                'ATK_3', 'ATK_4', 'ATK_5', 'ATK_6', 'DEF_1', 'DEF_2', 'DEF_3', 'DEF_4',
                'DEF_5', 'DEF_6']]
            main_df['INSERT_TS'] = datetime.datetime.now()
            
            # List of columns to check for empty values
            columns_to_check = ['WIN_LOSE', 'ATK_1', 'ATK_2', 'ATK_3', 'ATK_4', 'ATK_5', 'ATK_6', 
                                'DEF_1', 'DEF_2', 'DEF_3', 'DEF_4', 'DEF_5', 'DEF_6']
            
            # Drop the rows where any of the specified columns have empty values
            main_df = main_df.dropna(subset=columns_to_check, how='any')

            
            print(main_df)
        
        
        #deleting and ingesting section
        project_id = 'single-ripsaw-400606'
        schema_id = 'PVPDATASET'
        table_id = 'PVP_OUTDOOR_S2_ATK'
        table_path = f'{project_id}.{schema_id}.{table_id}'
        
        # Set the table_id in the format 'project_id.dataset_id.table_id'
        delete_data(days=1)
        insert_data(main_df,table_path,project_id)
        
        job_monitoring('PVPDATASET_MAIN','Success',job_path,'','','')
        
    except Exception as e:
        job_monitoring('PVPDATASET_MAIN','Fail',job_path,str(e)[:1000],str(traceback.format_exc()[:1000]),'')


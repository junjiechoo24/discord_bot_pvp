from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery.client import Client
import pandas as pd
import pandas_gbq
import os

def job_monitoring(job_name,job_status,job_path,error,error2,error3):
    import pandas_gbq
    import pandas as pd
    from datetime import datetime
    
    project_id = 'single-ripsaw-400606'
    schema_id = 'PVPDATASET'
    table_id = 'JOB_MONITORING'
    table_path = f'{project_id}.{schema_id}.{table_id}'

    # Define the data as per your inputs
    data = {
        "JOB_NAME": job_name,
        "JOB_STATUS": job_status,
        "JOB_PATH": job_path,
        "ERROR": error,
        "ERROR2": error2,
        "ERROR3": error3,
        "INSERT_TS": [datetime.now()]  # Use the current timestamp as an example
    }
    
    # Create a DataFrame with the provided data
    df = pd.DataFrame(data)
    
    pandas_gbq.to_gbq(df, table_path, project_id=project_id, if_exists='append')
    print(f"Error occured: {job_name}. {error}. {error2}. {error3}")

def preprocess_image(image_path,scale):
    import requests
    import io
    from PIL import Image
    
    img = Image.open(image_path)
    width, height = img.size
    new_size = int(width*scale), int(height*scale)
    img = img.resize(new_size, Image.LANCZOS)
    img = img.convert('L')
    img = img.point(lambda x: 0 if x < 200 else 255, '1')
    # img.save(image_path)
    return img

def delete_data(days=1):
    print(f"deleting data num days: {days}")
    # Create a BigQuery client
    client = bigquery.Client()

    # Define your SQL query
    sql_query = f"""
    DELETE FROM `single-ripsaw-400606.PVPDATASET.PVP_OUTDOOR_S2_ATK`
    WHERE TIMESTAMP_TRUNC(MESSAGE_TIMESTAMP, DAY) >= TIMESTAMP_TRUNC(TIMESTAMP_SUB(TIMESTAMP_ADD(CAST(CURRENT_DATETIME() AS TIMESTAMP), INTERVAL 8 HOUR), INTERVAL {days} DAY),DAY)
    """

    # Run the query
    query_job = client.query(sql_query)

    # Wait for the query job to complete
    query_job.result()

    print("Delete executed successfully.")
    
def insert_data(main_df,table_path,project_id):
    # project_id = 'single-ripsaw-400606'
    # schema_id = 'PVPDATASET'
    # table_id = 'PVP_OUTDOOR_S2_ATK'
    # table_path = f'{project_id}.{schema_id}.{table_id}'
    # Set the table_id in the format 'project_id.dataset_id.table_id'
    
    pandas_gbq.to_gbq(main_df, table_path, project_id=project_id, if_exists='append')
    print("Data loaded into BigQuery table.")

def insert_data2(main_df,table_path):
    # project_id = 'single-ripsaw-400606'
    # schema_id = 'PVPDATASET'
    # table_id = 'PVP_OUTDOOR_S2_ATK'
    # table_path = f'{project_id}.{schema_id}.{table_id}'
    # Set the table_id in the format 'project_id.dataset_id.table_id'
    
    client = bigquery.Client()
    # Load the DataFrame into the BigQuery table
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("MESSAGE_AUTHOR", "STRING"),
            bigquery.SchemaField("FILENAME", "STRING"),
            bigquery.SchemaField("MESSAGE_TIMESTAMP", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("WIN_LOSE", "STRING"),
            bigquery.SchemaField("ATK_1", "STRING"),
            bigquery.SchemaField("ATK_2", "STRING"),
            bigquery.SchemaField("ATK_3", "STRING"),
            bigquery.SchemaField("ATK_4", "STRING"),
            bigquery.SchemaField("ATK_5", "STRING"),
            bigquery.SchemaField("ATK_6", "STRING"),
            bigquery.SchemaField("DEF_1", "STRING"),
            bigquery.SchemaField("DEF_2", "STRING"),
            bigquery.SchemaField("DEF_3", "STRING"),
            bigquery.SchemaField("DEF_4", "STRING"),
            bigquery.SchemaField("DEF_5", "STRING"),
            bigquery.SchemaField("DEF_6", "STRING"),
            bigquery.SchemaField("INSERT_TS", "TIMESTAMP", mode="REQUIRED")
        ],
        write_disposition="WRITE_APPEND",  # Change to WRITE_TRUNCATE if needed
    )

    # Load the DataFrame into the BigQuery table
    job = client.load_table_from_dataframe(main_df, table_path, job_config=job_config)
    job.result()  # Wait for the job to complete


def create_dataset():
    import os
    from google.cloud import bigquery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Anaconda3\Projects\discord_bot\keys\discord_bot.json'

    
    project_id = 'single-ripsaw-400606'
    schema_id = 'PVPDATASET'
    # table_id = 'PVP_OUTDOOR_S2_ATK'
    # table_path = f'{project_id}.{schema_id}.{table_id}'
    # Set the table_id in the format 'project_id.dataset_id.table_id'
    
    dataset_path = f"{project_id}.{schema_id}"
    dataset = bigquery.Dataset(dataset_path)
    dataset.location = "US"

    client = bigquery.Client()
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


    #checking dataset or schema available

    # Construct a BigQuery client object.
    client = bigquery.Client()

    datasets = list(client.list_datasets())  # Make an API request.
    project = client.project

    if datasets:
        print("Datasets in project {}:".format(project))
        for dataset in datasets:
            print("\t{}".format(dataset.dataset_id))
    else:
        print("{} project does not contain any datasets.".format(project))
        
        
def create_table():
    
    project_id = 'single-ripsaw-400606'
    schema_id = 'PVPDATASET'
    table_id = 'PVP_OUTDOOR_S2_ATK'
    table_path = f'{project_id}.{schema_id}.{table_id}'
    
    # Set the table_id in the format 'project_id.dataset_id.table_id'
    from google.cloud import bigquery
    import os
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Anaconda3\Projects\discord_bot\keys\discord_bot.json'
    
    client = bigquery.Client()
    schema = [
        bigquery.SchemaField("MESSAGE_AUTHOR", "STRING"),
        bigquery.SchemaField("FILENAME", "STRING"),
        bigquery.SchemaField("MESSAGE_TIMESTAMP", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("WIN_LOSE", "STRING"),
        bigquery.SchemaField("ATK_1", "STRING"),
        bigquery.SchemaField("ATK_2", "STRING"),
        bigquery.SchemaField("ATK_3", "STRING"),
        bigquery.SchemaField("ATK_4", "STRING"),
        bigquery.SchemaField("ATK_5", "STRING"),
        bigquery.SchemaField("ATK_6", "STRING"),
        bigquery.SchemaField("DEF_1", "STRING"),
        bigquery.SchemaField("DEF_2", "STRING"),
        bigquery.SchemaField("DEF_3", "STRING"),
        bigquery.SchemaField("DEF_4", "STRING"),
        bigquery.SchemaField("DEF_5", "STRING"),
        bigquery.SchemaField("DEF_6", "STRING"),
        bigquery.SchemaField("INSERT_TS", "TIMESTAMP", mode="REQUIRED")
    ]

    # Create the table with the defined schema
    table = bigquery.Table(table_path, schema=schema)
    table = client.create_table(table)  # Make an API request.

    print("Created table")


def get_char_list():
    #getting name list
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd

    # Define the URL of the website you want to scrape
    url = 'https://bluearchive.wiki/wiki/Characters'

    # Send an HTTP GET request to the URL
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the page using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the table you want to scrape (assuming it's the first table on the page)
        table = soup.find('table')

        # Initialize empty lists to store the data
        headers = []
        data = []

        # Extract the table headers
        for th in table.find_all('th'):
            headers.append(th.text.strip())

        # Extract the table data rows
        for row in table.find_all('tr')[1:]:  # Skip the header row
            row_data = [td.text.strip() for td in row.find_all('td')]
            data.append(row_data)

        # Create a DataFrame from the data
        df = pd.DataFrame(data, columns=headers)
        df['Name'] = df['Name'].str.replace(r'(Bunny Girl', r'(Bunny',regex=False)
        df['Name'] = df['Name'].str.replace(r'(Sportswear', r'(Track',regex=False)
        df['Name'] = df['Name'].str.replace(r'(Riding', r'(Cycling',regex=False)
        df['Name'] = df['Name'].str.replace(r'(Cheerleader', r'(Cheer Squad',regex=False)
        df['Name'] = df['Name'].str.replace(r'(Kid', r'(Small',regex=False)
        df['Name'] = df['Name'].str.replace(r'(Bunny Girl', r'(Bunny',regex=False)

        # Print the DataFrame
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")

    name_list = df['Name']
    return name_list

def read_image(image_stream):
    import requests

    # Replace 'YOUR_API_KEY' with your actual OCR.space API key
    api_key = 'K85178701188957'

    # URL of the OCR.space API endpoint
    api_url = 'https://api.ocr.space/parse/image'

    # Path to the image you want to extract text from
    #image_path = r'C:\Anaconda3\Projects\discord_bot\image.png'

    # Parameters for the OCR request
    payload = {
        'apikey': api_key,
        'language': 'eng',  # Language code for English
        'istable':'True',
        'isOverlayRequired': 'True',
        'detectOrientation': 'True',
        'scale': 'True',
        'OCREngine': 3
    }

    # Read the image as a binary file
    # with open(image_path, 'rb') as image_file:
    #     # Include the image file in the request
    #     response = requests.post(api_url, files={'image': image_file}, data=payload)
    
    response = requests.post(api_url, files={'image': ('image.png', image_stream)}, data=payload)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        result = response.json()
        # Extract and print the recognized text
        if 'ParsedResults' in result and len(result['ParsedResults']) > 0:
            parsed_text = result['ParsedResults'][0]['ParsedText']
#             print('Recognized Text:')
    #         print(parsed_text)
        else:
            print('No text was recognized in the image.')
            return {}
            
    else:
        print(f'Error {response.status_code}: {response.text}')

    import json

    # Your JSON response as a string
    # response_text = response.text
    # parsed_response = json.loads(response_text)
    # formatted_response = json.dumps(parsed_response, indent=4)
    # print(formatted_response)

    # Access and use the coordinates for all words
    parsed_response = json.loads(response.text)
#     for line in parsed_response["ParsedResults"][0]["TextOverlay"]["Lines"]:
#         for word in line["Words"]:
#             word_text = word["WordText"]
#             left = word["Left"]
#             top = word["Top"]
#             width = word["Width"]
#             height = word["Height"]

#             print("Word:", word_text)
#             print("Left:", left)
#             print("Top:", top)
#             print("Width:", width)
#             print("Height:", height)
#             print("\n")

    return parsed_response

def get_win_lose(parsed_response):
    # Initialize lists to store words containing "win/wd" and "lose"
    win_words = []
    lose_words = []
    

    # Iterate through the lines and words in parsed_response
    for line in parsed_response["ParsedResults"][0]["TextOverlay"]["Lines"]:
        for word in line["Words"]:
            word_text = word["WordText"]
            height = word["Height"]
            left = word["Left"]

            # List of words to compare
            words_to_compare = ["win", "wd", "wyo", "wfo"]
            
            # Convert word_text to lowercase
            word_text_lower = word_text.lower()
            
            # Check if any of the words in the list exist in the lowercase word_text
            if any(word in word_text_lower for word in words_to_compare) or (word_text.startswith('W') and ((len(word_text) == 3) or (len(word_text) == 2) or (len(word_text) == 4) ) ):
                word['WordText'] = 'Win'
                win_words.append(word)

            # Check if the word contains "lose"
            if "los" in word_text.lower():
                word['WordText'] = 'Lose'
                lose_words.append(word)

    # Sort win_words and lose_words by height in descending order
    win_words.sort(key=lambda x: x["Height"], reverse=True)
    lose_words.sort(key=lambda x: x["Height"], reverse=True)

    # Select the two tallest words from both win_words and lose_words
    tallest_words = win_words[:2] + lose_words[:2]

    # Find the word with the lowest "left" variable among the selected words
    if tallest_words:
        lowest_left_word = min(tallest_words, key=lambda x: x["Left"])
        lowest_left_word_text = lowest_left_word["WordText"]

    else:
        lowest_left_word_text = None

    # Print the result
    #print("Word with lowest 'left' variable among the two tallest words:", lowest_left_word_text)
    return lowest_left_word_text

def get_df(parsed_response,is_attack,name_list):
        
    import pandas as pd
    
    if not parsed_response:
        return pd.DataFrame(),False,[]
    
    #getting Min top
    # Initialize a list to store "top" values for matching words
    top_values = []
    
    # Iterate through the lines and words in parsed_response
    for line in parsed_response["ParsedResults"][0]["TextOverlay"]["Lines"]:
        for word in line["Words"]:
            word_text = word["WordText"]
    
            # Check if the word matches any item in name_list
            if word_text in name_list.tolist():
                top_values.append(word["Top"])
    
    # Calculate the min "top" value from the extracted values
    if top_values:
        max_top = max(top_values)
    else:
        max_top = 0  # Handle the case where no matches were found
    
    # Now, average_top contains the average "top" value for matching words
    #print("Min top:", min_top)
    
    # Initialize an empty DataFrame
    df = pd.DataFrame()
    
    # Define a tolerance value for comparing "left" values
    tolerance = 125
    
    # Iterate through the words in the JSON
    for line in parsed_response["ParsedResults"][0]["TextOverlay"]["Lines"]:
        for word in line["Words"]:
            word_text = word["WordText"]
            left = word["Left"]
            top = word["Top"]
            
            if word_text.isdigit():
                continue
    
            # Check if the word is above 800 on the y-axis
            if max_top - tolerance <= top <= max_top + tolerance:
                # Check if a similar "left" value already exists in the DataFrame
                found = False
                for column in df.columns:
                    if abs(float(column) - left) <= tolerance:
                        # Concatenate the word with the existing cell
                        if '(Bun' in word_text:
                            word_text = '(Bunny)'
                        if '(Ma' in word_text:
                            word_text = '(Maid)'
                        if '(Swim' in word_text:
                            word_text = '(Swimsuit)'
                        if '(Ne' in word_text:
                            word_text = '(New Year)'
                            
                        df[column] += ' ' + word_text
                        found = True
                        break
    
                # If no similar "left" value is found, create a new column
                if not found:
                    df[str(left)] = [word_text]
    
    # Initialize column names
    column_names = []
    
    # Define prefixes based on is_attack
    first_prefix = "ATK_" if is_attack else "DEF_"
    second_prefix = "DEF_" if is_attack else "ATK_"
    
    # Create column names based on prefixes
    for i in range(1, 7):
        column_names.append(f'{first_prefix}{i}')
    for i in range(1, 7):    
        column_names.append(f'{second_prefix}{i}')
    
    # Set column names for the DataFrame
    df = df.iloc[:,:12]
    df.columns = column_names
    
    # Assuming you have a DataFrame 'df' with only one row




    
    #compare to name list values
    row_values = df.iloc[0]  
    
    fail_list = []
    for value in row_values:
        if value not in name_list.values:
            fail_list.append(value)

    # If fail_list is not empty, it means some elements did not match
    if fail_list:
        success = False
    else:
        success = True
        
        #making sure they are correct order
        if df['ATK_6'].iloc[0].lower() < df['ATK_5'].iloc[0].lower():
            df['ATK_5'], df['ATK_6'] = df['ATK_6'], df['ATK_5']
            
        if df['DEF_6'].iloc[0].lower() < df['DEF_5'].iloc[0].lower():
            df['DEF_5'], df['DEF_6'] = df['DEF_6'], df['DEF_5']
    
    return df,success,fail_list

def plot_boxes():
    import matplotlib.pyplot as plt

    # Create a 2D plane (figure)
    fig, ax = plt.subplots()

    # Set axis limits (adjust these as needed)
    ax.set_xlim(0, 2000)  # Adjust the X-axis limit as needed
    ax.set_ylim(0, 1000)  # Adjust the Y-axis limit as needed

    for line in parsed_response["ParsedResults"][0]["TextOverlay"]["Lines"]:
        for word in line["Words"]:
            word_text = word["WordText"]
            left = word["Left"]
            top = word["Top"]
            width = word["Width"]
            height = word["Height"]

            # Define the rectangle using the coordinates
            rect = plt.Rectangle((left, top), width, height, fill=False, edgecolor='r')

            # Add the rectangle to the plane
            ax.add_patch(rect)

            # Label the rectangle with word_text
            ax.annotate(word_text, (left, top), fontsize=8, color='r')

    # Invert the Y-axis to match typical Cartesian coordinates
    plt.gca().invert_yaxis()

    import os
    # Specify the save path and filename
    save_path = r'C:\Anaconda3\Projects\discord_bot'
    save_filename = 'output_image.png'

    # Save the plot as an image
    plt.savefig(os.path.join(save_path, save_filename))

    # Show the plane with all rectangles and labels
    plt.show()

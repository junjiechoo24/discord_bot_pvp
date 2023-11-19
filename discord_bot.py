import sys

# Add the directory to the system path
script_path = r'C:\Anaconda3\Projects\param'
sys.path.insert(0, script_path)

import discord
import datetime
import os
import json
import re
from google.cloud import storage
import param

#test test test

# Set the path to the service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Anaconda3\Projects\discord_bot\keys\discord_bot.json"

API_TOKEN = param.discord_bot_key
intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

win_channel_id = 1157509822573457501
lose_channel_id = 1157509920099422218

@client.event
async def on_ready():
    print(f'Logged in as {client.user.name}')
    try:
        await scan_channel_for_images(win_channel_id)
        await scan_channel_for_images(lose_channel_id)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        await logout_bot()
    finally:
        await logout_bot()

@client.event
async def on_message(message):
    # This event handler is not used for scanning messages.
    pass

async def scan_channel_for_images(channel_id,scan_days=1):
    # Get the channel object
    channel = client.get_channel(channel_id)

    # Calculate the start and end datetime for the past 3 days
    today = datetime.datetime.today()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    three_days_ago = today - datetime.timedelta(days=scan_days)
    print(f"scanning after: {three_days_ago}")
    print(f"scan_days: {scan_days}")

    # Create a directory to store the images and mapping JSON
    folder_name = "Fileholder_atk" if channel_id == win_channel_id else "Fileholder_def"
    folder_path = os.path.join("C:\\Anaconda3\\Projects\\discord_bot", folder_name)
    os.makedirs(folder_path, exist_ok=True)

    # Create a mapping dictionary to store image index and author
    image_mapping = {}

    # Initialize a counter for the image index
    index = 1
    
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    
    # Define your bucket name
    bucket_name = "discord_bot_pvp"
    bucket = storage_client.bucket(bucket_name)

    # Fetch messages from the past 3 days
    async for message in channel.history(after=three_days_ago,limit=200):
        for attachment in message.attachments:
            if attachment.content_type.startswith('image/png') or attachment.content_type.startswith('image/jpeg') or attachment.content_type.startswith('image/jpg'):
            # if attachment.content_type.startswith('image') and str(message.author) == 'hasad':
                print(f"image_{index}.png, {attachment.content_type}, {message.created_at + datetime.timedelta(hours=8)}, {str(message.author)}")
                image_bytes = await attachment.read()

                # Add 8 hours to the timestamp
                message_timestamp = message.created_at + datetime.timedelta(hours=8)

                # Format the timestamp to a string
                timestamp_str = message_timestamp.strftime("%Y%m%d_%H%M%S")

                # Construct the image filename
                image_filename = f'image_{index}.png'


                ########local save
                # # Save the image to the folder
                # image_path = os.path.join(folder_path, image_filename)
                # with open(image_path, 'wb') as file:
                #     file.write(image_bytes)
                
                # Construct the GCS blob (object) name
                blob_name = f"{folder_name}/image_{index}.png"
                blob = bucket.blob(blob_name)
                blob.upload_from_string(image_bytes)

                # Add the mapping entry to the dictionary
                image_mapping[f'image_{index}'] = [timestamp_str, str(message.author)]

                # Increment the index
                index += 1
    
    ######local save
    # # Save the mapping dictionary as a JSON file
    # mapping_file_path = os.path.join(folder_path, 'image_mapping.json')
    # with open(mapping_file_path, 'w') as json_file:
    #     json.dump(image_mapping, json_file)
    
    # Upload the JSON mapping to GCS
    blob_name = f"{folder_name}/image_mapping.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(image_mapping))

async def logout_bot():
    print(f'Logging out as {client.user.name}')
    await client.close()

if __name__ == "__main__":
    client.run(API_TOKEN)

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
from io import BytesIO
from PIL import Image
import pytesseract


# Set the path to the service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Anaconda3\Projects\discord_bot\keys\discord_bot.json"

API_TOKEN = param.discord_bot_key
intents = discord.Intents.default()
intents.message_content = True  # Enable the intent to receive message content

client = discord.Client(intents=intents)

# Define the directories to save images
win_dir = 'C:/Anaconda3/Projects/discord_bot/Fileholder_win'
lose_dir = 'C:/Anaconda3/Projects/discord_bot/Fileholder_lose'

win_channel_id = 1157509822573457501
lose_channel_id = 1157509920099422218

@client.event
async def on_message(message):
    if message.channel.id == win_channel_id or message.channel.id == lose_channel_id:
        message_content = message.content
        message_author = message.author

        # Process text messages
        if message_content:
            print(f'New message -> {message_author} said: {message_content}')

        # Process image attachments
        for attachment in message.attachments:
            if attachment.content_type.startswith('image'):
                # Check if the image was sent today
                today = datetime.date.today()
                sent_date = message.created_at.date()
                if sent_date == today:
                    image_bytes = await attachment.read()
                    image = Image.open(BytesIO(image_bytes))

                    # Print the filename of the image
                    print(f'Image from {message_author} - Filename: {attachment.filename}')

                    # Define the save directory based on the channel
                    if message.channel.id == win_channel_id:
                        save_dir = win_dir
                    else:
                        save_dir = lose_dir

                    # Ensure the save directory exists
                    os.makedirs(save_dir, exist_ok=True)

                    # Save the image to the directory
                    image.save(os.path.join(save_dir, attachment.filename))

if __name__ == "__main__":
    client.run(API_TOKEN)

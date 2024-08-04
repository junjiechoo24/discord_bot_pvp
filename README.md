# GCP Project to analyse PvP match results for Blue Archive

## Project Overview

The primary goal of this project is to gather win/lose results of PvP matches in the Blue Archive game that were posted onto Discord. The aim is to analyse winning and losing compositions to find the best PvP composition for every situation in the game.

The Discord bot will extract image data from specified Discord channels daily, process this data using Optical Character Recognition (OCR), and store the extracted text in Google BigQuery. Deployed on Google Cloud Platform (GCP) using Docker, the project also incorporates Google Cloud Scheduler to trigger the bot via Google Pub/Sub. This setup was used to experiment with these features despite their overkill for this use case. Google Cloud Functions could also have been used instead of Docker, but the project aimed to explore Docker's capabilities as well, despite it being overkill again.

The OCR process can often be improved with better preprocessing steps to enhance accuracy and reliability. The project details are as follows:

### Trigger Mechanism
- **Google Cloud Scheduler**: Used to schedule tasks and trigger a message to Google Pub/Sub at specified intervals.
- **Google Pub/Sub**: Acts as a messaging service to pass the trigger message to the Docker container.
- **Docker Container**: Deployed on GCP, the container is triggered by messages from Pub/Sub to execute the image processing and data extraction tasks.

### Daily Batch Processing
- The bot scans and processes image attachments from specified Discord channels on a daily basis, ensuring that recent data is always captured and ready for analysis.

### Image Processing
- **Preprocessing**: Images are resized, converted to grayscale, and thresholded to enhance OCR performance. This preprocessing step improves the clarity of the images and makes the text extraction more accurate.
- **OCR Extraction**: The preprocessed images are then analysed using an OCR API to extract text data, which is essential for tracking and analysing game-related metrics.

### Data Storage
- **Google Cloud Storage**: Processed images and their associated metadata are uploaded to Google Cloud Storage.
- **Google BigQuery**: The extracted text data is then integrated into Google BigQuery for structured analysis and reporting.

### Monitoring
- The bot logs and monitors processing jobs, tracking the success or failure of each job. Occasionally, image processing might fail, which is why it's important to track errors.

## Possible Future Experimentations

- **Real-Time Processing**: To enhance the system further, future experiments could involve using Google Pub/Sub to process and extract data from images in real-time as they are posted to Discord.


### Skills: Python, GCP (Google Pub/Sub, Cloud Scheduler, BigQuery), Docker, OCR

# Manual Metabase MongoDB Setup Guide

If the automated script doesn't work, follow these steps:

## Step 1: Complete Metabase Web Setup
1. Open http://localhost:3000 in your browser
2. Click "Get started" or "Let's get started"
3. Fill out the setup form:
   - **Language**: English
   - **Company/Team name**: Amazon Sentiment Analysis
   - **How many people**: Just me
4. Create admin account:
   - **Email**: your-email@example.com
   - **Password**: choose a strong password
   - **First name**: Your name
   - **Last name**: (optional)
5. Click "Next" and complete the setup

## Step 2: Add MongoDB Database
1. In Metabase, go to **Admin** (gear icon) → **Databases**
2. Click **Add database**
3. Fill out the form:
   - **Database type**: MongoDB
   - **Name**: MongoDB - Sentiment Analysis
   - **Host**: mongo
   - **Port**: 27017
   - **Database name**: mlplatform
   - **Username**: (leave empty)
   - **Password**: (leave empty)
   - **Use a secure connection (SSL)**: No
4. Click **Save**
5. Click **Next** to sync the database

## Step 3: Explore Your Data
1. Go to **Browse data** in the main navigation
2. Click on **MongoDB - Sentiment Analysis**
3. You'll see these collections:
   - `predictions` - Sentiment analysis results
   - `processed_events` - Raw streaming data
   - `streaming_metrics` - Performance metrics

## Step 4: Create Your First Dashboard
1. Click on a collection (like `predictions`)
2. Click **Summarize** or create questions
3. Try queries like:
   - Count of predictions by sentiment
   - Average confidence scores
   - Trends over time

## Troubleshooting

### Connection Timeout
- Ensure MongoDB is running: `docker compose ps mongo`
- Check MongoDB logs: `docker compose logs mongo`

### No Data Showing
- Run the streaming pipeline to generate data
- Check if collections exist: `docker compose exec mongo mongosh mlplatform --eval "db.listCollectionNames()"`

### Permission Issues
- Make sure you're logged in as admin
- Check that the database connection was saved successfully
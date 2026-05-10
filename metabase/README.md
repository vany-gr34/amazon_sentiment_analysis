# Metabase Configuration

This directory contains configuration files for Metabase analytics dashboard integration.

## Files

- `init.sh` - Initialization script that runs when Metabase starts
- `dashboard_template.json` - Template for sentiment analysis dashboard
- `setup_mongodb.sh` - Script to configure MongoDB connection in Metabase

## Setup Instructions

1. **Start Metabase** (it's included in docker-compose.yml)
   ```bash
   docker compose up -d metabase
   ```

2. **Initial Metabase Setup**
   - Go to http://localhost:3000
   - Complete the first-time setup wizard
   - Set up admin account

3. **Configure MongoDB Connection**
   ```bash
   ./metabase/setup_mongodb.sh
   ```

4. **Manual MongoDB Setup** (if script fails)
   - Go to Admin → Databases → Add database
   - Select "MongoDB" as database type
   - Configure:
     - **Name**: MongoDB - Sentiment Analysis
     - **Host**: mongo
     - **Port**: 27017
     - **Database name**: mlplatform
     - **SSL**: Disabled

## Available Collections

After connecting to MongoDB, you'll see these collections:

- **predictions** - Sentiment analysis predictions with confidence scores
- **processed_events** - Raw streaming events
- **streaming_metrics** - Performance metrics from Spark streaming

## Creating Dashboards

1. Go to http://localhost:3000
2. Click "Browse data" → "MongoDB - Sentiment Analysis"
3. Explore collections and create questions/dashboards

## Example Queries

### Total Predictions
```sql
db.predictions.count()
```

### Sentiment Distribution
```sql
db.predictions.aggregate([
  {$group: {_id: "$prediction", count: {$sum: 1}}}
])
```

### Average Confidence by Sentiment
```sql
db.predictions.aggregate([
  {$group: {_id: "$prediction", avg_confidence: {$avg: "$confidence"}}}
])
```
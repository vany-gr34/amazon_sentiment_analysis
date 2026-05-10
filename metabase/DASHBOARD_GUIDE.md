# Metabase Sentiment Analysis Dashboards

## Available Collections & Data

### 1. `scored_reviews` (2,587 records)
Contains sentiment analysis results with these key fields:
- `prediction`: "positive", "negative", "neutral"
- `confidence`: Model confidence score (0-1)
- `score`: Original Amazon rating (1-5)
- `api_latency_ms`: API response time
- `processed_at`: Timestamp
- `product_id`, `user_id`: Identifiers
- `text`, `summary`: Review content

### 2. `processed_events` (4,994 records)
Raw streaming events:
- `event_time`: When event was received
- `processed_at`: When event was processed
- `score`: Original rating
- `text`: Full review text

### 3. `failed_records` (1,370 records)
Failed processing attempts for analysis.

## Recommended Dashboards

### Dashboard 1: Sentiment Analysis Overview
**Cards to create:**

1. **Total Reviews Processed** (KPI)
   - Collection: `scored_reviews`
   - Type: Number
   - Field: Count of all records

2. **Sentiment Distribution** (Pie Chart)
   - Collection: `scored_reviews`
   - Group by: `prediction`
   - Metric: Count

3. **Average Confidence Score** (KPI)
   - Collection: `scored_reviews`
   - Type: Number
   - Field: Average of `confidence`

4. **Average API Latency** (KPI)
   - Collection: `scored_reviews`
   - Type: Number
   - Field: Average of `api_latency_ms`

### Dashboard 2: Performance Analytics
**Cards to create:**

1. **Processing Rate Over Time** (Line Chart)
   - Collection: `scored_reviews`
   - X-axis: `processed_at` (by hour/day)
   - Y-axis: Count of records

2. **Confidence Distribution** (Histogram)
   - Collection: `scored_reviews`
   - Field: `confidence`
   - Bins: 10

3. **Latency Distribution** (Histogram)
   - Collection: `scored_reviews`
   - Field: `api_latency_ms`
   - Bins: 20

4. **Error Rate** (KPI)
   - Formula: `failed_records.count / (scored_reviews.count + failed_records.count) * 100`

### Dashboard 3: Sentiment vs Ratings
**Cards to create:**

1. **Sentiment by Original Rating** (Bar Chart)
   - Collection: `scored_reviews`
   - X-axis: `score` (1-5 stars)
   - Y-axis: Count
   - Group by: `prediction`

2. **Confidence vs Rating Correlation** (Scatter Plot)
   - Collection: `scored_reviews`
   - X-axis: `score`
   - Y-axis: `confidence`
   - Color: `prediction`

3. **Accuracy Analysis** (Table)
   - Collection: `scored_reviews`
   - Fields: `score`, `prediction`, `confidence`
   - Filter: High confidence scores

## How to Create Dashboards in Metabase

### Step 1: Access Your Data
1. Go to `http://localhost:3000`
2. Click **"Browse data"**
3. Select **"MongoDB - Sentiment Analysis"**
4. Click on a collection (e.g., `scored_reviews`)

### Step 2: Create Questions
1. Click **"Summarize"** or **"Filter"** to explore data
2. Try these sample questions:

**Sentiment Breakdown:**
- Click `scored_reviews` collection
- Click **"Summarize"**
- Group by `prediction`
- Visualize as Pie Chart

**Confidence Over Time:**
- Click `scored_reviews` collection
- Click **"Summarize"**
- Group by `processed_at` (by day)
- Average of `confidence`
- Visualize as Line Chart

**Top Products:**
- Click `scored_reviews` collection
- Click **"Summarize"**
- Group by `product_id`
- Count of records
- Sort descending
- Limit to top 10

### Step 3: Build Dashboards
1. Save your questions
2. Go to **"Browse data"** → **"All items"**
3. Click **"New dashboard"**
4. Add your saved questions to the dashboard
5. Arrange and resize cards as needed

## Advanced Analytics Ideas

1. **Sentiment Trends**: How sentiment changes over time
2. **User Behavior**: Which users leave what types of reviews
3. **Product Analysis**: Which products get what sentiments
4. **Performance Monitoring**: API latency and error rates
5. **Quality Assurance**: High-confidence predictions vs low-confidence ones

## Sample MongoDB Queries for Custom Questions

```javascript
// Sentiment distribution
db.scored_reviews.aggregate([
  {$group: {_id: "$prediction", count: {$sum: 1}}}
])

// Average confidence by sentiment
db.scored_reviews.aggregate([
  {$group: {_id: "$prediction", avg_confidence: {$avg: "$confidence"}}}
])

// Reviews processed per hour
db.scored_reviews.aggregate([
  {$group: {
    _id: {
      $dateToString: {format: "%Y-%m-%d %H:00", date: {$toDate: "$processed_at"}}
    },
    count: {$sum: 1}
  }},
  {$sort: {_id: 1}}
])
```

## Next Steps

1. **Explore the data** in Metabase
2. **Create your first dashboard** with basic KPIs
3. **Add time-series charts** for trends
4. **Set up alerts** for unusual patterns
5. **Share dashboards** with stakeholders

Your sentiment analysis data is now ready for comprehensive analytics! 📊
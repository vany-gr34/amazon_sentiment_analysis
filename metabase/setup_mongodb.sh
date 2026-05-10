#!/bin/bash
# Metabase MongoDB Setup Script
# Run this after Metabase is running and you've completed the initial web setup

echo "Setting up Metabase MongoDB connection..."
echo ""

# Check if Metabase is running
echo "1. Checking Metabase health..."
if curl -s http://localhost:3000/api/health | grep -q "ok"; then
    echo "✅ Metabase is running"
else
    echo "❌ Metabase is not responding. Please start it first:"
    echo "   docker compose up -d metabase"
    exit 1
fi

echo ""
echo "2. IMPORTANT: Before running this script, you MUST complete Metabase setup in your browser:"
echo "   - Go to http://localhost:3000"
echo "   - Complete the 'Welcome to Metabase' setup wizard"
echo "   - Create an admin account"
echo "   - Skip adding a database for now"
echo ""
read -p "Have you completed the Metabase web setup? (y/N): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Please complete the web setup first, then run this script again."
    exit 1
fi

# Get Metabase session token
echo ""
echo "3. Getting Metabase session..."
echo "Please enter your Metabase admin credentials:"
read -p "Email: " ADMIN_EMAIL
read -s -p "Password: " ADMIN_PASSWORD
echo ""

SESSION_TOKEN=$(curl -s -X POST http://localhost:3000/api/session \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"$ADMIN_EMAIL\", \"password\": \"$ADMIN_PASSWORD\"}" | jq -r '.id')

if [ "$SESSION_TOKEN" = "null" ] || [ -z "$SESSION_TOKEN" ]; then
    echo "❌ Failed to authenticate. Please check your credentials and ensure you've completed the web setup."
    exit 1
fi

echo "✅ Authentication successful!"

# Add MongoDB database connection
echo ""
echo "4. Adding MongoDB database connection..."
RESPONSE=$(curl -s -X POST http://localhost:3000/api/database \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: $SESSION_TOKEN" \
  -d '{
    "name": "MongoDB - Sentiment Analysis",
    "engine": "mongo",
    "details": {
      "host": "mongo",
      "port": 27017,
      "dbname": "mlplatform",
      "ssl": false
    },
    "is_full_sync": true,
    "schedules": {
      "cache_field_values": {
        "schedule_type": "daily",
        "schedule_hour": 0
      },
      "metadata_sync": {
        "schedule_type": "daily",
        "schedule_hour": 1
      }
    }
  }')

if echo "$RESPONSE" | grep -q "id"; then
    echo "✅ MongoDB database added successfully!"
else
    echo "❌ Failed to add MongoDB database. Response: $RESPONSE"
    exit 1
fi

echo ""
echo "🎉 Setup complete! You can now:"
echo "1. Go to http://localhost:3000"
echo "2. Navigate to 'Browse data' → 'MongoDB - Sentiment Analysis'"
echo "3. Explore your sentiment analysis collections:"
echo "   - predictions (sentiment results)"
echo "   - processed_events (raw data)"
echo "   - streaming_metrics (performance data)"
echo "4. Create dashboards and questions!"
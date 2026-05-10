#!/bin/bash
# Test MongoDB connection and data availability for Metabase

echo "Testing MongoDB connection for Metabase analytics..."

# Check if MongoDB is running
echo "1. Checking MongoDB connectivity..."
docker compose exec mongo mongosh --eval "db.adminCommand('ping')" --quiet

if [ $? -eq 0 ]; then
    echo "✅ MongoDB is accessible"
else
    echo "❌ MongoDB connection failed"
    exit 1
fi

# Check available databases
echo ""
echo "2. Available databases:"
docker compose exec mongo mongosh --eval "db.adminCommand('listDatabases')" --quiet | jq -r '.databases[].name' 2>/dev/null || docker compose exec mongo mongosh --eval "db.adminCommand('listDatabases')" --quiet

# Check mlplatform database collections
echo ""
echo "3. Collections in mlplatform database:"
docker compose exec mongo mongosh mlplatform --eval "db.listCollectionNames()" --quiet

# Check if predictions collection has data
echo ""
echo "4. Sample prediction data:"
docker compose exec mongo mongosh mlplatform --eval "db.predictions.findOne()" --quiet

# Check collection counts
echo ""
echo "5. Collection document counts:"
docker compose exec mongo mongosh mlplatform --eval "
print('Predictions:', db.predictions.countDocuments());
print('Processed events:', db.processed_events.countDocuments());
print('Streaming metrics:', db.streaming_metrics.countDocuments());
" --quiet

echo ""
echo "✅ MongoDB setup verification complete!"
echo ""
echo "Next steps for Metabase:"
echo "1. Go to http://localhost:3000"
echo "2. Complete Metabase setup if first time"
echo "3. Add MongoDB database connection (host: mongo, port: 27017, db: mlplatform)"
echo "4. Start creating dashboards from the collections above"
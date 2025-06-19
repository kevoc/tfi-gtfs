
# run this script to download sample data for the unit tests.

# your working directory should be the root of the git repo, not this folder.
source settings.env

curl -o tests/GTFS.zip "$GTFS_STATIC_URL"
curl -o tests/live_data.bin -H "X-API-KEY: $API_KEY" "$GTFS_LIVE_URL"

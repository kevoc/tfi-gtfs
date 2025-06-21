
# run this script to download sample data for the unit tests.

# your working directory should be the root of the git repo, not this folder.
source settings.env

rm tests/GTFS.zip tests/realtime_data.bin tests/asset_timestamp.txt

curl -o tests/GTFS.zip "$GTFS_STATIC_URL"
curl -o tests/realtime_data.bin -H "X-API-KEY: $API_KEY" "$GTFS_LIVE_URL"

# the datetime.now() function will return this timestamp when the
# server is testing with cached assets.
date "+%s" > tests/asset_timestamp.txt
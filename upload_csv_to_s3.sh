#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <bucket-name>"
  exit 1
fi

# Bucket name passed as the first argument
BUCKET_NAME=$1

# Directory containing the network CSV files
CSV_DIR="./csv"

# Check if the CSV directory exists
if [ ! -d "$CSV_DIR" ]; then
  echo "Directory $CSV_DIR does not exist."
  exit 1
fi

# Loop through all subdirectories in the CSV directory
for network_dir in "$CSV_DIR"/*/; do
  # Extract the network name (basename of the directory)
  network_name=$(basename "$network_dir")

  echo "Uploading files from $network_name to s3://$BUCKET_NAME/$network_name"
  
  # Upload the files in the current network directory to the corresponding S3 path
  aws s3 cp "$network_dir" "s3://$BUCKET_NAME/$network_name" --recursive
done

echo "All files uploaded to s3://$BUCKET_NAME"

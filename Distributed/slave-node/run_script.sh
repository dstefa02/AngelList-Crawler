#!/bin/bash
sudo docker-compose -f docker-compose.yml down
sudo docker-compose -f docker-compose.yml up -d
echo "Wait 10 seconds..."
sleep 10 # Waits 5 seconds.
echo "Finish waiting."

python3 slave_crawler.py
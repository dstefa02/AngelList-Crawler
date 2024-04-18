# AngelList-Crawler

This repository contains the implementation of a web crawler for AngelList, designed to collect data efficiently via two distinct architectures:

1. Multithreaded Approach: Utilizes multiple Selenium browser instances across different threads to parallelize data collection.
2. Distributed Approach: Employs a master-slave architecture with Apache Kafka to manage Selenium instances across multiple machines, each with unique IP addresses.

# Features
* Multithreading: Leverages multithreading to improve the speed and efficiency of data collection on a single machine.
* Distributed Crawling: Uses Apache Kafka to distribute tasks across multiple nodes, enhancing the crawler's scalability and reliability by diversifying IP addresses and computational resources.
* Robust Error Handling: Implements sophisticated error-handling mechanisms to manage and recover from failures during crawling processes.
* Data Extraction: Efficiently extracts and processes structured data from AngelList, catering to specific data analysis needs.

# Installation
Clone the repository to your local machine:
  ```git clone https://github.com/yourusername/angellist-crawler.git
  cd angellist-crawler
  ```

# Prerequisites
Ensure you have the following installed:
* Selenium WebDriver
* Apache Kafka

# Install the required Python libraries:
```
pip install -r requirements.txt
```

# Usage
## Multithreaded Crawler
To run the multithreaded crawler:
```
python multithreaded_crawler.py
```

## Distributed Crawler
Ensure your Apache Kafka and Zookeeper instances are up and running before executing the distributed crawler.

To start the master:
```
python master_crawler.py
```

To start each slave in a different machine:
```
python slave_crawler.py
```

# Configuration
Edit the config.json file to set the number of threads, Kafka topic names, and other relevant parameters based on your requirements.
```
{
    "num_threads": 4,
    "kafka_topic": "crawler_jobs",
    "kafka_server": "localhost:9092"
}
```

# License
This project is licensed under the MIT License - see the LICENSE.md file for details.


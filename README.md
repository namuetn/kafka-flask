## Requires:

* Kafka

* Python3 with Flask Framework

* Database: MongoDB

* Postman

## Installation:

* Install and active Kafka with default port: 9092 (details: https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-ubuntu-20-04#step-5-mdash-testing-the-kafka-installation\

* Install and active MongoDB with default port: 27017

* Set Up a Virtual Environment (details: https://www.freecodecamp.org/news/how-to-setup-virtual-environments-in-python/)

* Install python libraries like: Flask, kafka-python, pymongo,...
```sh
  pip install -r requirements.txt
```

## Running:
* MongoDB: running mongo script
```sh
  mongo
```

* Kafka: in Terminal:
```sh
  ~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic WorkerTopic --from-beginning
```

* Flask:
Run main.py:
```sh
  flask --app main run --debug
```

Run consumer.py:
```sh
  python consumer.py
```

## Step:
1. Run Postman and send data 
![Screenshot from 2023-11-29 15-00-14](https://github.com/namuetn/kafka-flask/assets/48872422/deb9c903-2045-4862-a578-fe8cbd03b74d)
2. Check kafka:
![Screenshot from 2023-11-29 15-01-44](https://github.com/namuetn/kafka-flask/assets/48872422/d5af817f-4ee8-43cf-9ce1-86031488832e)
3. Check database:
  In mongo workspace run:
  ```sh
    db.message.find().pretty()
  ```
  ![Screenshot from 2023-11-29 15-03-30](https://github.com/namuetn/kafka-flask/assets/48872422/ce5b0d6b-0962-4a30-8f63-cfb708a065fd)






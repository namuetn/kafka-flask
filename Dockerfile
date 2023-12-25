FROM ubuntu:20.04

WORKDIR /app

RUN apt-get update && \
    apt-get install -y python3.8 python3-pip

COPY loop_python_file.sh /app/
COPY consumer_message.py /app/
COPY topic.py /app/

RUN pip3 install kafka-python mysql-connector-python confluent_kafka

ENV BOOTSTRAP_SERVERS=""
ENV TOPIC=""

CMD ["/bin/bash", "-c", "./loop_python_file.sh $BOOTSTRAP_SERVERS $TOPIC"]


# FROM ubuntu:20.04

# RUN apt-get update && \
#     apt-get install -y python3.8 python3-pip 

# WORKDIR /app

# COPY producer_message.py /app/
# COPY topic.py /app/

# RUN pip3 install kafka-python confluent_kafka

# CMD ["/usr/bin/python3", "producer_message.py"]

from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json


app = Flask(__name__)

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/message', methods=['POST'])
def handle_api_request():
    try:
        data = request.get_json()
        producer.send('WorkerTopic', value=data)

        return jsonify({'message': 'Request received successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

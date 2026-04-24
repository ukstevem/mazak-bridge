FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt requests python-dotenv
COPY bridge_mtc_to_mqtt.py /app/
ENV PYTHONUNBUFFERED=1
CMD ["python", "bridge_mtc_to_mqtt.py"]

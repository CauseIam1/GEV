FROM python:3.11-slim

WORKDIR /app

# Dependencies first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        flask>=3.0.0 \
        flask-cors>=4.0.0 \
        flask-socketio>=5.3.0 \
        simple-websocket>=1.0.0

# GEV application code (service + Cesium viewer template)
COPY gev_server.py .
COPY templates/ ./templates/

# GEV_PORT (compose publishes host 7004 -> container 5001)
EXPOSE 5001

# Direct python run — threading Socket.IO mode, mirrors burst_receiver.py
CMD ["python3", "gev_server.py"]
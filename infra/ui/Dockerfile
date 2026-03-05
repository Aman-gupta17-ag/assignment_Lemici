FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONPATH=/app
EXPOSE 8501
ENV API_URL=http://api:8000
CMD ["streamlit", "run", "rag/ui.py", "--server.port=8501", "--server.address=0.0.0.0"]

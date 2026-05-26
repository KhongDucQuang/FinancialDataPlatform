# Sử dụng Python base image mỏng nhẹ
FROM python:3.9-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Copy file requirements và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy script python vào
COPY scraper.py .

# Biến môi trường báo cho Python không buffer log (để xem log realtime trên Airflow)
ENV PYTHONUNBUFFERED=1

# Lệnh thực thi khi container chạy
CMD ["python", "scraper.py"]
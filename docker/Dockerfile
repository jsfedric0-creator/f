FROM ubuntu:22.04

# تثبيت المتطلبات
RUN apt-get update && apt-get install -y \
    nginx \
    ffmpeg \
    python3 \
    python3-pip \
    wget \
    curl \
    sqlite3 \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# إنشاء المجلدات
RUN mkdir -p /flussonic/{data,logs,streams,config}

# نسخ الملفات
COPY . /flussonic
WORKDIR /flussonic

# تثبيت مكتبات Python
RUN pip3 install -r requirements.txt

# نسخ إعدادات Nginx
COPY nginx/nginx.conf /etc/nginx/
COPY nginx/streams.conf /etc/nginx/sites-available/

# تفعيل المواقع
RUN ln -sf /etc/nginx/sites-available/streams.conf /etc/nginx/sites-enabled/

# الصلاحيات
RUN chmod +x /flussonic/scripts/*.sh

EXPOSE 80 1935 8000 25462

CMD ["supervisord", "-c", "/flussonic/config/supervisor.conf"]

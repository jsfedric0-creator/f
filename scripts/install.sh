#!/bin/bash

echo "📦 Installing Flussonic-like IPTV Server"

# تحديث النظام
apt-get update
apt-get upgrade -y

# تثبيت المتطلبات
apt-get install -y \
    python3 \
    python3-pip \
    nginx \
    ffmpeg \
    sqlite3 \
    supervisor

# إنشاء المجلدات
mkdir -p /flussonic/{data,logs,streams,config}

# نسخ الملفات
cp -r . /flussonic/

# تثبيت مكتبات Python
pip3 install -r /flussonic/requirements.txt

# إعدادات Nginx
cp /flussonic/nginx/nginx.conf /etc/nginx/
cp /flussonic/nginx/streams.conf /etc/nginx/sites-available/

# تفعيل الموقع
ln -sf /etc/nginx/sites-available/streams.conf /etc/nginx/sites-enabled/

# إعداد Supervisor
cp /flussonic/config/supervisor.conf /etc/supervisor/conf.d/

# إعادة تشغيل الخدمات
systemctl restart nginx
systemctl restart supervisor

echo "✅ Installation complete!"
echo "🌐 Admin Panel: http://your-ip:8000"
echo "📺 Xtream Codes: http://your-ip:25462"
echo "🔐 Default: admin / admin123"

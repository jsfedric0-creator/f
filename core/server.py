import socket
import threading
import json
import hashlib
import sqlite3
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse

class XtreamCodesServer:
    def __init__(self, host='0.0.0.0', port=25462):
        self.host = host
        self.port = port
        self.db_path = "/flussonic/data/iptv.db"
        self.setup_database()
        
    def setup_database(self):
        """إنشاء قاعدة بيانات Xtream Codes"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # جدول المستخدمين
        c.execute('''CREATE TABLE IF NOT EXISTS users_xtream (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE,
            password TEXT,
            is_active INTEGER DEFAULT 1,
            max_connections INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            total_streams INTEGER DEFAULT 0
        )''')
        
        # جدول القنوات
        c.execute('''CREATE TABLE IF NOT EXISTS streams_xtream (
            stream_id INTEGER PRIMARY KEY,
            stream_type TEXT,
            category_id INTEGER,
            category_name TEXT,
            stream_name TEXT,
            stream_icon TEXT,
            epg_channel_id TEXT,
            added TEXT,
            is_live INTEGER DEFAULT 1,
            direct_source TEXT,
            direct_source_type TEXT
        )''')
        
        # جدول الفئات
        c.execute('''CREATE TABLE IF NOT EXISTS categories_xtream (
            category_id INTEGER PRIMARY KEY,
            category_name TEXT,
            parent_id INTEGER DEFAULT 0
        )''')
        
        conn.commit()
        conn.close()
        
        # إضافة بيانات افتراضية
        self.add_default_data()
    
    def add_default_data(self):
        """إضافة بيانات افتراضية"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # إضافة مستخدم افتراضي
        try:
            hashed_pass = hashlib.md5("demo".encode()).hexdigest()
            c.execute("INSERT OR IGNORE INTO users_xtream (username, password) VALUES (?, ?)",
                     ("demo", hashed_pass))
        except:
            pass
        
        # إضافة فئات افتراضية
        categories = [
            (1, "Live TV", 0),
            (2, "Movies", 0),
            (3, "Series", 0),
            (4, "Sports", 1),
            (5, "News", 1),
            (6, "Kids", 1)
        ]
        
        for cat_id, name, parent in categories:
            c.execute("INSERT OR IGNORE INTO categories_xtream VALUES (?, ?, ?)",
                     (cat_id, name, parent))
        
        # إضافة قنوات افتراضية
        streams = [
            (1, "live", 1, "Live TV", "BBC One", "", "bbc1", "2023-01-01 00:00:00", 1, "http://example.com/bbc1.m3u8", "m3u8"),
            (2, "live", 1, "Live TV", "CNN", "", "cnn", "2023-01-01 00:00:00", 1, "http://example.com/cnn.m3u8", "m3u8"),
            (3, "vod", 2, "Movies", "Avengers", "", "avengers", "2023-01-01 00:00:00", 0, "http://example.com/avengers.mp4", "mp4")
        ]
        
        for stream in streams:
            c.execute("INSERT OR IGNORE INTO streams_xtream VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     stream)
        
        conn.commit()
        conn.close()
    
    def handle_request(self, data):
        """معالجة طلبات Xtream Codes"""
        try:
            # تحليل البيانات
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            
            # طلب API
            if "/player_api.php" in data:
                return self.handle_api_request(data)
            # طلب البث المباشر
            elif "/live/" in data:
                return self.handle_stream_request(data)
            # طلب M3U
            elif "/get.php" in data:
                return self.handle_m3u_request(data)
            
            return b"Invalid request"
            
        except Exception as e:
            return f"Error: {str(e)}".encode()
    
    def handle_api_request(self, data):
        """معالجة طلبات API"""
        parsed = urllib.parse.urlparse(data.split(' ')[1] if ' ' in data else data)
        params = urllib.parse.parse_qs(parsed.query)
        
        action = params.get('action', [''])[0]
        username = params.get('username', [''])[0]
        password = params.get('password', [''])[0]
        
        # التحقق من المستخدم
        user = self.authenticate_user(username, password)
        if not user:
            return json.dumps({"user_info": {"auth": 0}}).encode()
        
        # معالجة الإجراءات
        if action == "get_live_categories":
            return self.get_live_categories()
        elif action == "get_live_streams":
            category_id = params.get('category_id', [''])[0]
            return self.get_live_streams(category_id)
        elif action == "get_vod_categories":
            return self.get_vod_categories()
        elif action == "get_vod_streams":
            category_id = params.get('category_id', [''])[0]
            return self.get_vod_streams(category_id)
        else:
            # معلومات المستخدم
            return self.get_user_info(username)
    
    def authenticate_user(self, username, password):
        """المصادقة"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        hashed_pass = hashlib.md5(password.encode()).hexdigest()
        c.execute("SELECT * FROM users_xtream WHERE username=? AND password=? AND is_active=1",
                 (username, hashed_pass))
        user = c.fetchone()
        conn.close()
        return user
    
    def get_user_info(self, username):
        """الحصول على معلومات المستخدم"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT * FROM users_xtream WHERE username=?", (username,))
        user = c.fetchone()
        conn.close()
        
        if user:
            response = {
                "user_info": {
                    "username": user[1],
                    "password": "********",
                    "message": "Authorized",
                    "auth": 1,
                    "status": "Active",
                    "exp_date": user[6] or "2099-12-31",
                    "is_trial": 0,
                    "active_cons": 0,
                    "created_at": user[5],
                    "max_connections": user[4],
                    "allowed_output_formats": ["m3u8", "ts", "rtmp"]
                }
            }
            return json.dumps(response).encode()
        
        return json.dumps({"user_info": {"auth": 0}}).encode()
    
    def get_live_categories(self):
        """الحصول على فئات البث المباشر"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT category_id, category_name FROM categories_xtream WHERE parent_id=1")
        categories = c.fetchall()
        conn.close()
        
        result = []
        for cat_id, name in categories:
            result.append({
                "category_id": cat_id,
                "category_name": name,
                "parent_id": 1
            })
        
        return json.dumps({"categories": result}).encode()
    
    def start(self):
        """بدء الخادم"""
        server = HTTPServer((self.host, self.port), XtreamRequestHandler)
        server.xtream_server = self
        print(f"Xtream Codes Server started on {self.host}:{self.port}")
        server.serve_forever()

class XtreamRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.xtream_server.handle_http_request(self)
    
    def do_POST(self):
        self.server.xtream_server.handle_http_request(self)

if __name__ == "__main__":
    server = XtreamCodesServer()
    server.start()

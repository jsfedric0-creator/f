import sqlite3
import json
import hashlib
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import threading

class IPTVDatabase:
    """قاعدة بيانات IPTV متكاملة مع دعم Xtream Codes"""
    
    def __init__(self, db_path: str = "/flussonic/data/iptv.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_database()
    
    def init_database(self):
        """تهيئة قاعدة البيانات وإنشاء الجداول"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            # Enable foreign keys
            c.execute("PRAGMA foreign_keys = ON")
            
            # ============ جدول المستخدمين ============
            c.execute('''CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                email TEXT,
                role TEXT DEFAULT 'user',  -- admin, user, reseller
                is_active INTEGER DEFAULT 1,
                max_connections INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                last_login TIMESTAMP,
                total_streams INTEGER DEFAULT 0,
                credits REAL DEFAULT 0.0,
                settings TEXT DEFAULT '{}',
                api_key TEXT UNIQUE,
                UNIQUE(username)
            )''')
            
            # ============ جدول البثوث المباشرة ============
            c.execute('''CREATE TABLE IF NOT EXISTS live_streams (
                stream_id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_name TEXT NOT NULL,
                stream_url TEXT NOT NULL,
                stream_type TEXT DEFAULT 'live',  -- live, vod, series
                category_id INTEGER,
                category_name TEXT,
                stream_icon TEXT,
                epg_channel_id TEXT,
                added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                bitrate INTEGER DEFAULT 2000,
                resolution TEXT DEFAULT '1920x1080',
                codec TEXT DEFAULT 'h264',
                buffer_size INTEGER DEFAULT 8192,
                user_id INTEGER,
                proxy_url TEXT,
                last_checked TIMESTAMP,
                status TEXT DEFAULT 'stopped',  -- stopped, running, error
                viewers INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (category_id) REFERENCES categories(category_id)
            )''')
            
            # ============ جدول VOD (فيديو عند الطلب) ============
            c.execute('''CREATE TABLE IF NOT EXISTS vod_streams (
                vod_id INTEGER PRIMARY KEY AUTOINCREMENT,
                vod_name TEXT NOT NULL,
                vod_url TEXT NOT NULL,
                category_id INTEGER,
                category_name TEXT,
                vod_icon TEXT,
                description TEXT,
                rating REAL DEFAULT 0.0,
                year INTEGER,
                duration TEXT,  -- HH:MM:SS
                added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                director TEXT,
                actors TEXT,  -- JSON array
                imdb_id TEXT,
                trailer_url TEXT,
                subtitles TEXT,  -- JSON array of subtitle files
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )''')
            
            # ============ جدول المسلسلات ============
            c.execute('''CREATE TABLE IF NOT EXISTS series (
                series_id INTEGER PRIMARY KEY AUTOINCREMENT,
                series_name TEXT NOT NULL,
                category_id INTEGER,
                series_icon TEXT,
                description TEXT,
                rating REAL DEFAULT 0.0,
                year INTEGER,
                added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                imdb_id TEXT,
                trailer_url TEXT,
                total_seasons INTEGER DEFAULT 1,
                total_episodes INTEGER DEFAULT 0,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )''')
            
            # ============ جدول حلقات المسلسل ============
            c.execute('''CREATE TABLE IF NOT EXISTS series_episodes (
                episode_id INTEGER PRIMARY KEY AUTOINCREMENT,
                series_id INTEGER NOT NULL,
                season_number INTEGER DEFAULT 1,
                episode_number INTEGER,
                episode_name TEXT,
                episode_url TEXT NOT NULL,
                description TEXT,
                duration TEXT,  -- HH:MM:SS
                added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                subtitles TEXT,  -- JSON array
                FOREIGN KEY (series_id) REFERENCES series(series_id) ON DELETE CASCADE
            )''')
            
            # ============ جدول الفئات ============
            c.execute('''CREATE TABLE IF NOT EXISTS categories (
                category_id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_name TEXT NOT NULL,
                parent_id INTEGER DEFAULT 0,
                category_type TEXT DEFAULT 'live',  -- live, vod, series
                is_active INTEGER DEFAULT 1,
                user_id INTEGER,
                sort_order INTEGER DEFAULT 0,
                UNIQUE(category_name, category_type, user_id)
            )''')
            
            # ============ جدول EPG (دليل البرامج) ============
            c.execute('''CREATE TABLE IF NOT EXISTS epg_data (
                epg_id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                channel_name TEXT,
                programme_title TEXT NOT NULL,
                programme_desc TEXT,
                start TIMESTAMP NOT NULL,
                stop TIMESTAMP NOT NULL,
                category TEXT,
                icon TEXT,
                rating TEXT,
                episode_num TEXT,
                previously_shown TEXT,
                is_active INTEGER DEFAULT 1,
                UNIQUE(channel_id, programme_title, start)
            )''')
            
            # ============ جدول الجلسات النشطة ============
            c.execute('''CREATE TABLE IF NOT EXISTS active_sessions (
                session_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                stream_id INTEGER,
                client_ip TEXT,
                user_agent TEXT,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                bytes_sent INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (stream_id) REFERENCES live_streams(stream_id) ON DELETE SET NULL
            )''')
            
            # ============ جدول الإحصائيات ============
            c.execute('''CREATE TABLE IF NOT EXISTS statistics (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                stat_date DATE NOT NULL,
                user_id INTEGER,
                stream_id INTEGER,
                total_views INTEGER DEFAULT 0,
                total_bytes BIGINT DEFAULT 0,
                peak_viewers INTEGER DEFAULT 0,
                average_bitrate REAL DEFAULT 0.0,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
                FOREIGN KEY (stream_id) REFERENCES live_streams(stream_id) ON DELETE SET NULL
            )''')
            
            # ============ جدول السيرفرات ============
            c.execute('''CREATE TABLE IF NOT EXISTS servers (
                server_id INTEGER PRIMARY KEY AUTOINCREMENT,
                server_name TEXT NOT NULL,
                server_ip TEXT NOT NULL,
                server_port INTEGER DEFAULT 80,
                server_type TEXT DEFAULT 'stream',  -- stream, proxy, loadbalancer
                is_active INTEGER DEFAULT 1,
                max_streams INTEGER DEFAULT 100,
                current_streams INTEGER DEFAULT 0,
                location TEXT,
                priority INTEGER DEFAULT 1,
                api_url TEXT,
                api_key TEXT,
                last_checked TIMESTAMP
            )''')
            
            # ============ جدول الوكالات (Resellers) ============
            c.execute('''CREATE TABLE IF NOT EXISTS resellers (
                reseller_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                company_name TEXT,
                contact_person TEXT,
                phone TEXT,
                address TEXT,
                commission_rate REAL DEFAULT 10.0,
                total_sales REAL DEFAULT 0.0,
                balance REAL DEFAULT 0.0,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )''')
            
            # ============ جدول الفواتير ============
            c.execute('''CREATE TABLE IF NOT EXISTS invoices (
                invoice_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                invoice_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                amount REAL NOT NULL,
                status TEXT DEFAULT 'pending',  -- pending, paid, cancelled
                payment_method TEXT,
                transaction_id TEXT,
                due_date TIMESTAMP,
                description TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )''')
            
            # ============ جدول السجلات ============
            c.execute('''CREATE TABLE IF NOT EXISTS logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                log_level TEXT DEFAULT 'info',  -- info, warning, error, debug
                user_id INTEGER,
                action TEXT,
                details TEXT,
                ip_address TEXT,
                user_agent TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
            )''')
            
            # ============ جدول الإعدادات ============
            c.execute('''CREATE TABLE IF NOT EXISTS settings (
                setting_id INTEGER PRIMARY KEY AUTOINCREMENT,
                setting_key TEXT UNIQUE NOT NULL,
                setting_value TEXT,
                setting_type TEXT DEFAULT 'string',  -- string, integer, boolean, json
                category TEXT DEFAULT 'general',
                description TEXT,
                is_public INTEGER DEFAULT 0
            )''')
            
            # إنشاء الفهارس لتحسين الأداء
            self.create_indexes(c)
            
            # إضافة البيانات الافتراضية
            self.add_default_data(c)
            
            conn.commit()
            conn.close()
    
    def create_indexes(self, cursor):
        """إنشاء فهارس لتحسين الأداء"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)",
            "CREATE INDEX IF NOT EXISTS idx_users_api_key ON users(api_key)",
            "CREATE INDEX IF NOT EXISTS idx_live_streams_user ON live_streams(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_live_streams_category ON live_streams(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_vod_streams_category ON vod_streams(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_series_category ON series(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_epg_channel ON epg_data(channel_id)",
            "CREATE INDEX IF NOT EXISTS idx_epg_time ON epg_data(start, stop)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_user ON active_sessions(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_active ON active_sessions(is_active)",
            "CREATE INDEX IF NOT EXISTS idx_statistics_date ON statistics(stat_date)",
            "CREATE INDEX IF NOT EXISTS idx_logs_date ON logs(log_date)",
            "CREATE INDEX IF NOT EXISTS idx_logs_user ON logs(user_id)"
        ]
        
        for index in indexes:
            cursor.execute(index)
    
    def add_default_data(self, cursor):
        """إضافة البيانات الافتراضية للنظام"""
        
        # إعدادات النظام الافتراضية
        default_settings = [
            ('system_name', 'Flussonic IPTV', 'string', 'general', 'اسم النظام'),
            ('system_version', '1.0.0', 'string', 'general', 'إصدار النظام'),
            ('max_connections_per_user', '3', 'integer', 'streaming', 'أقصى اتصالات لكل مستخدم'),
            ('stream_timeout', '30', 'integer', 'streaming', 'مهلة البث بالثواني'),
            ('hls_segment_duration', '2', 'integer', 'streaming', 'مدة قطعة HLS'),
            ('hls_playlist_length', '10', 'integer', 'streaming', 'طول قائمة HLS'),
            ('enable_registration', '1', 'boolean', 'security', 'تفعيل التسجيل'),
            ('require_email_verification', '0', 'boolean', 'security', 'تفعيل البريد الإلكتروني'),
            ('default_user_expiry_days', '30', 'integer', 'billing', 'مدة صلاحية المستخدم'),
            ('currency', 'USD', 'string', 'billing', 'العملة'),
            ('timezone', 'UTC', 'string', 'general', 'المنطقة الزمنية'),
            ('language', 'ar', 'string', 'general', 'اللغة الافتراضية'),
            ('enable_xtream_api', '1', 'boolean', 'api', 'تفعيل Xtream API'),
            ('enable_m3u_api', '1', 'boolean', 'api', 'تفعيل M3U API'),
            ('enable_epg', '1', 'boolean', 'epg', 'تفعيل EPG')
        ]
        
        for key, value, type_, category, desc in default_settings:
            cursor.execute('''
                INSERT OR IGNORE INTO settings (setting_key, setting_value, setting_type, category, description)
                VALUES (?, ?, ?, ?, ?)
            ''', (key, value, type_, category, desc))
        
        # فئات افتراضية
        default_categories = [
            ('عامة', 0, 'live', 1),
            ('رياضية', 0, 'live', 2),
            ('أخبار', 0, 'live', 3),
            ('ترفيه', 0, 'live', 4),
            ('أطفال', 0, 'live', 5),
            ('أفلام', 0, 'vod', 1),
            ('مسلسلات', 0, 'vod', 2),
            ('وثائقيات', 0, 'vod', 3)
        ]
        
        for name, parent, cat_type, order in default_categories:
            cursor.execute('''
                INSERT OR IGNORE INTO categories (category_name, parent_id, category_type, sort_order)
                VALUES (?, ?, ?, ?)
            ''', (name, parent, cat_type, order))
        
        # مستخدم مدير افتراضي
        admin_password = hashlib.sha256("admin123".encode()).hexdigest()
        cursor.execute('''
            INSERT OR IGNORE INTO users (username, password, email, role, max_connections)
            VALUES (?, ?, ?, ?, ?)
        ''', ('admin', admin_password, 'admin@iptv.com', 'admin', 10))
        
        # مستخدم تجريبي
        demo_password = hashlib.sha256("demo123".encode()).hexdigest()
        cursor.execute('''
            INSERT OR IGNORE INTO users (username, password, email, role, expires_at)
            VALUES (?, ?, ?, ?, ?)
        ''', ('demo', demo_password, 'demo@iptv.com', 'user', 
              (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')))
    
    # ============ إدارة المستخدمين ============
    
    def create_user(self, username: str, password: str, email: str = None, 
                   role: str = "user", max_connections: int = 1) -> int:
        """إنشاء مستخدم جديد"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            hashed_password = hashlib.sha256(password.encode()).hexdigest()
            api_key = hashlib.sha256(f"{username}{datetime.now()}".encode()).hexdigest()
            
            c.execute('''
                INSERT INTO users (username, password, email, role, max_connections, api_key)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (username, hashed_password, email, role, max_connections, api_key))
            
            user_id = c.lastrowid
            conn.commit()
            conn.close()
            
            self.log_action(user_id, "user_created", f"Created user: {username}")
            return user_id
    
    def authenticate_user(self, username: str, password: str) -> Optional[dict]:
        """المصادقة على المستخدم"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            hashed_password = hashlib.sha256(password.encode()).hexdigest()
            
            c.execute('''
                SELECT * FROM users 
                WHERE username = ? AND password = ? AND is_active = 1
            ''', (username, hashed_password))
            
            user = c.fetchone()
            
            if user:
                # تحديث آخر دخول
                c.execute('''
                    UPDATE users SET last_login = CURRENT_TIMESTAMP 
                    WHERE id = ?
                ''', (user['id'],))
                conn.commit()
                
                self.log_action(user['id'], "user_login", "User logged in")
            
            conn.close()
            return dict(user) if user else None
    
    def get_user_by_api_key(self, api_key: str) -> Optional[dict]:
        """الحصول على مستخدم باستخدام مفتاح API"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            c.execute('''
                SELECT * FROM users 
                WHERE api_key = ? AND is_active = 1
            ''', (api_key,))
            
            user = c.fetchone()
            conn.close()
            return dict(user) if user else None
    
    # ============ إدارة البث المباشر ============
    
    def add_live_stream(self, stream_data: dict) -> int:
        """إضافة بث مباشر جديد"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            # إنشاء رابط بروكسي
            stream_hash = hashlib.md5(stream_data['stream_url'].encode()).hexdigest()[:8]
            proxy_url = f"http://localhost/live/{stream_hash}"
            
            c.execute('''
                INSERT INTO live_streams (
                    stream_name, stream_url, stream_type, category_id, category_name,
                    stream_icon, epg_channel_id, user_id, proxy_url, bitrate, resolution
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stream_data.get('name'),
                stream_data.get('url'),
                stream_data.get('type', 'live'),
                stream_data.get('category_id'),
                stream_data.get('category_name'),
                stream_data.get('icon'),
                stream_data.get('epg_id'),
                stream_data.get('user_id'),
                proxy_url,
                stream_data.get('bitrate', 2000),
                stream_data.get('resolution', '1920x1080')
            ))
            
            stream_id = c.lastrowid
            
            # تحديث عدد بثوث المستخدم
            if stream_data.get('user_id'):
                c.execute('''
                    UPDATE users SET total_streams = total_streams + 1 
                    WHERE id = ?
                ''', (stream_data['user_id'],))
            
            conn.commit()
            conn.close()
            
            self.log_action(stream_data.get('user_id'), "stream_added", 
                          f"Added stream: {stream_data.get('name')}")
            return stream_id
    
    def get_live_streams(self, user_id: int = None, category_id: int = None) -> List[dict]:
        """الحصول على قائمة البثوث المباشرة"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            query = "SELECT * FROM live_streams WHERE is_active = 1"
            params = []
            
            if user_id:
                query += " AND user_id = ?"
                params.append(user_id)
            
            if category_id:
                query += " AND category_id = ?"
                params.append(category_id)
            
            query += " ORDER BY stream_name"
            
            c.execute(query, params)
            streams = [dict(row) for row in c.fetchall()]
            conn.close()
            return streams
    
    def get_stream_by_id(self, stream_id: int) -> Optional[dict]:
        """الحصول على بث بواسطة المعرف"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            c.execute('''
                SELECT ls.*, u.username, c.category_name 
                FROM live_streams ls
                LEFT JOIN users u ON ls.user_id = u.id
                LEFT JOIN categories c ON ls.category_id = c.category_id
                WHERE ls.stream_id = ? AND ls.is_active = 1
            ''', (stream_id,))
            
            stream = c.fetchone()
            conn.close()
            return dict(stream) if stream else None
    
    def update_stream_status(self, stream_id: int, status: str, viewers: int = 0):
        """تحديث حالة البث"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            c.execute('''
                UPDATE live_streams 
                SET status = ?, viewers = ?, last_checked = CURRENT_TIMESTAMP
                WHERE stream_id = ?
            ''', (status, viewers, stream_id))
            
            conn.commit()
            conn.close()
    
    # ============ إدارة VOD ============
    
    def add_vod(self, vod_data: dict) -> int:
        """إضافة فيديو عند الطلب"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            actors = json.dumps(vod_data.get('actors', [])) if vod_data.get('actors') else None
            subtitles = json.dumps(vod_data.get('subtitles', [])) if vod_data.get('subtitles') else None
            
            c.execute('''
                INSERT INTO vod_streams (
                    vod_name, vod_url, category_id, category_name,
                    vod_icon, description, rating, year, duration,
                    director, actors, imdb_id, trailer_url, subtitles, user_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                vod_data.get('name'),
                vod_data.get('url'),
                vod_data.get('category_id'),
                vod_data.get('category_name'),
                vod_data.get('icon'),
                vod_data.get('description'),
                vod_data.get('rating', 0.0),
                vod_data.get('year'),
                vod_data.get('duration'),
                vod_data.get('director'),
                actors,
                vod_data.get('imdb_id'),
                vod_data.get('trailer_url'),
                subtitles,
                vod_data.get('user_id')
            ))
            
            vod_id = c.lastrowid
            conn.commit()
            conn.close()
            
            self.log_action(vod_data.get('user_id'), "vod_added", 
                          f"Added VOD: {vod_data.get('name')}")
            return vod_id
    
    # ============ إدارة الفئات ============
    
    def get_categories(self, category_type: str = None, user_id: int = None) -> List[dict]:
        """الحصول على الفئات"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            query = "SELECT * FROM categories WHERE is_active = 1"
            params = []
            
            if category_type:
                query += " AND category_type = ?"
                params.append(category_type)
            
            if user_id:
                query += " AND (user_id = ? OR user_id IS NULL)"
                params.append(user_id)
            
            query += " ORDER BY sort_order, category_name"
            
            c.execute(query, params)
            categories = [dict(row) for row in c.fetchall()]
            conn.close()
            return categories
    
    def add_category(self, name: str, category_type: str = "live", 
                    parent_id: int = 0, user_id: int = None) -> int:
        """إضافة فئة جديدة"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            # الحصول على أعلى ترتيب
            c.execute('''
                SELECT MAX(sort_order) FROM categories 
                WHERE category_type = ? AND user_id = ?
            ''', (category_type, user_id))
            
            max_order = c.fetchone()[0] or 0
            
            c.execute('''
                INSERT INTO categories (category_name, category_type, parent_id, user_id, sort_order)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, category_type, parent_id, user_id, max_order + 1))
            
            category_id = c.lastrowid
            conn.commit()
            conn.close()
            
            self.log_action(user_id, "category_added", f"Added category: {name}")
            return category_id
    
    # ============ إدارة EPG ============
    
    def add_epg_data(self, channel_id: str, programmes: List[dict]):
        """إضافة بيانات EPG"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            for prog in programmes:
                c.execute('''
                    INSERT OR REPLACE INTO epg_data 
                    (channel_id, channel_name, programme_title, programme_desc, 
                     start, stop, category, icon, rating, episode_num, previously_shown)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    channel_id,
                    prog.get('channel'),
                    prog.get('title'),
                    prog.get('desc'),
                    prog.get('start'),
                    prog.get('stop'),
                    prog.get('category'),
                    prog.get('icon'),
                    prog.get('rating'),
                    prog.get('episode'),
                    prog.get('previously_shown')
                ))
            
            conn.commit()
            conn.close()
    
    def get_epg_for_channel(self, channel_id: str, date: str = None) -> List[dict]:
        """الحصول على EPG للقناة"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            if date:
                query = '''
                    SELECT * FROM epg_data 
                    WHERE channel_id = ? AND date(start) = date(?)
                    ORDER BY start
                '''
                c.execute(query, (channel_id, date))
            else:
                query = '''
                    SELECT * FROM epg_data 
                    WHERE channel_id = ? AND stop > datetime('now')
                    ORDER BY start
                '''
                c.execute(query, (channel_id,))
            
            programmes = [dict(row) for row in c.fetchall()]
            conn.close()
            return programmes
    
    # ============ إدارة الجلسات ============
    
    def create_session(self, user_id: int, stream_id: int = None, 
                      client_ip: str = None, user_agent: str = None) -> str:
        """إنشاء جلسة جديدة"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            session_id = hashlib.sha256(
                f"{user_id}{datetime.now()}{client_ip}".encode()
            ).hexdigest()[:32]
            
            c.execute('''
                INSERT INTO active_sessions 
                (session_id, user_id, stream_id, client_ip, user_agent)
                VALUES (?, ?, ?, ?, ?)
            ''', (session_id, user_id, stream_id, client_ip, user_agent))
            
            conn.commit()
            conn.close()
            
            self.log_action(user_id, "session_created", "New session created")
            return session_id
    
    def update_session(self, session_id: str, bytes_sent: int = 0):
        """تحديث الجلسة"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            c.execute('''
                UPDATE active_sessions 
                SET last_active = CURRENT_TIMESTAMP, 
                    bytes_sent = bytes_sent + ?
                WHERE session_id = ? AND is_active = 1
            ''', (bytes_sent, session_id))
            
            conn.commit()
            conn.close()
    
    def check_user_connections(self, user_id: int) -> Tuple[int, int]:
        """فحص اتصالات المستخدم"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            # الحصول على عدد الاتصالات النشطة
            c.execute('''
                SELECT COUNT(*) FROM active_sessions 
                WHERE user_id = ? AND is_active = 1
            ''', (user_id,))
            
            active_connections = c.fetchone()[0]
            
            # الحصول على الحد الأقصى للمستخدم
            c.execute('''
                SELECT max_connections FROM users WHERE id = ?
            ''', (user_id,))
            
            max_connections = c.fetchone()[0] or 1
            
            conn.close()
            return active_connections, max_connections
    
    # ============ الإحصائيات ============
    
    def add_statistic(self, user_id: int = None, stream_id: int = None, 
                     bytes_sent: int = 0, viewers: int = 0):
        """إضافة إحصائية"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            today = datetime.now().strftime('%Y-%m-%d')
            
            # التحقق من وجود إحصائية اليوم
            c.execute('''
                SELECT stat_id FROM statistics 
                WHERE stat_date = ? AND user_id = ? AND stream_id = ?
            ''', (today, user_id, stream_id))
            
            existing = c.fetchone()
            
            if existing:
                # تحديث الإحصائية الموجودة
                c.execute('''
                    UPDATE statistics 
                    SET total_views = total_views + ?,
                        total_bytes = total_bytes + ?,
                        peak_viewers = CASE WHEN ? > peak_viewers THEN ? ELSE peak_viewers END
                    WHERE stat_id = ?
                ''', (viewers, bytes_sent, viewers, viewers, existing[0]))
            else:
                # إضافة إحصائية جديدة
                c.execute('''
                    INSERT INTO statistics (stat_date, user_id, stream_id, total_views, total_bytes, peak_viewers)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (today, user_id, stream_id, viewers, bytes_sent, viewers))
            
            conn.commit()
            conn.close()
    
    def get_user_stats(self, user_id: int, days: int = 30) -> dict:
        """الحصول على إحصائيات المستخدم"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            # إحصائيات البث
            c.execute('''
                SELECT 
                    COUNT(DISTINCT stat_date) as days_active,
                    SUM(total_views) as total_views,
                    SUM(total_bytes) as total_bytes,
                    AVG(peak_viewers) as avg_viewers,
                    MAX(peak_viewers) as max_viewers
                FROM statistics 
                WHERE user_id = ? AND stat_date >= ?
            ''', (user_id, start_date))
            
            stats = dict(c.fetchone() or {})
            
            # عدد البثوث
            c.execute('SELECT COUNT(*) FROM live_streams WHERE user_id = ? AND is_active = 1', (user_id,))
            stats['total_streams'] = c.fetchone()[0] or 0
            
            # الاتصالات النشطة
            c.execute('SELECT COUNT(*) FROM active_sessions WHERE user_id = ? AND is_active = 1', (user_id,))
            stats['active_connections'] = c.fetchone()[0] or 0
            
            conn.close()
            return stats
    
    # ============ الإعدادات ============
    
    def get_setting(self, key: str, default: str = None) -> str:
        """الحصول على إعداد"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            c.execute('SELECT setting_value FROM settings WHERE setting_key = ?', (key,))
            result = c.fetchone()
            
            conn.close()
            return result[0] if result else default
    
    def update_setting(self, key: str, value: str):
        """تحديث إعداد"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            c.execute('''
                INSERT OR REPLACE INTO settings (setting_key, setting_value)
                VALUES (?, ?)
            ''', (key, value))
            
            conn.commit()
            conn.close()
    
    # ============ Xtream Codes API ============
    
    def get_xtream_user_info(self, username: str) -> dict:
        """الحصول على معلومات مستخدم لـ Xtream Codes"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            c.execute('''
                SELECT 
                    u.id, u.username, u.expires_at, u.max_connections,
                    COUNT(DISTINCT ls.stream_id) as total_streams,
                    COUNT(DISTINCT vs.vod_id) as total_vod,
                    COUNT(DISTINCT s.series_id) as total_series
                FROM users u
                LEFT JOIN live_streams ls ON u.id = ls.user_id AND ls.is_active = 1
                LEFT JOIN vod_streams vs ON u.id = vs.user_id AND vs.is_active = 1
                LEFT JOIN series s ON u.id = s.user_id AND s.is_active = 1
                WHERE u.username = ? AND u.is_active = 1
                GROUP BY u.id
            ''', (username,))
            
            user = c.fetchone()
            conn.close()
            
            if user:
                return {
                    "username": user['username'],
                    "password": "********",
                    "message": "تم المصادقة بنجاح",
                    "auth": 1,
                    "status": "Active",
                    "exp_date": user['expires_at'] or "2099-12-31",
                    "is_trial": 0,
                    "active_cons": 0,
                    "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "max_connections": user['max_connections'],
                    "allowed_output_formats": ["m3u8", "ts", "rtmp"],
                    "total_streams": user['total_streams'],
                    "total_vod": user['total_vod'],
                    "total_series": user['total_series']
                }
            
            return {"user_info": {"auth": 0}}
    
    # ============ توليد M3U ============
    
    def generate_m3u_playlist(self, user_id: int, output_format: str = "ts") -> str:
        """توليد قائمة M3U"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            # الحصول على البثوث الخاصة بالمستخدم
            c.execute('''
                SELECT ls.*, c.category_name 
                FROM live_streams ls
                LEFT JOIN categories c ON ls.category_id = c.category_id
                WHERE ls.user_id = ? AND ls.is_active = 1
                ORDER BY c.category_name, ls.stream_name
            ''', (user_id,))
            
            streams = c.fetchall()
            
            # توليد M3U
            m3u_content = "#EXTM3U\n"
            m3u_content += f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            m3u_content += f"# User ID: {user_id}\n\n"
            
            current_category = ""
            
            for stream in streams:
                stream_dict = dict(stream)
                
                # إضافة فئة إذا تغيرت
                if stream_dict['category_name'] != current_category:
                    m3u_content += f"\n#EXTINF:-1 group-title=\"{stream_dict['category_name']}\",{stream_dict['category_name']}\n"
                    m3u_content += f"#EXTVLCOPT:network-caching=1000\n"
                    current_category = stream_dict['category_name']
                
                # معلومات القناة
                m3u_content += f'#EXTINF:-1 tvg-id="{stream_dict["epg_channel_id"] or stream_dict["stream_name"]}" '
                m3u_content += f'tvg-name="{stream_dict["stream_name"]}" '
                m3u_content += f'tvg-logo="{stream_dict["stream_icon"]}" '
                m3u_content += f'group-title="{stream_dict["category_name"]}",{stream_dict["stream_name"]}\n'
                
                # رابط البث
                if output_format == "ts":
                    m3u_content += f"{stream_dict['proxy_url']}\n"
                else:
                    m3u_content += f"{stream_dict['stream_url']}\n"
            
            conn.close()
            return m3u_content
    
    # ============ السجلات ============
    
    def log_action(self, user_id: int = None, action: str = "", details: str = ""):
        """تسجيل إجراء"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            c.execute('''
                INSERT INTO logs (user_id, action, details)
                VALUES (?, ?, ?)
            ''', (user_id, action, details))
            
            conn.commit()
            conn.close()
    
    # ============ النسخ الاحتياطي ============
    
    def backup_database(self, backup_path: str):
        """إنشاء نسخة احتياطية من قاعدة البيانات"""
        import shutil
        shutil.copy2(self.db_path, backup_path)
        
        self.log_action(None, "database_backup", f"Backup created: {backup_path}")
    
    def restore_database(self, backup_path: str):
        """استعادة قاعدة البيانات من نسخة احتياطية"""
        import shutil
        shutil.copy2(backup_path, self.db_path)
        
        self.log_action(None, "database_restore", f"Database restored from: {backup_path}")
    
    # ============ التنظيف ============
    
    def cleanup_old_data(self, days: int = 30):
        """تنظيف البيانات القديمة"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            
            # تنظيف الجلسات القديمة
            c.execute('''
                UPDATE active_sessions 
                SET is_active = 0 
                WHERE last_active < ?
            ''', (cutoff_date,))
            
            # تنظيف السجلات القديمة
            c.execute('''
                DELETE FROM logs 
                WHERE log_date < ? AND log_level != 'error'
            ''', (cutoff_date,))
            
            conn.commit()
            conn.close()
            
            self.log_action(None, "cleanup", f"Cleaned up data older than {days} days")

# ============ فئة مساعدة للتعامل مع قاعدة البيانات ============

class DatabaseManager:
    """مدير قاعدة البيانات للاستخدام العام"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.db = IPTVDatabase()
        return cls._instance
    
    @property
    def instance(self):
        """الحصول على نسخة واحدة من قاعدة البيانات"""
        return self._instance.db if self._instance else None

# استخدام سهل
def get_db() -> IPTVDatabase:
    """الحصول على كائن قاعدة البيانات"""
    return DatabaseManager().instance

# اختبار قاعدة البيانات
if __name__ == "__main__":
    db = IPTVDatabase("test.db")
    
    # اختبار إنشاء مستخدم
    user_id = db.create_user("test", "test123", "test@example.com")
    print(f"User created with ID: {user_id}")
    
    # اختبار المصادقة
    user = db.authenticate_user("test", "test123")
    print(f"Authenticated user: {user}")
    
    # اختبار إضافة بث
    stream_id = db.add_live_stream({
        "name": "Test Channel",
        "url": "http://example.com/stream.m3u8",
        "category_id": 1,
        "category_name": "عامة",
        "user_id": user_id
    })
    print(f"Stream added with ID: {stream_id}")
    
    # اختبار توليد M3U
    m3u = db.generate_m3u_playlist(user_id)
    print(f"M3U Playlist:\n{m3u}")
    
    # تنظيف
    os.remove("test.db")
    print("Test completed successfully!")

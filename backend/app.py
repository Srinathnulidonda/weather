#backend/app.py
import os
import json
import random
import hashlib
import secrets
import requests
import redis
import time
import base64
import uuid
import traceback
from datetime import datetime, timedelta, timezone
from functools import wraps, lru_cache
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from enum import Enum
import threading
import queue
import logging
import sys

from flask import Flask, request, jsonify, g, Response
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_caching import Cache
from marshmallow import Schema, fields, validate, ValidationError, pre_load, post_dump
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, JSON, Text, ForeignKey, Index, UniqueConstraint, event, and_, or_, func
from sqlalchemy.orm import relationship, validates, deferred, joinedload, selectinload
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY, TSVECTOR
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.pool import NullPool, QueuePool
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
import jwt
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from prometheus_flask_exporter import PrometheusMetrics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler('app.log', maxBytes=10485760, backupCount=5)
    ]
)
logger = logging.getLogger(__name__)

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', secrets.token_hex(32))
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', '').replace('postgres://', 'postgresql://')
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_size': 20,
        'pool_recycle': 3600,
        'pool_pre_ping': True,
        'max_overflow': 40,
        'echo_pool': False
    }
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
    CACHE_TYPE = 'redis'
    CACHE_REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/1')
    CACHE_DEFAULT_TIMEOUT = 300
    OPENWEATHER_API_KEY = os.environ.get('OPENWEATHER_API_KEY')
    SPOTIFY_CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
    SPOTIFY_CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')
    IPAPI_KEY = os.environ.get('IPAPI_KEY')
    FIREBASE_SERVER_KEY = os.environ.get('FIREBASE_SERVER_KEY')
    SENTRY_DSN = os.environ.get('SENTRY_DSN')
    ENVIRONMENT = os.environ.get('ENVIRONMENT', 'production')
    API_VERSION = 'v1'
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024
    JWT_EXPIRATION_DAYS = 7
    JWT_REFRESH_EXPIRATION_DAYS = 30
    BCRYPT_LOG_ROUNDS = 12
    RATE_LIMIT_STORAGE_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/2')

app = Flask(__name__)
app.config.from_object(Config)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

db = SQLAlchemy(app)
migrate = Migrate(app, db)
cache = Cache(app)
metrics = PrometheusMetrics(app)

CORS(app, 
     origins=os.environ.get('ALLOWED_ORIGINS', '*').split(','),
     supports_credentials=True,
     max_age=3600,
     methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
     allow_headers=['Content-Type', 'Authorization', 'X-Request-ID', 'X-Client-Version'])

limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri=app.config['RATE_LIMIT_STORAGE_URL']
)

if app.config['SENTRY_DSN']:
    sentry_sdk.init(
        dsn=app.config['SENTRY_DSN'],
        integrations=[FlaskIntegration()],
        traces_sample_rate=0.1,
        environment=app.config['ENVIRONMENT']
    )

redis_client = redis.from_url(app.config['REDIS_URL'], decode_responses=True)

class WeatherCondition(Enum):
    CLEAR = 'clear'
    CLOUDS = 'clouds'
    RAIN = 'rain'
    DRIZZLE = 'drizzle'
    THUNDERSTORM = 'thunderstorm'
    SNOW = 'snow'
    MIST = 'mist'
    FOG = 'fog'
    HAZE = 'haze'
    DUST = 'dust'
    SAND = 'sand'
    ASH = 'ash'
    SQUALL = 'squall'
    TORNADO = 'tornado'

class AlertSeverity(Enum):
    LOW = 'low'
    MEDIUM = 'medium'
    HIGH = 'high'
    CRITICAL = 'critical'

class TriggerCondition(Enum):
    TEMP_BELOW = 'temp_below'
    TEMP_ABOVE = 'temp_above'
    WIND_ABOVE = 'wind_above'
    RAIN_STARTS = 'rain_starts'
    HUMIDITY_ABOVE = 'humidity_above'
    UV_ABOVE = 'uv_above'
    PRESSURE_DROPS = 'pressure_drops'

@dataclass
class WeatherData:
    temperature: float
    feels_like: float
    humidity: int
    pressure: int
    wind_speed: float
    wind_direction: int
    visibility: float
    uv_index: float
    condition: str
    description: str
    icon: str
    clouds: int
    rain: Optional[float] = None
    snow: Optional[float] = None

@dataclass
class LocationData:
    latitude: float
    longitude: float
    city: str
    country: str
    timezone: Optional[str] = None
    region: Optional[str] = None

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60, expected_exception=Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'closed'
        self._lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        with self._lock:
            if self.state == 'open':
                if datetime.now().timestamp() - self.last_failure_time > self.recovery_timeout:
                    self.state = 'half-open'
                else:
                    raise Exception('Circuit breaker is open')

        try:
            result = func(*args, **kwargs)
            with self._lock:
                if self.state == 'half-open':
                    self.state = 'closed'
                    self.failure_count = 0
            return result
        except self.expected_exception as e:
            with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.now().timestamp()
                if self.failure_count >= self.failure_threshold:
                    self.state = 'open'
            raise e

weather_api_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30)
spotify_api_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

class User(db.Model):
    __tablename__ = 'users'
    __table_args__ = (
        Index('idx_user_email', 'email'),
        Index('idx_user_username', 'username'),
        Index('idx_user_active', 'is_active'),
        {'schema': 'public'}
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False)
    username = Column(String(80), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    is_verified = Column(Boolean, default=False, nullable=False)
    is_premium = Column(Boolean, default=False, nullable=False)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    last_active = Column(DateTime(timezone=True))
    email_verified_at = Column(DateTime(timezone=True))
    
    streak_count = Column(Integer, default=0, nullable=False)
    max_streak = Column(Integer, default=0, nullable=False)
    total_checks = Column(Integer, default=0, nullable=False)
    
    preferences = Column(JSONB, default=dict, nullable=False)
    metadata = Column(JSONB, default=dict)
    feature_flags = Column(JSONB, default=dict)
    
    notification_token = Column(String(500))
    api_key = Column(String(64), unique=True)
    refresh_token = Column(String(500))
    
    alerts = relationship('WeatherAlert', back_populates='user', lazy='dynamic', cascade='all, delete-orphan')
    logs = relationship('WeatherLog', back_populates='user', lazy='dynamic', cascade='all, delete-orphan')
    triggers = relationship('CustomTrigger', back_populates='user', lazy='dynamic', cascade='all, delete-orphan')
    sessions = relationship('UserSession', back_populates='user', lazy='dynamic', cascade='all, delete-orphan')
    
    @validates('email')
    def validate_email(self, key, email):
        if not email or '@' not in email:
            raise ValueError('Invalid email address')
        return email.lower()
    
    @hybrid_property
    def is_streak_active(self):
        if not self.last_active:
            return False
        return (datetime.now(timezone.utc) - self.last_active).days <= 1
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
    
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    def generate_api_key(self):
        self.api_key = secrets.token_urlsafe(48)
        return self.api_key
    
    def update_streak(self):
        now = datetime.now(timezone.utc)
        if self.last_active:
            days_diff = (now.date() - self.last_active.date()).days
            if days_diff == 1:
                self.streak_count += 1
                self.max_streak = max(self.max_streak, self.streak_count)
            elif days_diff > 1:
                self.streak_count = 1
        else:
            self.streak_count = 1
        self.last_active = now
        self.total_checks += 1

class UserSession(db.Model):
    __tablename__ = 'user_sessions'
    __table_args__ = (
        Index('idx_session_token', 'token'),
        Index('idx_session_user', 'user_id'),
        Index('idx_session_expires', 'expires_at'),
        {'schema': 'public'}
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    token = Column(String(500), unique=True, nullable=False)
    refresh_token = Column(String(500), unique=True)
    
    ip_address = Column(String(45))
    user_agent = Column(String(500))
    device_id = Column(String(255))
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True))
    last_used = Column(DateTime(timezone=True))
    
    is_active = Column(Boolean, default=True)
    
    user = relationship('User', back_populates='sessions')

class WeatherAlert(db.Model):
    __tablename__ = 'weather_alerts'
    __table_args__ = (
        Index('idx_alert_user', 'user_id'),
        Index('idx_alert_created', 'created_at'),
        Index('idx_alert_read', 'is_read'),
        {'schema': 'public'}
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    
    alert_type = Column(String(50), nullable=False)
    severity = Column(String(20), nullable=False)
    title = Column(String(255), nullable=False)
    message = Column(Text, nullable=False)
    
    location = Column(JSONB)
    weather_data = Column(JSONB)
    metadata = Column(JSONB)
    
    is_read = Column(Boolean, default=False, nullable=False)
    is_pushed = Column(Boolean, default=False)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    read_at = Column(DateTime(timezone=True))
    expires_at = Column(DateTime(timezone=True))
    
    user = relationship('User', back_populates='alerts')

class WeatherLog(db.Model):
    __tablename__ = 'weather_logs'
    __table_args__ = (
        Index('idx_log_user', 'user_id'),
        Index('idx_log_created', 'created_at'),
        Index('idx_log_location', 'location_hash'),
        {'schema': 'public'}
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    
    location = Column(JSONB, nullable=False)
    location_hash = Column(String(64))
    weather_data = Column(JSONB, nullable=False)
    
    mood = Column(String(50))
    note = Column(Text)
    tags = Column(ARRAY(String))
    
    photos = Column(ARRAY(String))
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    user = relationship('User', back_populates='logs')
    
    def set_location(self, location_data):
        self.location = location_data
        self.location_hash = hashlib.sha256(json.dumps(location_data, sort_keys=True).encode()).hexdigest()

class CustomTrigger(db.Model):
    __tablename__ = 'custom_triggers'
    __table_args__ = (
        Index('idx_trigger_user', 'user_id'),
        Index('idx_trigger_active', 'is_active'),
        UniqueConstraint('user_id', 'name', name='unique_user_trigger_name'),
        {'schema': 'public'}
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    
    name = Column(String(100), nullable=False)
    description = Column(Text)
    
    condition_type = Column(String(50), nullable=False)
    condition_value = Column(Float)
    condition_operator = Column(String(10))
    
    location = Column(JSONB)
    schedule = Column(JSONB)
    
    is_active = Column(Boolean, default=True, nullable=False)
    is_recurring = Column(Boolean, default=True)
    
    last_triggered = Column(DateTime(timezone=True))
    trigger_count = Column(Integer, default=0)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), onupdate=lambda: datetime.now(timezone.utc))
    
    user = relationship('User', back_populates='triggers')

class WeatherCache(db.Model):
    __tablename__ = 'weather_cache'
    __table_args__ = (
        Index('idx_cache_location', 'location_hash'),
        Index('idx_cache_expires', 'expires_at'),
        {'schema': 'public'}
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    location_hash = Column(String(64), unique=True, nullable=False)
    location = Column(JSONB, nullable=False)
    weather_data = Column(JSONB, nullable=False)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True))

class AuditLog(db.Model):
    __tablename__ = 'audit_logs'
    __table_args__ = (
        Index('idx_audit_user', 'user_id'),
        Index('idx_audit_created', 'created_at'),
        Index('idx_audit_action', 'action'),
        {'schema': 'public'}
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True))
    
    action = Column(String(100), nullable=False)
    resource_type = Column(String(50))
    resource_id = Column(String(255))
    
    ip_address = Column(String(45))
    user_agent = Column(String(500))
    
    request_data = Column(JSONB)
    response_data = Column(JSONB)
    
    status_code = Column(Integer)
    duration_ms = Column(Integer)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class UserSchema(Schema):
    id = fields.UUID(dump_only=True)
    email = fields.Email(required=True, validate=validate.Length(max=255))
    username = fields.Str(required=True, validate=validate.Length(min=3, max=80))
    password = fields.Str(required=True, load_only=True, validate=validate.Length(min=8))
    is_active = fields.Bool(dump_only=True)
    is_verified = fields.Bool(dump_only=True)
    is_premium = fields.Bool(dump_only=True)
    created_at = fields.DateTime(dump_only=True)
    last_active = fields.DateTime(dump_only=True)
    streak_count = fields.Int(dump_only=True)
    preferences = fields.Dict()
    
    @pre_load
    def process_input(self, data, **kwargs):
        if 'email' in data:
            data['email'] = data['email'].lower().strip()
        if 'username' in data:
            data['username'] = data['username'].strip()
        return data

class LocationSchema(Schema):
    latitude = fields.Float(required=True, validate=validate.Range(min=-90, max=90))
    longitude = fields.Float(required=True, validate=validate.Range(min=-180, max=180))
    city = fields.Str(validate=validate.Length(max=100))
    country = fields.Str(validate=validate.Length(max=100))

class WeatherRequestSchema(Schema):
    lat = fields.Float(validate=validate.Range(min=-90, max=90))
    lon = fields.Float(validate=validate.Range(min=-180, max=180))
    city = fields.Str(validate=validate.Length(max=100))
    units = fields.Str(validate=validate.OneOf(['metric', 'imperial']), missing='metric')
    lang = fields.Str(validate=validate.Length(max=5), missing='en')

class TriggerSchema(Schema):
    name = fields.Str(required=True, validate=validate.Length(min=1, max=100))
    description = fields.Str(validate=validate.Length(max=500))
    condition_type = fields.Str(required=True, validate=validate.OneOf([e.value for e in TriggerCondition]))
    condition_value = fields.Float()
    location = fields.Nested(LocationSchema)
    is_active = fields.Bool(missing=True)

class CustomException(Exception):
    def __init__(self, message, status_code=400, payload=None):
        super().__init__()
        self.message = message
        self.status_code = status_code
        self.payload = payload

class AuthenticationError(CustomException):
    def __init__(self, message='Authentication failed'):
        super().__init__(message, 401)

class AuthorizationError(CustomException):
    def __init__(self, message='Insufficient permissions'):
        super().__init__(message, 403)

class ValidationError(CustomException):
    def __init__(self, message='Validation failed', errors=None):
        super().__init__(message, 422, errors)

class ResourceNotFoundError(CustomException):
    def __init__(self, message='Resource not found'):
        super().__init__(message, 404)

class RateLimitError(CustomException):
    def __init__(self, message='Rate limit exceeded'):
        super().__init__(message, 429)

class ExternalAPIError(CustomException):
    def __init__(self, message='External API error'):
        super().__init__(message, 502)

@app.errorhandler(CustomException)
def handle_custom_exception(error):
    response = {'error': error.message}
    if error.payload:
        response['details'] = error.payload
    return jsonify(response), error.status_code

@app.errorhandler(ValidationError)
def handle_validation_error(error):
    return jsonify({'error': 'Validation failed', 'details': error.messages}), 422

@app.errorhandler(404)
def handle_not_found(error):
    return jsonify({'error': 'Resource not found'}), 404

@app.errorhandler(500)
def handle_internal_error(error):
    logger.error(f'Internal server error: {str(error)}', exc_info=True)
    return jsonify({'error': 'Internal server error', 'request_id': g.get('request_id')}), 500

@app.before_request
def before_request():
    g.request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    g.start_time = time.time()
    g.user = None
    
    if request.method != 'OPTIONS':
        logger.info(f'Request: {request.method} {request.path} - ID: {g.request_id}')

@app.after_request
def after_request(response):
    if hasattr(g, 'start_time'):
        duration = round((time.time() - g.start_time) * 1000, 2)
        response.headers['X-Response-Time'] = f'{duration}ms'
    
    response.headers['X-Request-ID'] = g.get('request_id', '')
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['Content-Security-Policy'] = "default-src 'self'"
    
    if request.method != 'OPTIONS':
        logger.info(f'Response: {response.status_code} - ID: {g.request_id} - Time: {duration}ms')
    
    return response

def require_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        if not auth_header:
            raise AuthenticationError('Missing authorization header')
        
        try:
            scheme, token = auth_header.split(' ', 1)
            if scheme.lower() != 'bearer':
                raise AuthenticationError('Invalid authentication scheme')
            
            payload = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            
            session = UserSession.query.filter_by(
                token=token,
                is_active=True
            ).first()
            
            if not session or session.expires_at < datetime.now(timezone.utc):
                raise AuthenticationError('Invalid or expired token')
            
            g.user = User.query.get(payload['user_id'])
            if not g.user or not g.user.is_active:
                raise AuthenticationError('User account is inactive')
            
            session.last_used = datetime.now(timezone.utc)
            db.session.commit()
            
        except jwt.ExpiredSignatureError:
            raise AuthenticationError('Token has expired')
        except jwt.InvalidTokenError:
            raise AuthenticationError('Invalid token')
        except Exception as e:
            logger.error(f'Authentication error: {str(e)}')
            raise AuthenticationError()
        
        return f(*args, **kwargs)
    return decorated_function

def require_premium(f):
    @wraps(f)
    @require_auth
    def decorated_function(*args, **kwargs):
        if not g.user.is_premium:
            raise AuthorizationError('Premium subscription required')
        return f(*args, **kwargs)
    return decorated_function

def validate_request(schema_class):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            schema = schema_class()
            try:
                if request.method in ['POST', 'PUT', 'PATCH']:
                    data = schema.load(request.json or {})
                else:
                    data = schema.load(request.args.to_dict())
                g.validated_data = data
            except ValidationError as e:
                raise ValidationError('Invalid request data', errors=e.messages)
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def audit_log(action, resource_type=None):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            start_time = time.time()
            response = None
            status_code = 200
            
            try:
                response = f(*args, **kwargs)
                if isinstance(response, tuple):
                    status_code = response[1]
                return response
            finally:
                duration_ms = int((time.time() - start_time) * 1000)
                
                log_entry = AuditLog(
                    user_id=g.user.id if g.user else None,
                    action=action,
                    resource_type=resource_type,
                    resource_id=kwargs.get('id'),
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent'),
                    request_data=request.json if request.method in ['POST', 'PUT', 'PATCH'] else None,
                    status_code=status_code,
                    duration_ms=duration_ms
                )
                db.session.add(log_entry)
                db.session.commit()
                
        return decorated_function
    return decorator

@cache.memoize(timeout=300)
def get_spotify_token():
    cache_key = 'spotify:token'
    cached_token = redis_client.get(cache_key)
    
    if cached_token:
        return cached_token
    
    try:
        auth_str = f"{app.config['SPOTIFY_CLIENT_ID']}:{app.config['SPOTIFY_CLIENT_SECRET']}"
        auth_bytes = auth_str.encode('ascii')
        auth_base64 = base64.b64encode(auth_bytes).decode('ascii')
        
        headers = {
            'Authorization': f'Basic {auth_base64}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        response = requests.post(
            'https://accounts.spotify.com/api/token',
            headers=headers,
            data={'grant_type': 'client_credentials'},
            timeout=10
        )
        
        if response.status_code == 200:
            token_data = response.json()
            token = token_data['access_token']
            expires_in = token_data.get('expires_in', 3600)
            
            redis_client.setex(cache_key, expires_in - 60, token)
            return token
            
    except Exception as e:
        logger.error(f'Spotify token error: {str(e)}')
        
    return None

@cache.memoize(timeout=600)
def get_user_location_from_ip(ip_address):
    cache_key = f'location:ip:{ip_address}'
    cached_location = redis_client.get(cache_key)
    
    if cached_location:
        return json.loads(cached_location)
    
    try:
        if ip_address in ['127.0.0.1', 'localhost']:
            location = {
                'latitude': 40.7128,
                'longitude': -74.0060,
                'city': 'New York',
                'country': 'United States'
            }
        else:
            response = requests.get(
                f'http://ip-api.com/json/{ip_address}',
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'success':
                    location = {
                        'latitude': data['lat'],
                        'longitude': data['lon'],
                        'city': data['city'],
                        'country': data['country'],
                        'region': data.get('regionName'),
                        'timezone': data.get('timezone')
                    }
                else:
                    raise Exception('IP geolocation failed')
            else:
                raise Exception('IP API request failed')
        
        redis_client.setex(cache_key, 3600, json.dumps(location))
        return location
        
    except Exception as e:
        logger.error(f'IP geolocation error: {str(e)}')
        return {
            'latitude': 40.7128,
            'longitude': -74.0060,
            'city': 'New York',
            'country': 'United States'
        }

def fetch_weather_data(lat, lon, units='metric', lang='en'):
    cache_key = f'weather:{lat}:{lon}:{units}:{lang}'
    cached_data = redis_client.get(cache_key)
    
    if cached_data:
        return json.loads(cached_data)
    
    def _make_request():
        url = f"https://api.openweathermap.org/data/2.5/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': app.config['OPENWEATHER_API_KEY'],
            'units': units,
            'lang': lang
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    
    try:
        data = weather_api_breaker.call(_make_request)
        redis_client.setex(cache_key, 300, json.dumps(data))
        return data
    except Exception as e:
        logger.error(f'Weather API error: {str(e)}')
        raise ExternalAPIError('Weather service temporarily unavailable')

def fetch_forecast_data(lat, lon, units='metric', lang='en'):
    cache_key = f'forecast:{lat}:{lon}:{units}:{lang}'
    cached_data = redis_client.get(cache_key)
    
    if cached_data:
        return json.loads(cached_data)
    
    def _make_request():
        url = f"https://api.openweathermap.org/data/2.5/onecall"
        params = {
            'lat': lat,
            'lon': lon,
            'exclude': 'minutely',
            'appid': app.config['OPENWEATHER_API_KEY'],
            'units': units,
            'lang': lang
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    
    try:
        data = weather_api_breaker.call(_make_request)
        redis_client.setex(cache_key, 900, json.dumps(data))
        return data
    except Exception as e:
        logger.error(f'Forecast API error: {str(e)}')
        raise ExternalAPIError('Forecast service temporarily unavailable')

def get_weather_playlist(condition):
    playlist_map = {
        WeatherCondition.CLEAR: {'name': 'Sunny Vibes', 'query': 'sunny day playlist'},
        WeatherCondition.CLOUDS: {'name': 'Cloudy Chill', 'query': 'cloudy day music'},
        WeatherCondition.RAIN: {'name': 'Rainy Mood', 'query': 'rain sounds lofi'},
        WeatherCondition.THUNDERSTORM: {'name': 'Storm Energy', 'query': 'thunderstorm epic'},
        WeatherCondition.SNOW: {'name': 'Winter Wonder', 'query': 'cozy winter playlist'},
        WeatherCondition.MIST: {'name': 'Misty Morning', 'query': 'ambient fog music'}
    }
    
    playlist_info = playlist_map.get(
        WeatherCondition(condition.lower()) if condition.lower() in [e.value for e in WeatherCondition] else WeatherCondition.CLEAR,
        playlist_map[WeatherCondition.CLEAR]
    )
    
    token = get_spotify_token()
    if not token:
        return playlist_info
    
    try:
        def _make_request():
            headers = {'Authorization': f'Bearer {token}'}
            response = requests.get(
                'https://api.spotify.com/v1/search',
                headers=headers,
                params={'q': playlist_info['query'], 'type': 'playlist', 'limit': 1},
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        
        data = spotify_api_breaker.call(_make_request)
        
        if data['playlists']['items']:
            playlist = data['playlists']['items'][0]
            playlist_info.update({
                'spotify_url': playlist['external_urls']['spotify'],
                'id': playlist['id'],
                'image': playlist['images'][0]['url'] if playlist['images'] else None
            })
            
    except Exception as e:
        logger.error(f'Spotify API error: {str(e)}')
    
    return playlist_info

def get_weather_sounds(condition):
    sound_map = {
        WeatherCondition.CLEAR: {
            'name': 'Birds Chirping',
            'url': 'https://cdn.example.com/sounds/birds.mp3',
            'description': 'Nature sounds for sunny days'
        },
        WeatherCondition.RAIN: {
            'name': 'Rain Ambience',
            'url': 'https://cdn.example.com/sounds/rain.mp3',
            'description': 'Gentle rain for relaxation'
        },
        WeatherCondition.THUNDERSTORM: {
            'name': 'Thunder Storm',
            'url': 'https://cdn.example.com/sounds/thunder.mp3',
            'description': 'Dramatic storm sounds'
        },
        WeatherCondition.SNOW: {
            'name': 'Winter Fireplace',
            'url': 'https://cdn.example.com/sounds/fireplace.mp3',
            'description': 'Cozy fireplace crackling'
        },
        WeatherCondition.MIST: {
            'name': 'Ocean Waves',
            'url': 'https://cdn.example.com/sounds/ocean.mp3',
            'description': 'Calming ocean ambience'
        }
    }
    
    return sound_map.get(
        WeatherCondition(condition.lower()) if condition.lower() in [e.value for e in WeatherCondition] else WeatherCondition.CLEAR,
        sound_map[WeatherCondition.CLEAR]
    )

@lru_cache(maxsize=100)
def get_activity_suggestions(temp, condition, wind_speed):
    suggestions = []
    
    if condition in ['clear', 'clouds'] and 15 <= temp <= 28:
        suggestions.extend([
            {'icon': '🚴', 'activity': 'Cycling', 'description': 'Perfect weather for a bike ride'},
            {'icon': '🏃', 'activity': 'Running', 'description': 'Great conditions for outdoor exercise'},
            {'icon': '🧺', 'activity': 'Picnic', 'description': 'Ideal day for outdoor dining'},
            {'icon': '📸', 'activity': 'Photography', 'description': 'Excellent lighting conditions'}
        ])
    elif 'rain' in condition.lower():
        suggestions.extend([
            {'icon': '📚', 'activity': 'Reading', 'description': 'Cozy up with a good book'},
            {'icon': '☕', 'activity': 'Café Visit', 'description': 'Perfect for indoor socializing'},
            {'icon': '🎬', 'activity': 'Movie Marathon', 'description': 'Great day for entertainment'},
            {'icon': '🎨', 'activity': 'Creative Projects', 'description': 'Time for indoor hobbies'}
        ])
    elif temp > 30:
        suggestions.extend([
            {'icon': '🏊', 'activity': 'Swimming', 'description': 'Cool off in the water'},
            {'icon': '🍦', 'activity': 'Ice Cream', 'description': 'Treat yourself to something cold'},
            {'icon': '🏖️', 'activity': 'Beach Day', 'description': 'Head to the coast'},
            {'icon': '🌊', 'activity': 'Water Sports', 'description': 'Try surfing or paddleboarding'}
        ])
    elif temp < 10:
        suggestions.extend([
            {'icon': '🧣', 'activity': 'Winter Walk', 'description': 'Bundle up for fresh air'},
            {'icon': '🍲', 'activity': 'Cooking', 'description': 'Make warm comfort food'},
            {'icon': '🏋️', 'activity': 'Indoor Gym', 'description': 'Stay active indoors'},
            {'icon': '🎮', 'activity': 'Gaming', 'description': 'Perfect weather for indoor fun'}
        ])
    
    if wind_speed > 20:
        suggestions.append({'icon': '🪁', 'activity': 'Kite Flying', 'description': 'Great wind conditions'})
    
    return random.sample(suggestions, min(3, len(suggestions)))

def get_weather_fun_facts():
    facts = [
        {'emoji': '🌡️', 'fact': 'The highest temperature ever recorded was 134°F (56.7°C) in Death Valley!'},
        {'emoji': '❄️', 'fact': 'Antarctica recorded the coldest temperature: -128.6°F (-89.2°C)!'},
        {'emoji': '⚡', 'fact': 'Lightning strikes Earth 100 times per second globally!'},
        {'emoji': '🌪️', 'fact': 'The fastest tornado wind speed recorded was 301 mph!'},
        {'emoji': '🌧️', 'fact': 'One inch of rain equals approximately 10 inches of snow!'},
        {'emoji': '🌈', 'fact': 'Rainbows are actually full circles, but we only see half from the ground!'},
        {'emoji': '☁️', 'fact': 'A cumulus cloud can weigh as much as 100 elephants!'},
        {'emoji': '🦗', 'fact': 'Count cricket chirps for 14 seconds and add 40 to estimate temperature in °F!'},
        {'emoji': '❄️', 'fact': 'Every snowflake has exactly 6 sides due to water molecule structure!'},
        {'emoji': '🌊', 'fact': 'Tsunamis can travel at speeds up to 500 mph in deep ocean!'}
    ]
    return random.choice(facts)

def check_and_trigger_alerts(user, weather_data):
    triggers = CustomTrigger.query.filter_by(
        user_id=user.id,
        is_active=True
    ).all()
    
    for trigger in triggers:
        should_trigger = False
        current_value = None
        
        if trigger.condition_type == TriggerCondition.TEMP_BELOW.value:
            current_value = weather_data.get('main', {}).get('temp')
            if current_value and current_value < trigger.condition_value:
                should_trigger = True
                
        elif trigger.condition_type == TriggerCondition.TEMP_ABOVE.value:
            current_value = weather_data.get('main', {}).get('temp')
            if current_value and current_value > trigger.condition_value:
                should_trigger = True
                
        elif trigger.condition_type == TriggerCondition.WIND_ABOVE.value:
            current_value = weather_data.get('wind', {}).get('speed')
            if current_value and current_value > trigger.condition_value:
                should_trigger = True
                
        elif trigger.condition_type == TriggerCondition.RAIN_STARTS.value:
            weather_condition = weather_data.get('weather', [{}])[0].get('main', '').lower()
            if 'rain' in weather_condition:
                should_trigger = True
                current_value = 'Raining'
        
        if should_trigger:
            if trigger.last_triggered:
                time_diff = datetime.now(timezone.utc) - trigger.last_triggered
                if time_diff.total_seconds() < 3600:
                    continue
            
            alert = WeatherAlert(
                user_id=user.id,
                alert_type='custom_trigger',
                severity=AlertSeverity.MEDIUM.value,
                title=f'Weather Alert: {trigger.name}',
                message=f'{trigger.description or trigger.name} triggered. Current value: {current_value}',
                location=trigger.location,
                weather_data=weather_data,
                metadata={'trigger_id': str(trigger.id), 'value': current_value}
            )
            
            db.session.add(alert)
            trigger.last_triggered = datetime.now(timezone.utc)
            trigger.trigger_count += 1
            
    db.session.commit()

def send_push_notification(user, title, body, data=None):
    if not user.notification_token or not app.config['FIREBASE_SERVER_KEY']:
        return False
    
    try:
        headers = {
            'Authorization': f'key={app.config["FIREBASE_SERVER_KEY"]}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'to': user.notification_token,
            'notification': {
                'title': title,
                'body': body,
                'icon': '/icon-192x192.png',
                'badge': '/badge-72x72.png'
            },
            'data': data or {},
            'priority': 'high'
        }
        
        response = requests.post(
            'https://fcm.googleapis.com/fcm/send',
            headers=headers,
            json=payload,
            timeout=10
        )
        
        return response.status_code == 200
        
    except Exception as e:
        logger.error(f'Push notification error: {str(e)}')
        return False

@app.route('/health', methods=['GET'])
def health_check():
    health_status = {
        'status': 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'version': app.config['API_VERSION'],
        'environment': app.config['ENVIRONMENT']
    }
    
    try:
        db.session.execute('SELECT 1')
        health_status['database'] = 'connected'
    except:
        health_status['database'] = 'disconnected'
        health_status['status'] = 'degraded'
    
    try:
        redis_client.ping()
        health_status['cache'] = 'connected'
    except:
        health_status['cache'] = 'disconnected'
        health_status['status'] = 'degraded'
    
    status_code = 200 if health_status['status'] == 'healthy' else 503
    return jsonify(health_status), status_code

@app.route('/metrics', methods=['GET'])
def metrics_endpoint():
    return Response(metrics.generate_latest(), mimetype='text/plain')

@app.route('/api/v1/auth/register', methods=['POST'])
@limiter.limit("5 per hour")
@validate_request(UserSchema)
@audit_log('user.register', 'user')
def register():
    data = g.validated_data
    
    existing_user = User.query.filter(
        or_(User.email == data['email'], User.username == data['username'])
    ).first()
    
    if existing_user:
        if existing_user.email == data['email']:
            raise ValidationError('Email already registered')
        else:
            raise ValidationError('Username already taken')
    
    user = User(
        email=data['email'],
        username=data['username'],
        preferences={
            'units': {'temperature': 'celsius', 'wind': 'km/h'},
            'theme': 'auto',
            'language': 'en',
            'notifications': {'alerts': True, 'daily': True},
            'favorite_locations': []
        }
    )
    user.set_password(data['password'])
    user.generate_api_key()
    
    db.session.add(user)
    db.session.commit()
    
    token_payload = {
        'user_id': str(user.id),
        'email': user.email,
        'exp': datetime.utcnow() + timedelta(days=app.config['JWT_EXPIRATION_DAYS'])
    }
    token = jwt.encode(token_payload, app.config['SECRET_KEY'], algorithm='HS256')
    
    refresh_payload = {
        'user_id': str(user.id),
        'type': 'refresh',
        'exp': datetime.utcnow() + timedelta(days=app.config['JWT_REFRESH_EXPIRATION_DAYS'])
    }
    refresh_token = jwt.encode(refresh_payload, app.config['SECRET_KEY'], algorithm='HS256')
    
    session = UserSession(
        user_id=user.id,
        token=token,
        refresh_token=refresh_token,
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent'),
        expires_at=datetime.now(timezone.utc) + timedelta(days=app.config['JWT_EXPIRATION_DAYS'])
    )
    
    db.session.add(session)
    db.session.commit()
    
    return jsonify({
        'user': UserSchema().dump(user),
        'access_token': token,
        'refresh_token': refresh_token,
        'expires_in': app.config['JWT_EXPIRATION_DAYS'] * 86400
    }), 201

@app.route('/api/v1/auth/login', methods=['POST'])
@limiter.limit("10 per hour")
@audit_log('user.login', 'user')
def login():
    data = request.json
    
    if not data or not data.get('email') or not data.get('password'):
        raise ValidationError('Email and password required')
    
    user = User.query.filter_by(email=data['email'].lower()).first()
    
    if not user or not user.check_password(data['password']):
        raise AuthenticationError('Invalid credentials')
    
    if not user.is_active:
        raise AuthenticationError('Account is disabled')
    
    user.update_streak()
    db.session.commit()
    
    token_payload = {
        'user_id': str(user.id),
        'email': user.email,
        'exp': datetime.utcnow() + timedelta(days=app.config['JWT_EXPIRATION_DAYS'])
    }
    token = jwt.encode(token_payload, app.config['SECRET_KEY'], algorithm='HS256')
    
    refresh_payload = {
        'user_id': str(user.id),
        'type': 'refresh',
        'exp': datetime.utcnow() + timedelta(days=app.config['JWT_REFRESH_EXPIRATION_DAYS'])
    }
    refresh_token = jwt.encode(refresh_payload, app.config['SECRET_KEY'], algorithm='HS256')
    
    UserSession.query.filter_by(user_id=user.id, is_active=True).update({'is_active': False})
    
    session = UserSession(
        user_id=user.id,
        token=token,
        refresh_token=refresh_token,
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent'),
        device_id=data.get('device_id'),
        expires_at=datetime.now(timezone.utc) + timedelta(days=app.config['JWT_EXPIRATION_DAYS'])
    )
    
    db.session.add(session)
    db.session.commit()
    
    return jsonify({
        'user': UserSchema().dump(user),
        'access_token': token,
        'refresh_token': refresh_token,
        'expires_in': app.config['JWT_EXPIRATION_DAYS'] * 86400
    })

@app.route('/api/v1/auth/refresh', methods=['POST'])
@limiter.limit("20 per hour")
def refresh_token():
    data = request.json
    
    if not data or not data.get('refresh_token'):
        raise ValidationError('Refresh token required')
    
    try:
        payload = jwt.decode(data['refresh_token'], app.config['SECRET_KEY'], algorithms=['HS256'])
        
        if payload.get('type') != 'refresh':
            raise AuthenticationError('Invalid token type')
        
        user = User.query.get(payload['user_id'])
        if not user or not user.is_active:
            raise AuthenticationError('User not found or inactive')
        
        session = UserSession.query.filter_by(
            user_id=user.id,
            refresh_token=data['refresh_token'],
            is_active=True
        ).first()
        
        if not session:
            raise AuthenticationError('Invalid refresh token')
        
        new_token_payload = {
            'user_id': str(user.id),
            'email': user.email,
            'exp': datetime.utcnow() + timedelta(days=app.config['JWT_EXPIRATION_DAYS'])
        }
        new_token = jwt.encode(new_token_payload, app.config['SECRET_KEY'], algorithm='HS256')
        
        session.token = new_token
        session.last_used = datetime.now(timezone.utc)
        db.session.commit()
        
        return jsonify({
            'access_token': new_token,
            'expires_in': app.config['JWT_EXPIRATION_DAYS'] * 86400
        })
        
    except jwt.ExpiredSignatureError:
        raise AuthenticationError('Refresh token has expired')
    except jwt.InvalidTokenError:
        raise AuthenticationError('Invalid refresh token')

@app.route('/api/v1/auth/logout', methods=['POST'])
@require_auth
def logout():
    auth_header = request.headers.get('Authorization')
    token = auth_header.split(' ', 1)[1] if ' ' in auth_header else auth_header
    
    UserSession.query.filter_by(token=token).update({'is_active': False})
    db.session.commit()
    
    return jsonify({'message': 'Logged out successfully'})

@app.route('/api/v1/weather/current', methods=['GET'])
@limiter.limit("100 per hour")
@validate_request(WeatherRequestSchema)
def get_current_weather():
    data = g.validated_data
    
    if data.get('lat') and data.get('lon'):
        lat, lon = data['lat'], data['lon']
        location_name = f"{lat},{lon}"
    elif data.get('city'):
        geocode_url = f"http://api.openweathermap.org/geo/1.0/direct"
        params = {'q': data['city'], 'limit': 1, 'appid': app.config['OPENWEATHER_API_KEY']}
        
        try:
            response = requests.get(geocode_url, params=params, timeout=5)
            response.raise_for_status()
            geo_data = response.json()
            
            if not geo_data:
                raise ResourceNotFoundError('City not found')
            
            lat, lon = geo_data[0]['lat'], geo_data[0]['lon']
            location_name = data['city']
        except requests.RequestException:
            raise ExternalAPIError('Geocoding service unavailable')
    else:
        location = get_user_location_from_ip(request.remote_addr)
        lat, lon = location['latitude'], location['longitude']
        location_name = location['city']
    
    weather_data = fetch_weather_data(lat, lon, data['units'], data['lang'])
    
    uv_url = f"https://api.openweathermap.org/data/2.5/uvi"
    uv_params = {'lat': lat, 'lon': lon, 'appid': app.config['OPENWEATHER_API_KEY']}
    
    try:
        uv_response = requests.get(uv_url, params=uv_params, timeout=5)
        uv_index = uv_response.json().get('value', 0) if uv_response.status_code == 200 else 0
    except:
        uv_index = 0
    
    result = {
        'location': {
            'name': weather_data.get('name', location_name),
            'country': weather_data.get('sys', {}).get('country'),
            'coordinates': {'lat': lat, 'lon': lon},
            'timezone': weather_data.get('timezone')
        },
        'current': {
            'temperature': weather_data['main']['temp'],
            'feels_like': weather_data['main']['feels_like'],
            'temp_min': weather_data['main']['temp_min'],
            'temp_max': weather_data['main']['temp_max'],
            'pressure': weather_data['main']['pressure'],
            'humidity': weather_data['main']['humidity'],
            'visibility': weather_data.get('visibility', 10000),
            'uv_index': uv_index,
            'clouds': weather_data['clouds']['all'],
            'wind': {
                'speed': weather_data['wind']['speed'],
                'direction': weather_data['wind'].get('deg', 0),
                'gust': weather_data['wind'].get('gust')
            },
            'condition': weather_data['weather'][0]['main'],
            'description': weather_data['weather'][0]['description'],
            'icon': weather_data['weather'][0]['icon'],
            'sunrise': weather_data['sys']['sunrise'],
            'sunset': weather_data['sys']['sunset']
        },
        'activities': get_activity_suggestions(
            weather_data['main']['temp'],
            weather_data['weather'][0]['main'],
            weather_data['wind']['speed']
        ),
        'playlist': get_weather_playlist(weather_data['weather'][0]['main']),
        'sounds': get_weather_sounds(weather_data['weather'][0]['main']),
        'fun_fact': get_weather_fun_facts(),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    if g.user:
        check_and_trigger_alerts(g.user, weather_data)
    
    return jsonify(result)

@app.route('/api/v1/weather/forecast', methods=['GET'])
@limiter.limit("50 per hour")
@validate_request(WeatherRequestSchema)
def get_forecast():
    data = g.validated_data
    
    if data.get('lat') and data.get('lon'):
        lat, lon = data['lat'], data['lon']
    elif data.get('city'):
        geocode_url = f"http://api.openweathermap.org/geo/1.0/direct"
        params = {'q': data['city'], 'limit': 1, 'appid': app.config['OPENWEATHER_API_KEY']}
        
        try:
            response = requests.get(geocode_url, params=params, timeout=5)
            response.raise_for_status()
            geo_data = response.json()
            
            if not geo_data:
                raise ResourceNotFoundError('City not found')
            
            lat, lon = geo_data[0]['lat'], geo_data[0]['lon']
        except requests.RequestException:
            raise ExternalAPIError('Geocoding service unavailable')
    else:
        location = get_user_location_from_ip(request.remote_addr)
        lat, lon = location['latitude'], location['longitude']
    
    forecast_data = fetch_forecast_data(lat, lon, data['units'], data['lang'])
    
    result = {
        'location': {
            'coordinates': {'lat': lat, 'lon': lon},
            'timezone': forecast_data.get('timezone')
        },
        'daily': [{
            'date': datetime.fromtimestamp(day['dt']).date().isoformat(),
            'summary': day['summary'] if 'summary' in day else day['weather'][0]['description'],
            'temperatures': {
                'min': day['temp']['min'],
                'max': day['temp']['max'],
                'morning': day['temp']['morn'],
                'day': day['temp']['day'],
                'evening': day['temp']['eve'],
                'night': day['temp']['night']
            },
            'feels_like': {
                'morning': day['feels_like']['morn'],
                'day': day['feels_like']['day'],
                'evening': day['feels_like']['eve'],
                'night': day['feels_like']['night']
            },
            'humidity': day['humidity'],
            'wind_speed': day['wind_speed'],
            'wind_direction': day['wind_deg'],
            'weather': {
                'main': day['weather'][0]['main'],
                'description': day['weather'][0]['description'],
                'icon': day['weather'][0]['icon']
            },
            'clouds': day['clouds'],
            'precipitation_probability': day.get('pop', 0),
            'rain': day.get('rain', 0),
            'snow': day.get('snow', 0),
            'uv_index': day.get('uvi', 0),
            'sunrise': day['sunrise'],
            'sunset': day['sunset']
        } for day in forecast_data.get('daily', [])[:7]],
        'hourly': [{
            'time': datetime.fromtimestamp(hour['dt']).isoformat(),
            'temperature': hour['temp'],
            'feels_like': hour['feels_like'],
            'humidity': hour['humidity'],
            'clouds': hour['clouds'],
            'wind_speed': hour['wind_speed'],
            'weather': {
                'main': hour['weather'][0]['main'],
                'description': hour['weather'][0]['description'],
                'icon': hour['weather'][0]['icon']
            },
            'precipitation_probability': hour.get('pop', 0)
        } for hour in forecast_data.get('hourly', [])[:24]],
        'alerts': forecast_data.get('alerts', []),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    return jsonify(result)

@app.route('/api/v1/weather/historical', methods=['GET'])
@limiter.limit("20 per hour")
@require_auth
def get_historical_weather():
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    days = min(request.args.get('days', 7, type=int), 30)
    
    if not lat or not lon:
        location = get_user_location_from_ip(request.remote_addr)
        lat, lon = location['latitude'], location['longitude']
    
    historical_data = []
    
    for i in range(1, days + 1):
        timestamp = int((datetime.now(timezone.utc) - timedelta(days=i)).timestamp())
        
        cache_key = f'historical:{lat}:{lon}:{timestamp}'
        cached = redis_client.get(cache_key)
        
        if cached:
            historical_data.append(json.loads(cached))
        else:
            url = f"https://api.openweathermap.org/data/2.5/onecall/timemachine"
            params = {
                'lat': lat,
                'lon': lon,
                'dt': timestamp,
                'appid': app.config['OPENWEATHER_API_KEY'],
                'units': 'metric'
            }
            
            try:
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if 'current' in data:
                        day_data = {
                            'date': datetime.fromtimestamp(data['current']['dt']).date().isoformat(),
                            'temperature': data['current']['temp'],
                            'feels_like': data['current']['feels_like'],
                            'humidity': data['current']['humidity'],
                            'wind_speed': data['current']['wind_speed'],
                            'weather': data['current']['weather'][0] if data['current']['weather'] else {}
                        }
                        historical_data.append(day_data)
                        redis_client.setex(cache_key, 86400, json.dumps(day_data))
            except Exception as e:
                logger.error(f'Historical weather error: {str(e)}')
    
    return jsonify({
        'location': {'lat': lat, 'lon': lon},
        'days_requested': days,
        'days_returned': len(historical_data),
        'data': historical_data
    })

@app.route('/api/v1/weather/global-random', methods=['GET'])
@limiter.limit("30 per hour")
@cache.cached(timeout=1800)
def get_random_global_weather():
    cities = [
        {'name': 'Tokyo', 'country': 'JP', 'timezone': 'Asia/Tokyo'},
        {'name': 'London', 'country': 'GB', 'timezone': 'Europe/London'},
        {'name': 'New York', 'country': 'US', 'timezone': 'America/New_York'},
        {'name': 'Paris', 'country': 'FR', 'timezone': 'Europe/Paris'},
        {'name': 'Sydney', 'country': 'AU', 'timezone': 'Australia/Sydney'},
        {'name': 'Dubai', 'country': 'AE', 'timezone': 'Asia/Dubai'},
        {'name': 'Singapore', 'country': 'SG', 'timezone': 'Asia/Singapore'},
        {'name': 'Mumbai', 'country': 'IN', 'timezone': 'Asia/Kolkata'},
        {'name': 'São Paulo', 'country': 'BR', 'timezone': 'America/Sao_Paulo'},
        {'name': 'Cairo', 'country': 'EG', 'timezone': 'Africa/Cairo'}
    ]
    
    city = random.choice(cities)
    
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': f"{city['name']},{city['country']}",
            'appid': app.config['OPENWEATHER_API_KEY'],
            'units': 'metric'
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        return jsonify({
            'city': city['name'],
            'country': city['country'],
            'timezone': city['timezone'],
            'temperature': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'humidity': data['main']['humidity'],
            'weather': {
                'main': data['weather'][0]['main'],
                'description': data['weather'][0]['description'],
                'icon': data['weather'][0]['icon']
            },
            'wind_speed': data['wind']['speed'],
            'local_time': datetime.fromtimestamp(
                data['dt'] + data['timezone']
            ).strftime('%H:%M'),
            'message': f"Right now in {city['name']}, it's {data['main']['temp']}°C with {data['weather'][0]['description']}"
        })
        
    except Exception as e:
        logger.error(f'Global weather error: {str(e)}')
        raise ExternalAPIError('Failed to fetch global weather')

@app.route('/api/v1/user/profile', methods=['GET'])
@require_auth
def get_user_profile():
    return jsonify({
        'user': UserSchema().dump(g.user),
        'preferences': g.user.preferences,
        'stats': {
            'streak_count': g.user.streak_count,
            'max_streak': g.user.max_streak,
            'total_checks': g.user.total_checks,
            'member_since': g.user.created_at.isoformat()
        }
    })

@app.route('/api/v1/user/preferences', methods=['PUT'])
@require_auth
@audit_log('user.preferences.update', 'user')
def update_preferences():
    data = request.json
    
    if not data:
        raise ValidationError('No data provided')
    
    allowed_keys = ['units', 'theme', 'language', 'notifications', 'favorite_locations']
    
    for key in allowed_keys:
        if key in data:
            g.user.preferences[key] = data[key]
    
    db.session.commit()
    
    return jsonify({
        'message': 'Preferences updated successfully',
        'preferences': g.user.preferences
    })

@app.route('/api/v1/user/triggers', methods=['GET'])
@require_auth
def get_triggers():
    triggers = CustomTrigger.query.filter_by(user_id=g.user.id).all()
    
    return jsonify({
        'triggers': [{
            'id': str(trigger.id),
            'name': trigger.name,
            'description': trigger.description,
            'condition_type': trigger.condition_type,
            'condition_value': trigger.condition_value,
            'location': trigger.location,
            'is_active': trigger.is_active,
            'last_triggered': trigger.last_triggered.isoformat() if trigger.last_triggered else None,
            'trigger_count': trigger.trigger_count
        } for trigger in triggers]
    })

@app.route('/api/v1/user/triggers', methods=['POST'])
@require_auth
@validate_request(TriggerSchema)
@audit_log('trigger.create', 'trigger')
def create_trigger():
    data = g.validated_data
    
    existing = CustomTrigger.query.filter_by(
        user_id=g.user.id,
        name=data['name']
    ).first()
    
    if existing:
        raise ValidationError('Trigger with this name already exists')
    
    trigger = CustomTrigger(
        user_id=g.user.id,
        name=data['name'],
        description=data.get('description'),
        condition_type=data['condition_type'],
        condition_value=data.get('condition_value'),
        location=data.get('location'),
        is_active=data.get('is_active', True)
    )
    
    db.session.add(trigger)
    db.session.commit()
    
    return jsonify({
        'message': 'Trigger created successfully',
        'trigger': {
            'id': str(trigger.id),
            'name': trigger.name
        }
    }), 201

@app.route('/api/v1/user/triggers/<trigger_id>', methods=['DELETE'])
@require_auth
@audit_log('trigger.delete', 'trigger')
def delete_trigger(trigger_id):
    trigger = CustomTrigger.query.filter_by(
        id=trigger_id,
        user_id=g.user.id
    ).first()
    
    if not trigger:
        raise ResourceNotFoundError('Trigger not found')
    
    db.session.delete(trigger)
    db.session.commit()
    
    return jsonify({'message': 'Trigger deleted successfully'})

@app.route('/api/v1/user/alerts', methods=['GET'])
@require_auth
def get_alerts():
    page = request.args.get('page', 1, type=int)
    per_page = min(request.args.get('per_page', 20, type=int), 100)
    unread_only = request.args.get('unread', 'false').lower() == 'true'
    
    query = WeatherAlert.query.filter_by(user_id=g.user.id)
    
    if unread_only:
        query = query.filter_by(is_read=False)
    
    alerts = query.order_by(WeatherAlert.created_at.desc()).paginate(
        page=page,
        per_page=per_page,
        error_out=False
    )
    
    return jsonify({
        'alerts': [{
            'id': str(alert.id),
            'type': alert.alert_type,
            'severity': alert.severity,
            'title': alert.title,
            'message': alert.message,
            'location': alert.location,
            'is_read': alert.is_read,
            'created_at': alert.created_at.isoformat()
        } for alert in alerts.items],
        'pagination': {
            'page': alerts.page,
            'pages': alerts.pages,
            'per_page': alerts.per_page,
            'total': alerts.total
        }
    })

@app.route('/api/v1/user/alerts/<alert_id>/read', methods=['PUT'])
@require_auth
def mark_alert_read(alert_id):
    alert = WeatherAlert.query.filter_by(
        id=alert_id,
        user_id=g.user.id
    ).first()
    
    if not alert:
        raise ResourceNotFoundError('Alert not found')
    
    alert.is_read = True
    alert.read_at = datetime.now(timezone.utc)
    db.session.commit()
    
    return jsonify({'message': 'Alert marked as read'})

@app.route('/api/v1/user/logs', methods=['GET'])
@require_auth
def get_weather_logs():
    page = request.args.get('page', 1, type=int)
    per_page = min(request.args.get('per_page', 20, type=int), 100)
    
    logs = WeatherLog.query.filter_by(user_id=g.user.id).order_by(
        WeatherLog.created_at.desc()
    ).paginate(page=page, per_page=per_page, error_out=False)
    
    return jsonify({
        'logs': [{
            'id': str(log.id),
            'location': log.location,
            'weather_data': log.weather_data,
            'mood': log.mood,
            'note': log.note,
            'tags': log.tags,
            'created_at': log.created_at.isoformat()
        } for log in logs.items],
        'pagination': {
            'page': logs.page,
            'pages': logs.pages,
            'per_page': logs.per_page,
            'total': logs.total
        }
    })

@app.route('/api/v1/user/logs', methods=['POST'])
@require_auth
@audit_log('log.create', 'weather_log')
def create_weather_log():
    data = request.json
    
    if not data or not data.get('location'):
        raise ValidationError('Location is required')
    
    log = WeatherLog(
        user_id=g.user.id,
        mood=data.get('mood'),
        note=data.get('note'),
        tags=data.get('tags', []),
        weather_data=data.get('weather_data', {})
    )
    log.set_location(data['location'])
    
    db.session.add(log)
    db.session.commit()
    
    return jsonify({
        'message': 'Log created successfully',
        'log_id': str(log.id)
    }), 201

@app.route('/api/v1/entertainment/playlist', methods=['GET'])
@limiter.limit("50 per hour")
def get_playlist():
    condition = request.args.get('condition', 'clear')
    playlist = get_weather_playlist(condition)
    return jsonify(playlist)

@app.route('/api/v1/entertainment/sounds', methods=['GET'])
@limiter.limit("50 per hour")
def get_sounds():
    condition = request.args.get('condition', 'clear')
    sounds = get_weather_sounds(condition)
    return jsonify(sounds)

@app.route('/api/v1/entertainment/activities', methods=['GET'])
@limiter.limit("50 per hour")
def get_activities():
    temp = request.args.get('temperature', 20, type=float)
    condition = request.args.get('condition', 'clear')
    wind_speed = request.args.get('wind_speed', 0, type=float)
    
    activities = get_activity_suggestions(temp, condition, wind_speed)
    return jsonify({'activities': activities})

@app.route('/api/v1/entertainment/fact', methods=['GET'])
@limiter.limit("100 per hour")
def get_fact():
    return jsonify(get_weather_fun_facts())

@app.cli.command()
def init_db():
    db.create_all()
    print('Database initialized successfully')

@app.cli.command()
def seed_db():
    from datetime import datetime, timedelta
    import random
    
    test_user = User(
        email='test@example.com',
        username='testuser',
        is_verified=True,
        is_premium=True
    )
    test_user.set_password('Test123456')
    test_user.generate_api_key()
    
    db.session.add(test_user)
    db.session.commit()
    
    print(f'Test user created: test@example.com / Test123456')
    print(f'API Key: {test_user.api_key}')

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') == 'development'
    
    app.run(host='0.0.0.0', port=port, debug=debug)
# backend/services/weather.py
import os
import requests
import logging
import asyncio
import aiohttp
import hashlib
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, asdict
import redis
from functools import wraps
from collections import defaultdict
import uuid

logger = logging.getLogger(__name__)

@dataclass
class WeatherData:
    temperature: float
    feels_like: float
    humidity: int
    pressure: float
    wind_speed: float
    wind_direction: int
    visibility: float
    uv_index: float
    cloud_cover: int
    condition: str
    description: str
    icon: str
    timestamp: datetime
    precipitation: float = 0.0
    precipitation_probability: int = 0
    dew_point: float = 0.0
    etag: Optional[str] = None

@dataclass
class AirQualityData:
    aqi: int
    level: str
    pm2_5: float
    pm10: float
    o3: float
    no2: float
    co: float
    so2: float
    health_recommendation: str = ""
    etag: Optional[str] = None

@dataclass
class HealthInsights:
    heat_index: float
    wind_chill: float
    pollen_level: str
    air_quality_advice: str
    uv_advice: str
    general_health_tips: List[str]
    comfort_level: str
    hydration_advice: str
    exercise_advice: str

@dataclass
class RequestMetadata:
    request_id: str
    timestamp: float
    location_hash: str
    etag: Optional[str] = None
    cache_ttl: int = 600

class RateLimiter:
    def __init__(self):
        self.calls = defaultdict(list)
        self.limits = {
            'accuweather': {'calls': 50, 'window': 3600},
            'tomorrow': {'calls': 1000, 'window': 3600},
            'visual_crossing': {'calls': 1000, 'window': 86400},
            'openweather': {'calls': 1000, 'window': 3600}
        }
    
    def can_make_request(self, provider: str) -> bool:
        current_time = time.time()
        limit_info = self.limits.get(provider, {'calls': 100, 'window': 3600})
        
        cutoff_time = current_time - limit_info['window']
        self.calls[provider] = [call_time for call_time in self.calls[provider] 
                               if call_time > cutoff_time]
        
        return len(self.calls[provider]) < limit_info['calls']
    
    def record_call(self, provider: str):
        self.calls[provider].append(time.time())

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = defaultdict(int)
        self.last_failure_time = defaultdict(float)
        self.state = defaultdict(lambda: 'closed')
    
    def can_execute(self, provider: str) -> bool:
        if self.state[provider] == 'closed':
            return True
        elif self.state[provider] == 'open':
            if time.time() - self.last_failure_time[provider] > self.recovery_timeout:
                self.state[provider] = 'half-open'
                return True
            return False
        else:
            return True
    
    def record_success(self, provider: str):
        self.failure_count[provider] = 0
        self.state[provider] = 'closed'
    
    def record_failure(self, provider: str):
        self.failure_count[provider] += 1
        self.last_failure_time[provider] = time.time()
        
        if self.failure_count[provider] >= self.failure_threshold:
            self.state[provider] = 'open'

class RequestDeduplicator:
    def __init__(self):
        self.pending_requests = {}
        self.lock = asyncio.Lock()
    
    async def deduplicate_request(self, key: str, request_func):
        async with self.lock:
            if key in self.pending_requests:
                return await self.pending_requests[key]
            
            future = asyncio.create_task(request_func())
            self.pending_requests[key] = future
            
            try:
                result = await future
                return result
            finally:
                self.pending_requests.pop(key, None)

class SmartActivityRecommendations:
    def __init__(self):
        self.activity_time_windows = {
            'outdoor_yoga': {'start': 6, 'end': 10, 'ideal': [6, 7, 8, 9]},
            'jogging': {'start': 5, 'end': 22, 'ideal': [6, 7, 8, 17, 18, 19]},
            'cycling': {'start': 6, 'end': 20, 'ideal': [7, 8, 9, 17, 18]},
            'hiking': {'start': 6, 'end': 18, 'ideal': [7, 8, 9, 10]},
            'outdoor_exercise': {'start': 6, 'end': 20, 'ideal': [7, 8, 9, 17, 18]},
            'team_sports': {'start': 8, 'end': 20, 'ideal': [9, 10, 16, 17, 18]},
            'running': {'start': 5, 'end': 22, 'ideal': [6, 7, 8, 17, 18, 19]},
            'swimming': {'start': 8, 'end': 20, 'ideal': [10, 11, 12, 13, 14, 15]},
            'water_sports': {'start': 9, 'end': 18, 'ideal': [10, 11, 12, 13, 14]},
            'beach_activities': {'start': 8, 'end': 19, 'ideal': [9, 10, 11, 16, 17]},
            'outdoor_dining': {'start': 8, 'end': 23, 'ideal': [12, 13, 18, 19, 20]},
            'picnic': {'start': 10, 'end': 18, 'ideal': [11, 12, 13, 14, 15]},
            'barbecue': {'start': 11, 'end': 21, 'ideal': [17, 18, 19, 20]},
            'outdoor_events': {'start': 9, 'end': 22, 'ideal': [10, 11, 16, 17, 18]},
            'social_gatherings': {'start': 10, 'end': 23, 'ideal': [11, 17, 18, 19, 20]},
            'festivals': {'start': 10, 'end': 22, 'ideal': [11, 12, 16, 17, 18]},
            'walking': {'start': 6, 'end': 22, 'ideal': [7, 8, 17, 18, 19]},
            'light_walks': {'start': 6, 'end': 21, 'ideal': [7, 8, 18, 19]},
            'dog_walking': {'start': 6, 'end': 22, 'ideal': [7, 8, 17, 18, 19]},
            'sightseeing': {'start': 8, 'end': 19, 'ideal': [9, 10, 11, 16, 17]},
            'photography': {'start': 5, 'end': 21, 'ideal': [6, 7, 17, 18, 19]},
            'gardening': {'start': 6, 'end': 18, 'ideal': [7, 8, 9, 16, 17]},
            'outdoor_work': {'start': 7, 'end': 17, 'ideal': [8, 9, 10, 15, 16]},
            'shopping': {'start': 8, 'end': 22, 'ideal': [10, 11, 14, 15, 16]},
            'outdoor_markets': {'start': 8, 'end': 16, 'ideal': [9, 10, 11, 12]},
            'stargazing': {'start': 20, 'end': 5, 'ideal': [21, 22, 23, 0, 1]},
            'night_walks': {'start': 19, 'end': 23, 'ideal': [19, 20, 21]},
            'evening_strolls': {'start': 17, 'end': 22, 'ideal': [18, 19, 20]},
            'outdoor_concerts': {'start': 17, 'end': 23, 'ideal': [19, 20, 21]},
            'indoor_exercise': {'start': 0, 'end': 24, 'ideal': [6, 7, 8, 17, 18, 19]},
            'gym_workout': {'start': 5, 'end': 23, 'ideal': [6, 7, 8, 17, 18, 19]},
            'yoga_studio': {'start': 6, 'end': 22, 'ideal': [7, 8, 9, 17, 18, 19]},
            'indoor_sports': {'start': 6, 'end': 23, 'ideal': [8, 9, 17, 18, 19]},
            'shopping_malls': {'start': 9, 'end': 22, 'ideal': [10, 11, 14, 15, 16]},
            'museums': {'start': 9, 'end': 18, 'ideal': [10, 11, 14, 15, 16]},
            'movie_theaters': {'start': 10, 'end': 23, 'ideal': [14, 15, 19, 20, 21]},
            'restaurants': {'start': 11, 'end': 23, 'ideal': [12, 13, 18, 19, 20]},
            'cafes': {'start': 7, 'end': 22, 'ideal': [8, 9, 10, 14, 15, 16]},
            'libraries': {'start': 8, 'end': 20, 'ideal': [9, 10, 14, 15, 16]},
            'art_galleries': {'start': 10, 'end': 18, 'ideal': [11, 12, 14, 15, 16]},
            'reading': {'start': 0, 'end': 24, 'ideal': [8, 9, 14, 15, 20, 21, 22]},
            'meditation': {'start': 0, 'end': 24, 'ideal': [6, 7, 8, 20, 21, 22]},
            'relaxation': {'start': 0, 'end': 24, 'ideal': [14, 15, 20, 21, 22]},
            'sleep': {'start': 21, 'end': 8, 'ideal': [22, 23, 0, 1, 2, 3, 4, 5, 6, 7]},
            'nap': {'start': 13, 'end': 16, 'ideal': [14, 15]},
        }
        
        self.safety_considerations = {
            'night': {
                'avoid_activities': ['hiking', 'cycling', 'outdoor_exercise', 'jogging', 'team_sports'],
                'safety_tips': ['Use proper lighting', 'Stay in well-lit areas', 'Inform someone of your plans'],
                'preferred_activities': ['indoor_activities', 'reading', 'relaxation', 'sleep']
            },
            'early_morning': {
                'considerations': ['Limited daylight', 'Lower temperatures', 'Fewer people around'],
                'safety_tips': ['Wear reflective clothing', 'Use lights', 'Choose familiar routes']
            },
            'late_evening': {
                'considerations': ['Decreasing visibility', 'Traffic concerns'],
                'safety_tips': ['Be visible', 'Stay alert', 'Choose safe routes']
            }
        }
        
        self.weather_activity_modifiers = {
            'Rain': {
                'force_indoor': True,
                'avoid_all_outdoor': True,
                'preferred': ['indoor_exercise', 'museums', 'shopping_malls', 'movie_theaters', 'reading', 'cafes']
            },
            'Thunderstorm': {
                'force_indoor': True,
                'avoid_all_outdoor': True,
                'urgent_indoor': True,
                'preferred': ['indoor_activities', 'reading', 'relaxation', 'sleep']
            },
            'Snow': {
                'limit_outdoor': True,
                'cold_weather_only': ['skiing', 'snowboarding', 'snow_activities'],
                'avoid': ['cycling', 'outdoor_exercise', 'water_activities']
            },
            'Clear': {
                'boost_outdoor': True,
                'ideal_for': ['outdoor_exercise', 'hiking', 'cycling', 'photography']
            },
            'Clouds': {
                'good_for_all': True,
                'ideal_for': ['outdoor_exercise', 'walking', 'sightseeing']
            }
        }
    
    def get_time_appropriate_activities(self, current_hour: int, weather_condition: str, 
                                      temperature: float, wind_speed: float) -> Dict:
        appropriate_activities = {
            'highly_recommended': [],
            'suitable': [],
            'indoor_alternatives': [],
            'avoid': [],
            'safety_considerations': []
        }
        
        weather_mods = self.weather_activity_modifiers.get(weather_condition, {})
        
        if weather_mods.get('force_indoor') or weather_mods.get('urgent_indoor'):
            appropriate_activities['highly_recommended'] = [
                'indoor_exercise', 'reading', 'relaxation', 'cafes', 'shopping_malls'
            ]
            appropriate_activities['avoid'] = ['All outdoor activities due to weather']
            appropriate_activities['safety_considerations'] = ['Stay indoors for safety']
            return appropriate_activities
        
        for activity, time_window in self.activity_time_windows.items():
            is_time_appropriate = self._is_activity_time_appropriate(activity, current_hour, time_window)
            is_weather_appropriate = self._is_activity_weather_appropriate(activity, weather_condition, temperature, wind_speed)
            
            if is_time_appropriate and is_weather_appropriate:
                if current_hour in time_window.get('ideal', []):
                    appropriate_activities['highly_recommended'].append(activity)
                else:
                    appropriate_activities['suitable'].append(activity)
            elif not is_time_appropriate and activity.startswith('indoor'):
                appropriate_activities['indoor_alternatives'].append(activity)
            elif not is_time_appropriate or not is_weather_appropriate:
                appropriate_activities['avoid'].append(activity)
        
        safety_notes = self._get_safety_considerations(current_hour, weather_condition)
        appropriate_activities['safety_considerations'] = safety_notes
        
        appropriate_activities['highly_recommended'] = appropriate_activities['highly_recommended'][:6]
        appropriate_activities['suitable'] = appropriate_activities['suitable'][:8]
        appropriate_activities['indoor_alternatives'] = appropriate_activities['indoor_alternatives'][:5]
        
        return appropriate_activities
    
    def _is_activity_time_appropriate(self, activity: str, current_hour: int, time_window: Dict) -> bool:
        start_hour = time_window['start']
        end_hour = time_window['end']
        
        if start_hour > end_hour:
            return current_hour >= start_hour or current_hour <= end_hour
        else:
            return start_hour <= current_hour <= end_hour
    
    def _is_activity_weather_appropriate(self, activity: str, condition: str, temp: float, wind: float) -> bool:
        if 'indoor' in activity:
            return True
        
        if temp < -5 and activity in ['swimming', 'water_sports', 'beach_activities']:
            return False
        
        if temp > 35 and activity in ['jogging', 'hiking', 'outdoor_exercise', 'cycling']:
            return False
        
        if wind > 30 and activity in ['cycling', 'outdoor_yoga', 'picnic']:
            return False
        
        if condition in ['Rain', 'Thunderstorm'] and not activity.startswith('indoor'):
            return False
        
        if condition == 'Snow' and activity in ['swimming', 'water_sports', 'beach_activities']:
            return False
        
        return True
    
    def _get_safety_considerations(self, current_hour: int, weather_condition: str) -> List[str]:
        considerations = []
        
        if 22 <= current_hour or current_hour <= 5:
            considerations.extend([
                'Use proper lighting if going outside',
                'Stay in well-lit, familiar areas',
                'Inform someone of your plans',
                'Consider indoor alternatives for safety'
            ])
        elif 5 <= current_hour <= 7:
            considerations.extend([
                'Limited daylight - use reflective clothing',
                'Be extra cautious of traffic',
                'Warm up properly in cool temperatures'
            ])
        
        if weather_condition == 'Rain':
            considerations.extend([
                'Watch for slippery surfaces',
                'Use waterproof gear if going outside',
                'Drive carefully if traveling'
            ])
        elif weather_condition == 'Thunderstorm':
            considerations.extend([
                'Stay indoors for safety',
                'Avoid electrical equipment',
                'Stay away from windows'
            ])
        elif weather_condition == 'Snow':
            considerations.extend([
                'Dress warmly in layers',
                'Be cautious of icy surfaces',
                'Allow extra travel time'
            ])
        
        return considerations[:4]

class UltraWeatherService:
    def __init__(self):
        self.accuweather_api_key = os.getenv('ACCUWEATHER_API_KEY')
        self.tomorrow_api_key = os.getenv('TOMORROW_API_KEY')
        self.visual_crossing_api_key = os.getenv('VISUAL_CROSSING_API_KEY')
        self.openweather_api_key = os.getenv('OPENWEATHER_API_KEY')
        
        self.rate_limiter = RateLimiter()
        self.circuit_breaker = CircuitBreaker()
        self.request_deduplicator = RequestDeduplicator()
        self.activity_recommender = SmartActivityRecommendations()
        
        self.redis_client = self._setup_redis()
        self.memory_cache = {}
        
        self.spatial_threshold = 0.01
        self.temporal_threshold = 300
        self.cache_ttls = {
            'current_weather': 300,
            'forecast': 1800,
            'air_quality': 600,
            'location_weather': 900,
        }
        
        self.active_requests = set()
        self.request_history = defaultdict(list)
        
        self.provider_priority = ['accuweather', 'tomorrow', 'visual_crossing', 'openweather']
        
        self.time_periods = {
            'early_morning': (4, 7),
            'morning': (7, 11),
            'late_morning': (11, 13),
            'afternoon': (13, 17),
            'late_afternoon': (17, 19),
            'evening': (19, 22),
            'night': (22, 4)
        }
    
    def _setup_redis(self):
        try:
            redis_url = os.getenv('REDIS_URL')
            if redis_url:
                client = redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=5)
            else:
                client = redis.Redis(
                    host=os.getenv('REDIS_HOST', 'localhost'),
                    port=int(os.getenv('REDIS_PORT', 6379)),
                    db=1,
                    decode_responses=True,
                    socket_connect_timeout=5
                )
            client.ping()
            logger.info("Redis connected for weather caching")
            return client
        except Exception as e:
            logger.warning(f"Redis not available for weather caching: {e}")
            return None
    
    def _generate_cache_key(self, prefix: str, lat: float, lon: float, **kwargs) -> str:
        rounded_lat = round(lat, 3)
        rounded_lon = round(lon, 3)
        
        key_parts = [prefix, f"{rounded_lat},{rounded_lon}"]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")
        
        key_string = ":".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _should_fetch_new_data(self, lat: float, lon: float, data_type: str) -> bool:
        cache_key = self._generate_cache_key(f"{data_type}_meta", lat, lon)
        
        cached_meta = self._get_from_cache(cache_key, use_memory=True)
        if not cached_meta:
            return True
        
        time_diff = time.time() - cached_meta.get('timestamp', 0)
        if time_diff > self.temporal_threshold:
            return True
        
        cached_lat = cached_meta.get('lat', 0)
        cached_lon = cached_meta.get('lon', 0)
        
        lat_diff = abs(lat - cached_lat)
        lon_diff = abs(lon - cached_lon)
        
        if lat_diff > self.spatial_threshold or lon_diff > self.spatial_threshold:
            return True
        
        return False
    
    def _get_from_cache(self, key: str, use_memory: bool = False) -> Optional[Dict]:
        try:
            if self.redis_client and not use_memory:
                data = self.redis_client.get(key)
                if data:
                    return json.loads(data)
        except Exception as e:
            logger.warning(f"Redis get error: {e}")
        
        cached_item = self.memory_cache.get(key)
        if cached_item:
            if cached_item.get('expires', 0) > time.time():
                return cached_item.get('data')
            else:
                del self.memory_cache[key]
        return None
    
    def _set_cache(self, key: str, data: Dict, ttl: int, use_memory: bool = False):
        try:
            if self.redis_client and not use_memory:
                self.redis_client.setex(key, ttl, json.dumps(data, default=str))
                return
        except Exception as e:
            logger.warning(f"Redis set error: {e}")
        
        self.memory_cache[key] = {
            'data': data,
            'expires': time.time() + ttl
        }
    
    async def get_ultra_weather_analysis(self, lat: float, lon: float) -> Dict:
        request_id = str(uuid.uuid4())
        logger.info(f"[{request_id}] Starting ultra weather analysis for {lat:.4f}, {lon:.4f}")
        
        if not self._should_fetch_new_data(lat, lon, 'ultra_weather'):
            cache_key = self._generate_cache_key('ultra_weather', lat, lon)
            cached_data = self._get_from_cache(cache_key)
            if cached_data:
                logger.info(f"[{request_id}] Returning cached weather data")
                cached_data['cache_hit'] = True
                cached_data['request_id'] = request_id
                return cached_data
        
        location_key = f"{round(lat, 3)},{round(lon, 3)}"
        
        async def fetch_weather_data():
            return await self._fetch_comprehensive_weather_data(lat, lon, request_id)
        
        try:
            weather_data = await self.request_deduplicator.deduplicate_request(
                f"weather:{location_key}", fetch_weather_data
            )
            
            current_hour = datetime.now().hour
            time_period = self._get_precise_time_period(current_hour)
            
            activities = self.activity_recommender.get_time_appropriate_activities(
                current_hour,
                weather_data['weather']['condition'],
                weather_data['weather']['temperature']['current'],
                weather_data['weather']['wind']['speed']
            )
            
            enhanced_response = {
                'success': True,
                'request_id': request_id,
                'weather': weather_data['weather'],
                'air_quality': weather_data.get('air_quality'),
                'health_insights': weather_data.get('health_insights'),
                'current_hour': current_hour,
                'time_period': time_period,
                'smart_recommendations': {
                    'activities': activities,
                    'time_context': self._get_time_context(current_hour),
                    'weather_suitability': self._assess_weather_suitability(weather_data['weather']),
                    'comfort_score': self._calculate_comfort_score(weather_data['weather'])
                },
                'location': {'lat': lat, 'lon': lon},
                'cache_hit': False,
                'timestamp': datetime.utcnow().isoformat(),
                'accuracy_score': weather_data.get('accuracy_score', 0.85)
            }
            
            cache_key = self._generate_cache_key('ultra_weather', lat, lon)
            self._set_cache(cache_key, enhanced_response, self.cache_ttls['current_weather'])
            
            meta_key = self._generate_cache_key('ultra_weather_meta', lat, lon)
            meta_data = {'timestamp': time.time(), 'lat': lat, 'lon': lon}
            self._set_cache(meta_key, meta_data, self.cache_ttls['current_weather'], use_memory=True)
            
            logger.info(f"[{request_id}] Ultra weather analysis completed successfully")
            return enhanced_response
            
        except Exception as e:
            logger.error(f"[{request_id}] Ultra weather analysis failed: {e}")
            current_hour = datetime.now().hour
            return {
                'success': False,
                'error': 'Weather service temporarily unavailable',
                'request_id': request_id,
                'fallback_recommendations': self._get_fallback_recommendations(current_hour),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    async def get_current_weather_enhanced(self, lat: float, lon: float) -> Dict:
        weather_data = None
        air_quality = None
        health_insights = None
        
        if self.tomorrow_api_key:
            try:
                weather_data = await self._get_tomorrow_current(lat, lon)
                air_quality = await self._get_tomorrow_air_quality(lat, lon)
                health_insights = await self._get_tomorrow_health_insights(lat, lon)
            except Exception as e:
                logger.warning(f"Tomorrow.io failed: {e}")
        
        if not weather_data and self.visual_crossing_api_key:
            try:
                weather_data = await self._get_visual_crossing_current(lat, lon)
            except Exception as e:
                logger.warning(f"Visual Crossing failed: {e}")
        
        if not weather_data and self.openweather_api_key:
            try:
                weather_data = await self._get_openweather_current(lat, lon)
                if not air_quality:
                    air_quality = await self._get_openweather_air_quality(lat, lon)
            except Exception as e:
                logger.warning(f"OpenWeather failed: {e}")
        
        if not weather_data:
            raise Exception("All weather providers failed")
        
        current_hour = datetime.now().hour
        time_period = self._get_time_period(current_hour)
        
        recommendations = self._get_time_based_recommendations(
            weather_data.condition, time_period, weather_data.temperature
        )
        
        return {
            'success': True,
            'weather': {
                'temperature': {
                    'current': round(weather_data.temperature, 1),
                    'feels_like': round(weather_data.feels_like, 1),
                    'unit': '°C'
                },
                'condition': weather_data.condition,
                'description': weather_data.description,
                'icon': weather_data.icon,
                'humidity': weather_data.humidity,
                'pressure': weather_data.pressure,
                'wind': {
                    'speed': weather_data.wind_speed,
                    'direction': weather_data.wind_direction
                },
                'visibility': weather_data.visibility,
                'uv_index': weather_data.uv_index,
                'cloud_cover': weather_data.cloud_cover
            },
            'air_quality': air_quality.__dict__ if air_quality else None,
            'health_insights': health_insights.__dict__ if health_insights else None,
            'time_period': time_period,
            'recommendations': recommendations,
            'location': {'lat': lat, 'lon': lon},
            'timestamp': datetime.utcnow().isoformat()
        }
    
    async def get_forecast_enhanced(self, lat: float, lon: float, days: int = 7) -> Dict:
        forecast_data = None
        
        if self.accuweather_api_key:
            try:
                forecast_data = await self._get_accuweather_forecast(lat, lon, days)
            except Exception as e:
                logger.warning(f"AccuWeather forecast failed: {e}")
        
        if not forecast_data and self.tomorrow_api_key:
            try:
                forecast_data = await self._get_tomorrow_forecast(lat, lon, days)
            except Exception as e:
                logger.warning(f"Tomorrow.io forecast failed: {e}")
        
        if not forecast_data and self.visual_crossing_api_key:
            try:
                forecast_data = await self._get_visual_crossing_forecast(lat, lon, days)
            except Exception as e:
                logger.warning(f"Visual Crossing forecast failed: {e}")
        
        if not forecast_data and self.openweather_api_key:
            try:
                forecast_data = await self._get_openweather_forecast(lat, lon, days)
            except Exception as e:
                logger.warning(f"OpenWeather forecast failed: {e}")
        
        if not forecast_data:
            raise Exception("All forecast providers failed")
        
        best_times = self._calculate_best_times_detailed(forecast_data)
        
        return {
            'success': True,
            'forecast': forecast_data,
            'best_times': best_times,
            'location': {'lat': lat, 'lon': lon},
            'timestamp': datetime.utcnow().isoformat()
        }
    
    async def _fetch_comprehensive_weather_data(self, lat: float, lon: float, request_id: str) -> Dict:
        providers = ['accuweather', 'tomorrow', 'visual_crossing', 'openweather']
        
        for provider in providers:
            if not self.circuit_breaker.can_execute(provider):
                logger.warning(f"[{request_id}] Circuit breaker open for {provider}")
                continue
            
            if not self.rate_limiter.can_make_request(provider):
                logger.warning(f"[{request_id}] Rate limit exceeded for {provider}")
                continue
            
            try:
                logger.info(f"[{request_id}] Attempting to fetch from {provider}")
                
                if provider == 'accuweather' and self.accuweather_api_key:
                    weather_data = await self._get_accuweather_current(lat, lon)
                    air_quality = await self._get_accuweather_air_quality(lat, lon)
                elif provider == 'tomorrow' and self.tomorrow_api_key:
                    weather_data = await self._get_tomorrow_current(lat, lon)
                    air_quality = await self._get_tomorrow_air_quality(lat, lon)
                elif provider == 'visual_crossing' and self.visual_crossing_api_key:
                    weather_data = await self._get_visual_crossing_current(lat, lon)
                    air_quality = None
                elif provider == 'openweather' and self.openweather_api_key:
                    weather_data = await self._get_openweather_current(lat, lon)
                    air_quality = await self._get_openweather_air_quality(lat, lon)
                else:
                    continue
                
                self.rate_limiter.record_call(provider)
                self.circuit_breaker.record_success(provider)
                
                health_insights = await self._calculate_comprehensive_health_insights(weather_data, air_quality)
                
                logger.info(f"[{request_id}] Successfully fetched from {provider}")
                
                return {
                    'weather': self._format_weather_response(weather_data),
                    'air_quality': air_quality.__dict__ if air_quality else None,
                    'health_insights': health_insights.__dict__ if health_insights else None,
                    'provider': provider,
                    'accuracy_score': 0.98 if provider == 'accuweather' else 0.90 if provider == 'tomorrow' else 0.85
                }
                
            except Exception as e:
                logger.warning(f"[{request_id}] {provider} failed: {e}")
                self.circuit_breaker.record_failure(provider)
                continue
        
        raise Exception("All weather providers failed")
    
    async def _get_accuweather_current(self, lat: float, lon: float) -> WeatherData:
        location_key = await self._get_accuweather_location_key(lat, lon)
        
        url = f"http://dataservice.accuweather.com/currentconditions/v1/{location_key}"
        params = {
            'apikey': self.accuweather_api_key,
            'details': 'true'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"AccuWeather API error: {response.status}")
                
                data = await response.json()
                
                if not data or not isinstance(data, list) or len(data) == 0:
                    raise Exception("Invalid AccuWeather response")
                
                current = data[0]
                temperature = current['Temperature']['Metric']['Value']
                feels_like = current['RealFeelTemperature']['Metric']['Value']
                
                return WeatherData(
                    temperature=temperature,
                    feels_like=feels_like,
                    humidity=current.get('RelativeHumidity', 0),
                    pressure=current.get('Pressure', {}).get('Metric', {}).get('Value', 0),
                    wind_speed=current.get('Wind', {}).get('Speed', {}).get('Metric', {}).get('Value', 0),
                    wind_direction=current.get('Wind', {}).get('Direction', {}).get('Degrees', 0),
                    visibility=current.get('Visibility', {}).get('Metric', {}).get('Value', 0),
                    uv_index=current.get('UVIndex', 0),
                    cloud_cover=current.get('CloudCover', 0),
                    condition=current['WeatherText'],
                    description=current['WeatherText'],
                    icon=str(current['WeatherIcon']),
                    timestamp=datetime.now(),
                    precipitation=current.get('PrecipitationSummary', {}).get('PastHour', {}).get('Metric', {}).get('Value', 0),
                    dew_point=current.get('DewPoint', {}).get('Metric', {}).get('Value', 0)
                )
    
    async def _get_accuweather_location_key(self, lat: float, lon: float) -> str:
        url = "http://dataservice.accuweather.com/locations/v1/cities/geoposition/search"
        params = {
            'apikey': self.accuweather_api_key,
            'q': f"{lat},{lon}",
            'details': 'true'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"AccuWeather location API error: {response.status}")
                
                data = await response.json()
                
                if not data or 'Key' not in data:
                    raise Exception("Invalid AccuWeather location response")
                
                return data['Key']
    
    async def _get_accuweather_air_quality(self, lat: float, lon: float) -> Optional[AirQualityData]:
        try:
            logger.info("AccuWeather air quality not available in free tier")
            return None
        except Exception as e:
            logger.warning(f"AccuWeather air quality error: {e}")
            return None
    
    async def _get_accuweather_forecast(self, lat: float, lon: float, days: int) -> List[Dict]:
        location_key = await self._get_accuweather_location_key(lat, lon)
        
        url = f"http://dataservice.accuweather.com/forecasts/v1/daily/{days}day/{location_key}"
        params = {
            'apikey': self.accuweather_api_key,
            'details': 'true',
            'metric': 'true'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=15) as response:
                if response.status != 200:
                    raise Exception(f"AccuWeather forecast API error: {response.status}")
                
                data = await response.json()
                
                if not data or 'DailyForecasts' not in data:
                    raise Exception("Invalid AccuWeather forecast response")
                
                forecast = []
                for day_data in data['DailyForecasts']:
                    date = datetime.fromisoformat(day_data['Date'].split('T')[0]).date()
                    
                    forecast.append({
                        'date': date.isoformat(),
                        'day_name': date.strftime('%A'),
                        'temperature': {
                            'min': round(day_data['Temperature']['Minimum']['Value'], 1),
                            'max': round(day_data['Temperature']['Maximum']['Value'], 1),
                            'avg': round((day_data['Temperature']['Minimum']['Value'] + 
                                        day_data['Temperature']['Maximum']['Value']) / 2, 1)
                        },
                        'condition': day_data['Day']['IconPhrase'],
                        'description': day_data['Day']['LongPhrase'],
                        'icon': str(day_data['Day']['Icon']),
                        'humidity': day_data.get('Day', {}).get('RelativeHumidity', {}).get('Average', 0),
                        'pressure': 0,
                        'wind_speed': day_data.get('Day', {}).get('Wind', {}).get('Speed', {}).get('Value', 0),
                        'uv_index': day_data.get('AirAndPollen', [{}])[0].get('Value', 0) if day_data.get('AirAndPollen') else 0,
                        'precipitation_probability': day_data['Day'].get('PrecipitationProbability', 0)
                    })
                
                return forecast
    
    async def _get_tomorrow_current(self, lat: float, lon: float) -> WeatherData:
        url = "https://api.tomorrow.io/v4/weather/realtime"
        params = {
            'location': f"{lat},{lon}",
            'apikey': self.tomorrow_api_key,
            'fields': 'temperature,temperatureApparent,humidity,pressureSeaLevel,windSpeed,windDirection,visibility,uvIndex,cloudCover,weatherCode,precipitationIntensity,dewPoint'
        }
    
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status == 429:
                    raise Exception("Tomorrow.io rate limit exceeded")
                if response.status != 200:
                    raise Exception(f"Tomorrow.io API error: {response.status}")
                
                data = await response.json()
                
                if 'data' not in data or 'values' not in data.get('data', {}):
                    raise Exception("Invalid Tomorrow.io response structure")
                
                values = data['data']['values']
                weather_code = values.get('weatherCode', 1000)
                condition, description, icon = self._map_tomorrow_weather_code(weather_code)
                
                return WeatherData(
                    temperature=values.get('temperature', 0),
                    feels_like=values.get('temperatureApparent', 0),
                    humidity=values.get('humidity', 0),
                    pressure=values.get('pressureSeaLevel', 0),
                    wind_speed=values.get('windSpeed', 0),
                    wind_direction=values.get('windDirection', 0),
                    visibility=values.get('visibility', 0),
                    uv_index=values.get('uvIndex', 0),
                    cloud_cover=values.get('cloudCover', 0),
                    condition=condition,
                    description=description,
                    icon=icon,
                    timestamp=datetime.fromisoformat(data['data']['time'].replace('Z', '+00:00')),
                    precipitation=values.get('precipitationIntensity', 0),
                    dew_point=values.get('dewPoint', 0)
                )
    
    async def _get_tomorrow_air_quality(self, lat: float, lon: float) -> Optional[AirQualityData]:
        url = "https://api.tomorrow.io/v4/weather/realtime"
        params = {
            'location': f"{lat},{lon}",
            'apikey': self.tomorrow_api_key,
            'fields': 'epaIndex,particulateMatter25,particulateMatter10,ozoneLevel,nitrogenDioxideLevel,carbonMonoxideLevel,sulphurDioxideLevel'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"Tomorrow.io air quality API error: {response.status}")
                
                data = await response.json()
                
                if 'data' not in data or 'values' not in data.get('data', {}):
                    raise Exception("Invalid Tomorrow.io air quality response")
                
                values = data['data']['values']
                epa_index = values.get('epaIndex', 1)
                
                aqi_levels = {
                    1: 'Good',
                    2: 'Moderate',
                    3: 'Unhealthy for Sensitive Groups',
                    4: 'Unhealthy',
                    5: 'Very Unhealthy',
                    6: 'Hazardous'
                }
                
                return AirQualityData(
                    aqi=epa_index,
                    level=aqi_levels.get(epa_index, 'Unknown'),
                    pm2_5=values.get('particulateMatter25', 0),
                    pm10=values.get('particulateMatter10', 0),
                    o3=values.get('ozoneLevel', 0),
                    no2=values.get('nitrogenDioxideLevel', 0),
                    co=values.get('carbonMonoxideLevel', 0),
                    so2=values.get('sulphurDioxideLevel', 0)
                )
    
    async def _get_tomorrow_health_insights(self, lat: float, lon: float) -> HealthInsights:
        url = "https://api.tomorrow.io/v4/weather/realtime"
        params = {
            'location': f"{lat},{lon}",
            'apikey': self.tomorrow_api_key,
            'fields': 'temperature,temperatureApparent,humidity,uvIndex,windSpeed,treeIndex,grassIndex,weedIndex'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"Tomorrow.io health insights API error: {response.status}")
                
                data = await response.json()
                
                if 'data' not in data or 'values' not in data.get('data', {}):
                    raise Exception("Invalid Tomorrow.io health insights response")
                
                values = data['data']['values']
                temp = values.get('temperature', 20)
                feels_like = values.get('temperatureApparent', 20)
                humidity = values.get('humidity', 50)
                uv_index = values.get('uvIndex', 0)
                wind_speed = values.get('windSpeed', 0)
                
                heat_index = self._calculate_heat_index(temp, humidity)
                wind_chill = self._calculate_wind_chill(temp, wind_speed)
                
                pollen_scores = [
                    values.get('treeIndex', 0),
                    values.get('grassIndex', 0),
                    values.get('weedIndex', 0)
                ]
                avg_pollen = sum(pollen_scores) / 3 if pollen_scores else 0
                
                pollen_level = 'Low'
                if avg_pollen > 3:
                    pollen_level = 'High'
                elif avg_pollen > 1.5:
                    pollen_level = 'Moderate'
                
                health_tips = self._generate_health_tips(temp, humidity, uv_index, avg_pollen)
                
                return HealthInsights(
                    heat_index=heat_index,
                    wind_chill=wind_chill,
                    pollen_level=pollen_level,
                    air_quality_advice=self._get_air_quality_advice(1),
                    uv_advice=self._get_uv_advice(uv_index),
                    general_health_tips=health_tips,
                    comfort_level='Comfortable',
                    hydration_advice='Stay hydrated',
                    exercise_advice='Good conditions for exercise'
                )
    
    async def _get_tomorrow_forecast(self, lat: float, lon: float, days: int) -> List[Dict]:
        url = "https://api.tomorrow.io/v4/weather/forecast"
        params = {
            'location': f"{lat},{lon}",
            'apikey': self.tomorrow_api_key,
            'fields': 'temperature,temperatureMin,temperatureMax,humidity,pressureSeaLevel,windSpeed,uvIndex,weatherCode,precipitationProbability'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=15) as response:
                if response.status != 200:
                    raise Exception(f"Tomorrow.io forecast API error: {response.status}")
                
                data = await response.json()
                
                if 'timelines' not in data or 'daily' not in data.get('timelines', {}):
                    raise Exception("Invalid Tomorrow.io forecast response")
                
                forecast = []
                daily_data = data['timelines']['daily']
                
                for i, item in enumerate(daily_data[:days]):
                    values = item['values']
                    time_str = item['time']
                    date = datetime.fromisoformat(time_str.replace('Z', '+00:00')).date()
                    
                    weather_code = values.get('weatherCode', 1000)
                    condition, description, icon = self._map_tomorrow_weather_code(weather_code)
                    
                    forecast.append({
                        'date': date.isoformat(),
                        'day_name': date.strftime('%A'),
                        'temperature': {
                            'min': round(values.get('temperatureMin', 0), 1),
                            'max': round(values.get('temperatureMax', 0), 1),
                            'avg': round(values.get('temperature', 0), 1)
                        },
                        'condition': condition,
                        'description': description,
                        'icon': icon,
                        'humidity': values.get('humidity', 0),
                        'pressure': values.get('pressureSeaLevel', 0),
                        'wind_speed': values.get('windSpeed', 0),
                        'uv_index': values.get('uvIndex', 0),
                        'precipitation_probability': values.get('precipitationProbability', 0)
                    })
                
                return forecast
    
    async def _get_visual_crossing_current(self, lat: float, lon: float) -> WeatherData:
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat},{lon}"
        params = {
            'key': self.visual_crossing_api_key,
            'include': 'current',
            'elements': 'temp,feelslike,humidity,pressure,windspeed,winddir,visibility,uvindex,cloudcover,conditions,icon,precip,dew'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"Visual Crossing API error: {response.status}")
                
                data = await response.json()
                
                if 'currentConditions' not in data:
                    raise Exception("Invalid Visual Crossing response")
                
                current = data['currentConditions']
                
                return WeatherData(
                    temperature=current.get('temp', 0),
                    feels_like=current.get('feelslike', 0),
                    humidity=current.get('humidity', 0),
                    pressure=current.get('pressure', 0),
                    wind_speed=current.get('windspeed', 0),
                    wind_direction=current.get('winddir', 0),
                    visibility=current.get('visibility', 0),
                    uv_index=current.get('uvindex', 0),
                    cloud_cover=current.get('cloudcover', 0),
                    condition=self._map_visual_crossing_condition(current.get('conditions', '')),
                    description=current.get('conditions', ''),
                    icon=current.get('icon', ''),
                    timestamp=datetime.now(),
                    precipitation=current.get('precip', 0),
                    dew_point=current.get('dew', 0)
                )
    
    async def _get_visual_crossing_forecast(self, lat: float, lon: float, days: int) -> List[Dict]:
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat},{lon}"
        params = {
            'key': self.visual_crossing_api_key,
            'elements': 'tempmin,tempmax,temp,humidity,pressure,windspeed,uvindex,conditions,icon,precipprob'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=15) as response:
                if response.status != 200:
                    raise Exception(f"Visual Crossing forecast API error: {response.status}")
                
                data = await response.json()
                
                if 'days' not in data:
                    raise Exception("Invalid Visual Crossing forecast response")
                
                forecast = []
                for item in data['days'][:days]:
                    if 'datetime' not in item:
                        continue
                        
                    date = datetime.strptime(item['datetime'], '%Y-%m-%d').date()
                    
                    forecast.append({
                        'date': date.isoformat(),
                        'day_name': date.strftime('%A'),
                        'temperature': {
                            'min': round(item.get('tempmin', 0), 1),
                            'max': round(item.get('tempmax', 0), 1),
                            'avg': round(item.get('temp', 0), 1)
                        },
                        'condition': self._map_visual_crossing_condition(item.get('conditions', '')),
                        'description': item.get('conditions', ''),
                        'icon': item.get('icon', ''),
                        'humidity': item.get('humidity', 0),
                        'pressure': item.get('pressure', 0),
                        'wind_speed': item.get('windspeed', 0),
                        'uv_index': item.get('uvindex', 0),
                        'precipitation_probability': item.get('precipprob', 0)
                    })
                
                return forecast
    
    async def _get_openweather_current(self, lat: float, lon: float) -> WeatherData:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.openweather_api_key,
            'units': 'metric'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"OpenWeather API error: {response.status}")
                
                data = await response.json()
                
                main = data['main']
                weather = data['weather'][0]
                wind = data.get('wind', {})
                
                return WeatherData(
                    temperature=main.get('temp', 0),
                    feels_like=main.get('feels_like', 0),
                    humidity=main.get('humidity', 0),
                    pressure=main.get('pressure', 0),
                    wind_speed=wind.get('speed', 0),
                    wind_direction=wind.get('deg', 0),
                    visibility=data.get('visibility', 0) / 1000,
                    uv_index=0,
                    cloud_cover=data.get('clouds', {}).get('all', 0),
                    condition=weather.get('main', ''),
                    description=weather.get('description', ''),
                    icon=weather.get('icon', ''),
                    timestamp=datetime.fromtimestamp(data.get('dt', 0))
                )
    
    async def _get_openweather_air_quality(self, lat: float, lon: float) -> Optional[AirQualityData]:
        url = "http://api.openweathermap.org/data/2.5/air_pollution"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.openweather_api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"OpenWeather air quality API error: {response.status}")
                
                data = await response.json()
                
                if 'list' not in data or len(data['list']) == 0:
                    raise Exception("Invalid OpenWeather air quality response")
                
                pollution_data = data['list'][0]
                main = pollution_data['main']
                components = pollution_data['components']
                
                aqi_levels = {1: 'Good', 2: 'Fair', 3: 'Moderate', 4: 'Poor', 5: 'Very Poor'}
                
                return AirQualityData(
                    aqi=main.get('aqi', 1),
                    level=aqi_levels.get(main.get('aqi', 1), 'Unknown'),
                    pm2_5=components.get('pm2_5', 0),
                    pm10=components.get('pm10', 0),
                    o3=components.get('o3', 0),
                    no2=components.get('no2', 0),
                    co=components.get('co', 0),
                    so2=components.get('so2', 0)
                )
    
    async def _get_openweather_forecast(self, lat: float, lon: float, days: int) -> List[Dict]:
        url = "https://api.openweathermap.org/data/2.5/forecast"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.openweather_api_key,
            'units': 'metric'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"OpenWeather forecast API error: {response.status}")
                
                data = await response.json()
                
                if 'list' not in data:
                    raise Exception("Invalid OpenWeather forecast response")
                
                daily_data = {}
                for item in data['list']:
                    dt = datetime.fromtimestamp(item['dt'])
                    date_key = dt.date().isoformat()
                    
                    if date_key not in daily_data:
                        daily_data[date_key] = {
                            'temps': [],
                            'humidity': [],
                            'pressure': [],
                            'wind_speed': [],
                            'weather': item['weather'][0]['main'],
                            'description': item['weather'][0]['description'],
                            'icon': item['weather'][0]['icon']
                        }
                    
                    daily_data[date_key]['temps'].append(item['main']['temp'])
                    daily_data[date_key]['humidity'].append(item['main']['humidity'])
                    daily_data[date_key]['pressure'].append(item['main']['pressure'])
                    daily_data[date_key]['wind_speed'].append(item['wind']['speed'])
                
                forecast = []
                for date_str, data_dict in list(daily_data.items())[:days]:
                    date = datetime.fromisoformat(date_str).date()
                    
                    forecast.append({
                        'date': date.isoformat(),
                        'day_name': date.strftime('%A'),
                        'temperature': {
                            'min': round(min(data_dict['temps']), 1),
                            'max': round(max(data_dict['temps']), 1),
                            'avg': round(sum(data_dict['temps']) / len(data_dict['temps']), 1)
                        },
                        'condition': data_dict['weather'],
                        'description': data_dict['description'].title(),
                        'icon': data_dict['icon'],
                        'humidity': round(sum(data_dict['humidity']) / len(data_dict['humidity'])),
                        'pressure': round(sum(data_dict['pressure']) / len(data_dict['pressure'])),
                        'wind_speed': round(sum(data_dict['wind_speed']) / len(data_dict['wind_speed']), 1),
                        'uv_index': 0
                    })
                
                return forecast
    
    def _get_time_context(self, hour: int) -> Dict:
        contexts = {
            (5, 8): {
                'period': 'Early Morning',
                'description': 'Fresh start to the day',
                'energy_level': 'Building',
                'lighting': 'Sunrise/Dawn',
                'typical_activities': ['exercise', 'commuting', 'breakfast']
            },
            (8, 12): {
                'period': 'Morning',
                'description': 'Peak morning energy',
                'energy_level': 'High',
                'lighting': 'Bright daylight',
                'typical_activities': ['work', 'outdoor_activities', 'errands']
            },
            (12, 17): {
                'period': 'Afternoon',
                'description': 'Midday peak activity',
                'energy_level': 'Peak',
                'lighting': 'Full sunlight',
                'typical_activities': ['work', 'lunch', 'outdoor_activities']
            },
            (17, 20): {
                'period': 'Early Evening',
                'description': 'Wind down and social time',
                'energy_level': 'Moderate',
                'lighting': 'Golden hour',
                'typical_activities': ['dinner', 'socializing', 'light_exercise']
            },
            (20, 23): {
                'period': 'Evening',
                'description': 'Relaxation and leisure',
                'energy_level': 'Low to Moderate',
                'lighting': 'Artificial lighting',
                'typical_activities': ['dinner', 'entertainment', 'socializing']
            },
            (23, 5): {
                'period': 'Night',
                'description': 'Rest and sleep time',
                'energy_level': 'Very Low',
                'lighting': 'Dark/Minimal',
                'typical_activities': ['sleep', 'rest', 'quiet_activities']
            }
        }
        
        for (start, end), context in contexts.items():
            if start <= hour < end or (start > end and (hour >= start or hour < end)):
                return context
        
        return contexts[(8, 12)]
    
    def _assess_weather_suitability(self, weather: Dict) -> Dict:
        temp = weather['temperature']['current']
        condition = weather['condition']
        wind_speed = weather['wind']['speed']
        humidity = weather['humidity']
        
        suitability = {
            'outdoor_exercise': 'poor',
            'social_activities': 'good',
            'indoor_activities': 'excellent',
            'water_activities': 'poor',
            'travel': 'good'
        }
        
        if 15 <= temp <= 25:
            suitability['outdoor_exercise'] = 'excellent'
            suitability['social_activities'] = 'excellent'
        elif 10 <= temp <= 30:
            suitability['outdoor_exercise'] = 'good'
            suitability['social_activities'] = 'good'
        elif temp < 5 or temp > 35:
            suitability['outdoor_exercise'] = 'poor'
            suitability['social_activities'] = 'fair'
        
        if condition in ['Rain', 'Thunderstorm']:
            suitability['outdoor_exercise'] = 'poor'
            suitability['social_activities'] = 'fair'
            suitability['travel'] = 'fair'
        elif condition == 'Clear':
            if suitability['outdoor_exercise'] != 'poor':
                suitability['outdoor_exercise'] = 'excellent'
        
        if temp > 22 and condition in ['Clear', 'Clouds']:
            suitability['water_activities'] = 'excellent'
        elif temp > 18 and condition != 'Rain':
            suitability['water_activities'] = 'good'
        
        return suitability
    
    def _calculate_comfort_score(self, weather: Dict) -> Dict:
        temp = weather['temperature']['current']
        feels_like = weather['temperature']['feels_like']
        humidity = weather['humidity']
        wind_speed = weather['wind']['speed']
        condition = weather['condition']
        
        temp_comfort = 100
        if temp < 10 or temp > 30:
            temp_comfort = max(0, 100 - abs(20 - temp) * 5)
        elif 15 <= temp <= 25:
            temp_comfort = 100
        else:
            temp_comfort = max(70, 100 - abs(20 - temp) * 3)
        
        humidity_comfort = 100
        if 40 <= humidity <= 60:
            humidity_comfort = 100
        elif 30 <= humidity <= 70:
            humidity_comfort = 80
        else:
            humidity_comfort = max(30, 100 - abs(50 - humidity) * 2)
        
        wind_comfort = 100
        if wind_speed > 30:
            wind_comfort = 20
        elif wind_speed > 20:
            wind_comfort = 50
        elif wind_speed > 15:
            wind_comfort = 70
        
        condition_modifier = {
            'Clear': 1.0,
            'Clouds': 0.9,
            'Drizzle': 0.7,
            'Rain': 0.5,
            'Snow': 0.6,
            'Thunderstorm': 0.3,
            'Mist': 0.7
        }.get(condition, 0.8)
        
        base_comfort = (temp_comfort + humidity_comfort + wind_comfort) / 3
        overall_comfort = base_comfort * condition_modifier
        
        return {
            'overall': round(overall_comfort, 1),
            'temperature': round(temp_comfort, 1),
            'humidity': round(humidity_comfort, 1),
            'wind': round(wind_comfort, 1),
            'weather_impact': round(condition_modifier * 100, 1),
            'level': 'Excellent' if overall_comfort >= 80 else 'Good' if overall_comfort >= 60 else 'Fair' if overall_comfort >= 40 else 'Poor'
        }
    
    def _get_fallback_recommendations(self, hour: int) -> Dict:
        time_context = self._get_time_context(hour)
        
        if 22 <= hour or hour <= 5:
            return {
                'activities': {
                    'highly_recommended': ['reading', 'relaxation', 'sleep', 'meditation'],
                    'suitable': ['indoor_activities', 'quiet_music', 'light_stretching'],
                    'avoid': ['outdoor_activities', 'intense_exercise', 'loud_activities']
                },
                'time_context': time_context,
                'message': 'Weather data unavailable - showing time-appropriate recommendations'
            }
        elif 6 <= hour <= 10:
            return {
                'activities': {
                    'highly_recommended': ['indoor_exercise', 'reading', 'breakfast', 'light_activities'],
                    'suitable': ['walking', 'stretching', 'planning_day'],
                    'avoid': ['intense_outdoor_sports']
                },
                'time_context': time_context,
                'message': 'Weather data unavailable - showing time-appropriate recommendations'
            }
        else:
            return {
                'activities': {
                    'highly_recommended': ['indoor_activities', 'shopping', 'socializing'],
                    'suitable': ['light_exercise', 'errands', 'entertainment'],
                    'avoid': ['weather_dependent_activities']
                },
                'time_context': time_context,
                'message': 'Weather data unavailable - showing time-appropriate recommendations'
            }
    
    def _format_weather_response(self, weather_data: WeatherData) -> Dict:
        return {
            'temperature': {
                'current': round(weather_data.temperature, 1),
                'feels_like': round(weather_data.feels_like, 1),
                'unit': '°C'
            },
            'condition': weather_data.condition,
            'description': weather_data.description,
            'icon': weather_data.icon,
            'humidity': weather_data.humidity,
            'pressure': weather_data.pressure,
            'wind': {
                'speed': weather_data.wind_speed,
                'direction': weather_data.wind_direction,
                'description': self._get_wind_description(weather_data.wind_speed)
            },
            'visibility': weather_data.visibility,
            'uv_index': weather_data.uv_index,
            'cloud_cover': weather_data.cloud_cover,
            'precipitation': {
                'current': weather_data.precipitation,
                'probability': weather_data.precipitation_probability
            }
        }
    
    def _get_precise_time_period(self, hour: int) -> str:
        if 4 <= hour < 7:
            return 'early_morning'
        elif 7 <= hour < 11:
            return 'morning'
        elif 11 <= hour < 13:
            return 'late_morning'
        elif 13 <= hour < 17:
            return 'afternoon'
        elif 17 <= hour < 19:
            return 'late_afternoon'
        elif 19 <= hour < 22:
            return 'evening'
        else:
            return 'night'
    
    def _get_time_period(self, hour: int) -> str:
        if 6 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 18:
            return 'afternoon'
        elif 18 <= hour < 22:
            return 'evening'
        else:
            return 'night'
    
    def _get_wind_description(self, wind_speed: float) -> str:
        if wind_speed < 1:
            return "Calm"
        elif wind_speed < 5:
            return "Light air"
        elif wind_speed < 11:
            return "Light breeze"
        elif wind_speed < 19:
            return "Gentle breeze"
        elif wind_speed < 28:
            return "Moderate breeze"
        elif wind_speed < 38:
            return "Fresh breeze"
        elif wind_speed < 49:
            return "Strong breeze"
        else:
            return "Strong winds"
    
    def _map_tomorrow_weather_code(self, code: int) -> Tuple[str, str, str]:
        code_map = {
            1000: ('Clear', 'Clear sky', 'clear-day'),
            1100: ('Clouds', 'Mostly clear', 'partly-cloudy-day'),
            1101: ('Clouds', 'Partly cloudy', 'partly-cloudy-day'),
            1102: ('Clouds', 'Mostly cloudy', 'cloudy'),
            1001: ('Clouds', 'Cloudy', 'cloudy'),
            2000: ('Mist', 'Fog', 'fog'),
            2100: ('Mist', 'Light fog', 'fog'),
            4000: ('Drizzle', 'Drizzle', 'rain'),
            4001: ('Rain', 'Rain', 'rain'),
            4200: ('Rain', 'Light rain', 'rain'),
            4201: ('Rain', 'Heavy rain', 'rain'),
            5000: ('Snow', 'Snow', 'snow'),
            5001: ('Snow', 'Flurries', 'snow'),
            5100: ('Snow', 'Light snow', 'snow'),
            5101: ('Snow', 'Heavy snow', 'snow'),
            6000: ('Drizzle', 'Freezing drizzle', 'sleet'),
            6001: ('Rain', 'Freezing rain', 'sleet'),
            6200: ('Rain', 'Light freezing rain', 'sleet'),
            6201: ('Rain', 'Heavy freezing rain', 'sleet'),
            7000: ('Snow', 'Ice pellets', 'sleet'),
            7101: ('Snow', 'Heavy ice pellets', 'sleet'),
            7102: ('Snow', 'Light ice pellets', 'sleet'),
            8000: ('Thunderstorm', 'Thunderstorm', 'thunderstorm')
        }
        return code_map.get(code, ('Clear', 'Unknown condition', 'clear-day'))
    
    def _map_visual_crossing_condition(self, condition: str) -> str:
        condition_lower = condition.lower()
        if 'clear' in condition_lower or 'sunny' in condition_lower:
            return 'Clear'
        elif 'cloud' in condition_lower:
            return 'Clouds'
        elif 'rain' in condition_lower:
            return 'Rain'
        elif 'drizzle' in condition_lower:
            return 'Drizzle'
        elif 'snow' in condition_lower:
            return 'Snow'
        elif 'thunder' in condition_lower or 'storm' in condition_lower:
            return 'Thunderstorm'
        elif 'fog' in condition_lower or 'mist' in condition_lower:
            return 'Mist'
        else:
            return 'Clear'
    
    async def _calculate_comprehensive_health_insights(self, weather: WeatherData, air_quality: Optional[AirQualityData]) -> HealthInsights:
        temp = weather.temperature
        humidity = weather.humidity
        wind_speed = weather.wind_speed
        uv_index = weather.uv_index
        
        heat_index = self._calculate_heat_index(temp, humidity)
        wind_chill = self._calculate_wind_chill(temp, wind_speed)
        
        comfort_level = self._determine_comfort_level(temp, humidity, wind_speed)
        hydration_advice = self._get_hydration_advice(temp, humidity)
        exercise_advice = self._get_exercise_advice(temp, humidity, air_quality)
        
        health_tips = self._generate_comprehensive_health_tips(temp, humidity, uv_index, air_quality)
        
        return HealthInsights(
            heat_index=heat_index,
            wind_chill=wind_chill,
            pollen_level='Unknown',
            air_quality_advice=self._get_air_quality_advice(air_quality.aqi if air_quality else 1),
            uv_advice=self._get_uv_advice(uv_index),
            general_health_tips=health_tips,
            comfort_level=comfort_level,
            hydration_advice=hydration_advice,
            exercise_advice=exercise_advice
        )
    
    def _calculate_heat_index(self, temp: float, humidity: float) -> float:
        if temp < 27:
            return temp
        
        hi = -42.379 + 2.04901523 * temp + 10.14333127 * humidity
        hi -= 0.22475541 * temp * humidity
        hi -= 0.00683783 * temp * temp
        hi -= 0.05481717 * humidity * humidity
        hi += 0.00122874 * temp * temp * humidity
        hi += 0.00085282 * temp * humidity * humidity
        hi -= 0.00000199 * temp * temp * humidity * humidity
        
        return round(hi, 1)
    
    def _calculate_wind_chill(self, temp: float, wind_speed: float) -> float:
        if temp > 10 or wind_speed < 5:
            return temp
        
        wind_chill = 13.12 + 0.6215 * temp - 11.37 * (wind_speed ** 0.16) + 0.3965 * temp * (wind_speed ** 0.16)
        return round(wind_chill, 1)
    
    def _generate_health_tips(self, temp: float, humidity: float, uv_index: float, pollen: float) -> List[str]:
        tips = []
        
        if temp > 30:
            tips.extend(["Stay hydrated", "Seek shade", "Wear light colors"])
        elif temp < 5:
            tips.extend(["Layer clothing", "Protect extremities", "Stay warm"])
        
        if humidity > 80:
            tips.append("High humidity may cause discomfort")
        elif humidity < 30:
            tips.append("Dry air - consider moisturizing")
        
        if uv_index > 7:
            tips.extend(["Use SPF 30+ sunscreen", "Wear sunglasses", "Limit midday sun exposure"])
        elif uv_index > 3:
            tips.append("Moderate UV - protection recommended")
        
        if pollen > 2:
            tips.extend(["High pollen levels", "Close windows", "Consider allergy medication"])
        
        return tips[:5]
    
    def _generate_comprehensive_health_tips(self, temp: float, humidity: float, uv_index: float, air_quality: Optional[AirQualityData]) -> List[str]:
        tips = []
        
        if temp > 30:
            tips.extend([
                "Stay hydrated - drink water every 15-20 minutes",
                "Seek air-conditioned spaces during peak hours",
                "Wear lightweight, light-colored clothing",
                "Avoid strenuous outdoor activities"
            ])
        elif temp < 5:
            tips.extend([
                "Layer clothing to trap warm air",
                "Protect face and extremities from frostbite",
                "Stay active to maintain body heat",
                "Warm up gradually when going indoors"
            ])
        
        if humidity > 80:
            tips.extend([
                "High humidity reduces sweat evaporation",
                "Take frequent breaks in cool areas",
                "Monitor for heat exhaustion symptoms"
            ])
        elif humidity < 30:
            tips.extend([
                "Low humidity - use moisturizer",
                "Stay hydrated to combat dryness",
                "Consider a humidifier indoors"
            ])
        
        if uv_index > 7:
            tips.extend([
                "Apply SPF 30+ sunscreen every 2 hours",
                "Wear UV-blocking sunglasses",
                "Seek shade between 10 AM - 4 PM",
                "Wear protective clothing"
            ])
        
        if air_quality and air_quality.aqi > 100:
            tips.extend([
                "Limit outdoor activities",
                "Keep windows closed",
                "Use air purifiers indoors",
                "Monitor air quality updates"
            ])
        
        return tips[:8]
    
    def _determine_comfort_level(self, temp: float, humidity: float, wind_speed: float) -> str:
        if 20 <= temp <= 25 and 40 <= humidity <= 60 and wind_speed < 15:
            return "Optimal"
        elif 15 <= temp <= 30 and 30 <= humidity <= 70:
            return "Comfortable"
        elif temp > 30 or temp < 10 or humidity > 80:
            return "Uncomfortable"
        else:
            return "Moderate"
    
    def _get_hydration_advice(self, temp: float, humidity: float) -> str:
        if temp > 30 or humidity > 80:
            return "Drink water every 15-20 minutes, increase electrolyte intake"
        elif temp > 25:
            return "Stay well hydrated, drink water regularly"
        elif temp < 10:
            return "Maintain regular hydration despite cool weather"
        else:
            return "Normal hydration recommended"
    
    def _get_exercise_advice(self, temp: float, humidity: float, air_quality: Optional[AirQualityData]) -> str:
        if temp > 32 or (humidity > 80 and temp > 25):
            return "Avoid outdoor exercise, use indoor facilities"
        elif air_quality and air_quality.aqi > 150:
            return "Exercise indoors only due to poor air quality"
        elif 15 <= temp <= 25 and humidity < 70:
            return "Excellent conditions for outdoor exercise"
        elif temp < 5:
            return "Warm up thoroughly before outdoor exercise"
        else:
            return "Moderate conditions - adjust intensity accordingly"
    
    def _get_air_quality_advice(self, aqi: int) -> str:
        if aqi <= 50:
            return "Air quality is excellent - perfect for all outdoor activities"
        elif aqi <= 100:
            return "Air quality is good - suitable for most activities"
        elif aqi <= 150:
            return "Air quality is moderate - sensitive groups should limit prolonged exposure"
        elif aqi <= 200:
            return "Air quality is poor - limit outdoor activities"
        else:
            return "Air quality is hazardous - avoid outdoor activities"
    
    def _get_uv_advice(self, uv_index: float) -> str:
        if uv_index < 3:
            return "Low UV - minimal protection needed"
        elif uv_index < 6:
            return "Moderate UV - protection recommended"
        elif uv_index < 8:
            return "High UV - protection required"
        elif uv_index < 11:
            return "Very high UV - extra protection required"
        else:
            return "Extreme UV - avoid sun exposure"
    
    def _get_time_based_recommendations(self, condition: str, time_period: str, temperature: float) -> Dict:
        weather_recommendations = {
            'Clear': {
                'morning': {
                    'activities': ['Jogging', 'Cycling', 'Outdoor yoga', 'Hiking'],
                    'clothing': ['Light athletic wear', 'Sunglasses', 'Cap'],
                    'health_tips': ['Apply sunscreen', 'Stay hydrated', 'Perfect for exercise']
                },
                'afternoon': {
                    'activities': ['Swimming', 'Indoor activities', 'Shopping'],
                    'clothing': ['Light clothing', 'Sun hat', 'UV protection'],
                    'health_tips': ['Seek shade', 'Increase water intake', 'Avoid prolonged exposure']
                },
                'evening': {
                    'activities': ['Walking', 'Outdoor dining', 'Social gatherings'],
                    'clothing': ['Light layers', 'Comfortable wear'],
                    'health_tips': ['Perfect time for exercise', 'Stay visible']
                },
                'night': {
                    'activities': ['Reading', 'Relaxation', 'Sleep preparation'],
                    'clothing': ['Comfortable indoor wear', 'Light layers'],
                    'health_tips': ['Wind down', 'Prepare for rest']
                }
            },
            'Clouds': {
                'morning': {
                    'activities': ['Running', 'Cycling', 'Any outdoor activity'],
                    'clothing': ['Comfortable layers', 'Athletic wear'],
                    'health_tips': ['Perfect conditions for exercise']
                },
                'afternoon': {
                    'activities': ['Outdoor exploration', 'Sports', 'Walking'],
                    'clothing': ['Casual layers', 'Comfortable clothing'],
                    'health_tips': ['Ideal conditions for activities']
                },
                'evening': {
                    'activities': ['Social gatherings', 'Walking', 'Events'],
                    'clothing': ['Light layers', 'Casual options'],
                    'health_tips': ['Excellent conditions']
                },
                'night': {
                    'activities': ['Indoor activities', 'Relaxation'],
                    'clothing': ['Comfortable layers', 'Cozy wear'],
                    'health_tips': ['Good sleeping conditions']
                }
            },
            'Rain': {
                'morning': {
                    'activities': ['Indoor exercise', 'Reading', 'Cozy indoor activities'],
                    'clothing': ['Indoor comfortable wear'],
                    'health_tips': ['Stay dry and warm']
                },
                'afternoon': {
                    'activities': ['Indoor shopping', 'Museums', 'Movies'],
                    'clothing': ['Comfortable indoor clothing'],
                    'health_tips': ['Perfect for indoor activities']
                },
                'evening': {
                    'activities': ['Reading', 'Movies', 'Relaxation'],
                    'clothing': ['Cozy indoor wear'],
                    'health_tips': ['Enjoy peaceful indoor time']
                },
                'night': {
                    'activities': ['Sleep', 'Reading', 'Rest'],
                    'clothing': ['Comfortable sleepwear'],
                    'health_tips': ['Perfect sleeping weather']
                }
            }
        }
        
        base_recommendations = weather_recommendations.get(condition, {}).get(time_period, {})
        
        if not base_recommendations:
            base_recommendations = weather_recommendations['Clear'][time_period]
        
        recommendations = base_recommendations.copy()
        
        if temperature > 30:
            recommendations['activities'] = ['Indoor activities', 'Swimming', 'Air-conditioned venues']
            recommendations['clothing'].extend(['Cooling accessories', 'Light colors only'])
            recommendations['health_tips'].extend(['Stay very hydrated', 'Avoid heat exposure'])
        elif temperature < 0:
            recommendations['activities'] = ['Indoor activities', 'Warm beverages', 'Cozy indoor time']
            recommendations['clothing'].extend(['Heavy winter gear', 'Warm layers'])
            recommendations['health_tips'].extend(['Stay warm', 'Limit outdoor exposure'])
        
        return recommendations
    
    def _calculate_best_times_detailed(self, forecast_data: List[Dict]) -> Dict:
        if not forecast_data:
            return {}
        
        today_forecast = forecast_data[0] if forecast_data else None
        if not today_forecast:
            return {}
        
        time_periods = ['morning', 'afternoon', 'evening', 'night']
        period_scores = {}
        
        for period in time_periods:
            score = 50
            temp_avg = today_forecast['temperature']['avg']
            condition = today_forecast['condition']
            wind_speed = today_forecast.get('wind_speed', 5)
            humidity = today_forecast.get('humidity', 50)
            uv_index = today_forecast.get('uv_index', 5)
            
            if period == 'morning':
                if 15 <= temp_avg <= 25:
                    score += 25
                if condition == 'Clear':
                    score += 20
                if wind_speed < 10:
                    score += 10
                if humidity < 70:
                    score += 5
                reasons = [
                    "Cool and comfortable temperatures",
                    "Fresh air and energy",
                    "Perfect for exercise",
                    "Good visibility"
                ]
            
            elif period == 'afternoon':
                if 20 <= temp_avg <= 28:
                    score += 20
                elif temp_avg > 35:
                    score -= 15
                if condition in ['Clear', 'Clouds']:
                    score += 15
                if uv_index > 8:
                    score -= 10
                reasons = [
                    "Peak daylight hours",
                    "Warmest part of day",
                    "Good for outdoor activities",
                    "High energy period"
                ]
            
            elif period == 'evening':
                if 18 <= temp_avg <= 26:
                    score += 25
                if condition != 'Thunderstorm':
                    score += 15
                if wind_speed < 15:
                    score += 10
                reasons = [
                    "Comfortable cooling temperatures",
                    "Perfect for relaxation",
                    "Beautiful lighting",
                    "Social activities ideal"
                ]
            
            else:
                if 12 <= temp_avg <= 22:
                    score += 20
                if condition not in ['Thunderstorm', 'Rain']:
                    score += 10
                if humidity > 40:
                    score += 5
                reasons = [
                    "Cool and peaceful",
                    "Perfect for rest",
                    "Low activity requirements",
                    "Comfortable sleeping weather"
                ]
            
            period_scores[period] = {
                'score': max(0, min(100, score)),
                'reasons': reasons[:3],
                'temperature': temp_avg,
                'condition': condition,
                'recommendations': self._get_time_based_recommendations(condition, period, temp_avg)
            }
        
        best_period = max(period_scores.keys(), key=lambda k: period_scores[k]['score'])
        
        period_times = {
            'morning': '6:00 AM - 12:00 PM',
            'afternoon': '12:00 PM - 6:00 PM',
            'evening': '6:00 PM - 10:00 PM',
            'night': '10:00 PM - 6:00 AM'
        }
        
        return {
            'best_period': best_period,
            'best_time_range': period_times[best_period],
            'score': period_scores[best_period]['score'],
            'why_best': period_scores[best_period]['reasons'],
            'all_periods': period_scores,
            'detailed_analysis': {
                'temperature_optimal': period_scores[best_period]['temperature'],
                'weather_favorable': period_scores[best_period]['condition'],
                'activity_suitability': 'High' if period_scores[best_period]['score'] > 75 else 'Moderate' if period_scores[best_period]['score'] > 50 else 'Low'
            }
        }

weather_service = UltraWeatherService()
# backend/app.py
import os
import asyncio
import uuid
import hashlib
import time
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import requests
import random
import base64
import json
import redis
from urllib.parse import urlparse
from functools import wraps
from services.location import location_service, LocationServiceError
from services.weather import weather_service

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*", "supports_credentials": True}})

app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'production-secret-key-change-in-production')
app.config['JSON_SORT_KEYS'] = False

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["3000 per day", "500 per hour"],
    storage_uri="memory://"
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

try:
    redis_url = os.getenv('REDIS_URL')
    if redis_url:
        redis_client = redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=5)
    else:
        redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
    redis_client.ping()
    logger.info("Redis connected successfully")
except Exception as e:
    redis_client = None
    logger.warning(f"Redis not available: {e}, using memory cache")

CACHE_TTLS = {
    'weather_current': 600,
    'weather_forecast': 1800,
    'location_ip': 3600,
    'location_coords': 7200,
    'insights': 1800,
    'spotify': 3600,
    'activities': 1800,
    'ultra_weather': 900,
    'location_search': 86400,
    'ultra_location': 7200
}

SPATIAL_THRESHOLD = 0.01
TEMPORAL_THRESHOLD = 300

request_cache = {}
active_location_requests = {}

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')

WEATHER_FUN_FACTS = [
    "The highest temperature ever recorded on Earth was 134°F (56.7°C) in Death Valley, California in 1913",
    "Lightning strikes the Earth about 100 times every second",
    "Antarctica is the world's largest desert by area, not the Sahara",
    "Modern weather forecasting has a 5-day accuracy rate of approximately 90%",
    "Rainbows are actually full circles, but we typically see only half from ground level",
    "A single cumulus cloud can weigh more than 1 million pounds",
    "The fastest wind speed ever recorded was 253 mph during Tropical Cyclone Olivia in 1996",
    "Snowflakes have six sides due to the molecular structure of water",
    "The Amazon rainforest produces about 20% of the world's oxygen",
    "Tornadoes can have wind speeds exceeding 300 mph",
    "The driest place on Earth is the Atacama Desert in Chile",
    "Hailstones can be as large as grapefruits and fall at 100+ mph",
    "Weather satellites orbit Earth at 22,236 miles above the equator",
    "A single raindrop falls at about 20 mph",
    "The word 'hurricane' comes from the Mayan storm god Hurakan"
]

COMPREHENSIVE_RECOMMENDATIONS = {
    'Clear': {
        'morning': {
            'health_safety': {
                'uv_warning': 'Low to moderate UV levels',
                'air_quality': 'Excellent conditions for outdoor activities',
                'heat_stress': 'Minimal risk, perfect for exercise',
                'safety_tips': ['Apply sunscreen', 'Stay hydrated', 'Perfect for outdoor exercise'],
                'health_benefits': ['Vitamin D absorption', 'Fresh air benefits', 'Mood enhancement']
            },
            'clothing': {
                'recommended': ['Light athletic wear', 'Breathable fabrics', 'Light cap', 'Sunglasses'],
                'avoid': ['Heavy layers', 'Dark colors', 'Non-breathable materials'],
                'accessories': ['Water bottle', 'Sunscreen SPF 30+', 'Fitness tracker'],
                'footwear': ['Running shoes', 'Breathable sneakers', 'Athletic socks']
            },
            'activities': {
                'highly_recommended': ['Jogging', 'Cycling', 'Outdoor yoga', 'Hiking', 'Photography'],
                'suitable': ['Walking', 'Gardening', 'Outdoor sports', 'Picnic preparation'],
                'avoid': ['Heavy outdoor work', 'Prolonged sun exposure'],
                'energy_level': 'High - perfect for active pursuits'
            },
            'spotify_moods': ['workout', 'morning motivation', 'upbeat pop', 'energetic indie'],
            'color_palette': ['#FFD700', '#FFA500', '#87CEEB', '#00BFFF', '#32CD32'],
            'fun_facts': [
                'Morning sunlight helps regulate circadian rhythm',
                'Cool morning air contains more oxygen',
                'Early morning is the best time for vitamin D synthesis'
            ]
        },
        'afternoon': {
            'health_safety': {
                'uv_warning': 'High UV levels - protection required',
                'air_quality': 'Monitor air quality during peak heat',
                'heat_stress': 'Moderate to high risk during peak hours',
                'safety_tips': ['Seek shade 12-4PM', 'Increase water intake', 'Limit strenuous activity'],
                'health_benefits': ['Peak daylight exposure', 'High energy levels', 'Social activity time']
            },
            'clothing': {
                'recommended': ['Lightweight clothing', 'Sun hat', 'UV-protective clothing', 'Sunglasses'],
                'avoid': ['Heavy fabrics', 'Dark colors', 'Tight clothing'],
                'accessories': ['Large water bottle', 'Cooling towel', 'Portable shade'],
                'footwear': ['Ventilated shoes', 'Moisture-wicking socks', 'Sandals for casual wear']
            },
            'activities': {
                'highly_recommended': ['Swimming', 'Beach activities', 'Shaded outdoor dining', 'Indoor sports'],
                'suitable': ['Shopping', 'Museum visits', 'Air-conditioned venues'],
                'avoid': ['Intense outdoor exercise', 'Prolonged sun exposure', 'Heavy physical work'],
                'energy_level': 'Peak - but avoid overheating'
            },
            'spotify_moods': ['summer hits', 'beach vibes', 'pop classics', 'feel good music'],
            'color_palette': ['#FF6B35', '#F7931E', '#FFD23F', '#06FFA5', '#FF4081'],
            'fun_facts': [
                'Peak UV radiation occurs between 12-4 PM',
                'Afternoon light is ideal for photography',
                'Body temperature naturally peaks in afternoon'
            ]
        },
        'evening': {
            'health_safety': {
                'uv_warning': 'Decreasing UV levels - minimal protection needed',
                'air_quality': 'Excellent for outdoor activities',
                'heat_stress': 'Low risk, comfortable conditions',
                'safety_tips': ['Perfect time for exercise', 'Stay visible if walking', 'Hydration still important'],
                'health_benefits': ['Stress relief', 'Social connection', 'Improved sleep preparation']
            },
            'clothing': {
                'recommended': ['Light layers', 'Comfortable casual wear', 'Light jacket if breezy'],
                'avoid': ['Heavy clothing', 'Overdressing'],
                'accessories': ['Light scarf', 'Comfortable shoes', 'Phone light for visibility'],
                'footwear': ['Comfortable walking shoes', 'Casual sneakers', 'Breathable options']
            },
            'activities': {
                'highly_recommended': ['Outdoor dining', 'Walking', 'Social gatherings', 'Sunset viewing'],
                'suitable': ['Light exercise', 'Photography', 'Outdoor events', 'Barbecue'],
                'avoid': ['Intense workouts', 'Heavy meals before activity'],
                'energy_level': 'Moderate to high - ideal for social activities'
            },
            'spotify_moods': ['sunset chill', 'acoustic evening', 'indie folk', 'mellow vibes'],
            'color_palette': ['#FF8C42', '#FF6B35', '#C73E1D', '#592E34', '#8E44AD'],
            'fun_facts': [
                'Golden hour provides the best natural lighting',
                'Evening exercise can improve sleep quality',
                'Sunset colors are caused by light scattering'
            ]
        },
        'night': {
            'health_safety': {
                'uv_warning': 'No UV concern',
                'air_quality': 'Cool, fresh air ideal for relaxation',
                'heat_stress': 'No risk, comfortable conditions',
                'safety_tips': ['Use proper lighting', 'Dress for temperature drop', 'Stay visible'],
                'health_benefits': ['Better sleep preparation', 'Stress reduction', 'Peaceful environment']
            },
            'clothing': {
                'recommended': ['Light layers', 'Comfortable evening wear', 'Light jacket'],
                'avoid': ['Too many layers', 'Uncomfortable clothing'],
                'accessories': ['Light scarf', 'Comfortable shoes', 'Phone/flashlight'],
                'footwear': ['Comfortable shoes', 'Non-slip soles', 'Warm socks if cool']
            },
            'activities': {
                'highly_recommended': ['Stargazing', 'Peaceful walks', 'Outdoor socializing', 'Reading'],
                'suitable': ['Light stretching', 'Meditation', 'Quiet conversations'],
                'avoid': ['Intense exercise', 'Loud activities', 'Heavy meals'],
                'energy_level': 'Low to moderate - focus on relaxation'
            },
            'spotify_moods': ['night jazz', 'ambient sleep', 'lo-fi chill', 'peaceful instrumentals'],
            'color_palette': ['#2C3E50', '#34495E', '#5D6D7E', '#85929E', '#1ABC9C'],
            'fun_facts': [
                'Night air is typically 10-15% more humid',
                'Stars are most visible during astronomical twilight',
                'Cool night air can improve sleep quality'
            ]
        }
    },
    'Clouds': {
        'morning': {
            'health_safety': {
                'uv_warning': 'Reduced UV levels due to cloud cover',
                'air_quality': 'Stable air conditions, good for sensitive individuals',
                'heat_stress': 'Very low risk, ideal exercise conditions',
                'safety_tips': ['Perfect conditions for exercise', 'Light protection still recommended'],
                'health_benefits': ['Comfortable temperature', 'Reduced glare', 'Stable conditions']
            },
            'clothing': {
                'recommended': ['Comfortable layers', 'Light athletic wear', 'Versatile clothing'],
                'avoid': ['Heavy sun protection', 'Overly light clothing'],
                'accessories': ['Light jacket option', 'Water bottle', 'Comfortable shoes'],
                'footwear': ['Athletic shoes', 'Comfortable sneakers', 'Breathable socks']
            },
            'activities': {
                'highly_recommended': ['Running', 'Cycling', 'Outdoor sports', 'Walking', 'Hiking'],
                'suitable': ['Any outdoor activity', 'Photography', 'Exercise', 'Gardening'],
                'avoid': ['Activities requiring specific lighting'],
                'energy_level': 'High - perfect conditions for activity'
            },
            'spotify_moods': ['indie morning', 'alternative rock', 'chill beats', 'acoustic pop'],
            'color_palette': ['#BDC3C7', '#95A5A6', '#7F8C8D', '#AEB6BF', '#5DADE2'],
            'fun_facts': [
                'Cloudy mornings provide even, soft lighting',
                'Cloud cover reduces temperature extremes',
                'Overcast skies can enhance focus and productivity'
            ]
        },
        'afternoon': {
            'health_safety': {
                'uv_warning': 'Moderate UV levels, some protection recommended',
                'air_quality': 'Stable atmospheric conditions',
                'heat_stress': 'Low risk, comfortable for most activities',
                'safety_tips': ['Ideal conditions for most activities', 'Stay hydrated'],
                'health_benefits': ['Comfortable exercise conditions', 'Reduced heat stress']
            },
            'clothing': {
                'recommended': ['Casual layers', 'Comfortable clothing', 'Light options'],
                'avoid': ['Heavy clothing', 'Excessive sun protection'],
                'accessories': ['Light sweater option', 'Comfortable shoes', 'Water bottle'],
                'footwear': ['Casual shoes', 'Comfortable walking shoes', 'Breathable options']
            },
            'activities': {
                'highly_recommended': ['Outdoor exploration', 'Sports', 'Walking', 'Cycling'],
                'suitable': ['Shopping', 'Sightseeing', 'Outdoor dining', 'Photography'],
                'avoid': ['Sun-dependent activities'],
                'energy_level': 'High - excellent for various activities'
            },
            'spotify_moods': ['indie pop', 'alternative hits', 'chill rock', 'acoustic covers'],
            'color_palette': ['#D5DBDB', '#AEB6BF', '#85929E', '#566573', '#3498DB'],
            'fun_facts': [
                'Cloudy afternoons have consistent lighting for photography',
                'Overcast conditions can boost creativity',
                'Cloud cover provides natural air conditioning'
            ]
        },
        'evening': {
            'health_safety': {
                'uv_warning': 'Minimal UV concern',
                'air_quality': 'Stable, comfortable conditions',
                'heat_stress': 'No risk, perfect comfort zone',
                'safety_tips': ['Excellent conditions for evening activities'],
                'health_benefits': ['Stress reduction', 'Comfortable socializing', 'Pleasant atmosphere']
            },
            'clothing': {
                'recommended': ['Comfortable evening wear', 'Light layers', 'Casual options'],
                'avoid': ['Heavy clothing', 'Overdressing'],
                'accessories': ['Light jacket', 'Comfortable shoes', 'Evening accessories'],
                'footwear': ['Comfortable evening shoes', 'Casual sneakers', 'Walking shoes']
            },
            'activities': {
                'highly_recommended': ['Social gatherings', 'Outdoor dining', 'Walking', 'Events'],
                'suitable': ['Exercise', 'Shopping', 'Entertainment', 'Relaxation'],
                'avoid': ['Activities requiring specific weather'],
                'energy_level': 'Moderate to high - great for social activities'
            },
            'spotify_moods': ['evening chill', 'indie acoustic', 'mellow rock', 'coffee shop vibes'],
            'color_palette': ['#909497', '#717D7E', '#515A5A', '#2C3E50', '#8E44AD'],
            'fun_facts': [
                'Cloudy evenings often have dramatic skies',
                'Overcast conditions can enhance mood lighting',
                'Cloud cover moderates evening temperatures'
            ]
        },
        'night': {
            'health_safety': {
                'uv_warning': 'No UV concern',
                'air_quality': 'Stable, mild conditions',
                'heat_stress': 'No risk, comfortable for rest',
                'safety_tips': ['Comfortable conditions for evening activities'],
                'health_benefits': ['Good sleeping conditions', 'Comfortable humidity', 'Peaceful environment']
            },
            'clothing': {
                'recommended': ['Comfortable layers', 'Cozy evening wear', 'Light jacket'],
                'avoid': ['Too light clothing', 'Uncomfortable fabrics'],
                'accessories': ['Comfortable layers', 'Cozy accessories', 'Good shoes'],
                'footwear': ['Comfortable shoes', 'Warm options if cool', 'Non-slip soles']
            },
            'activities': {
                'highly_recommended': ['Indoor activities', 'Cozy socializing', 'Reading', 'Relaxation'],
                'suitable': ['Light walks', 'Indoor entertainment', 'Quiet activities'],
                'avoid': ['Outdoor activities requiring clear skies'],
                'energy_level': 'Low to moderate - focus on comfort'
            },
            'spotify_moods': ['night ambient', 'lo-fi hip hop', 'jazz instrumentals', 'sleep sounds'],
            'color_palette': ['#5D6D7E', '#566573', '#515A5A', '#424949', '#1ABC9C'],
            'fun_facts': [
                'Cloudy nights retain heat better than clear nights',
                'Overcast skies create cozy atmospheric conditions',
                'Cloud cover can improve sleep by reducing temperature fluctuations'
            ]
        }
    },
    'Rain': {
        'morning': {
            'health_safety': {
                'uv_warning': 'Very low UV due to cloud cover',
                'air_quality': 'Rain cleanses air, excellent quality',
                'heat_stress': 'No risk, cool and comfortable',
                'safety_tips': ['Stay dry', 'Watch for slippery surfaces', 'Use proper rain gear'],
                'health_benefits': ['Clean air', 'Natural humidification', 'Peaceful atmosphere']
            },
            'clothing': {
                'recommended': ['Waterproof jacket', 'Rain boots', 'Quick-dry clothing', 'Umbrella'],
                'avoid': ['Cotton clothing', 'Suede/leather', 'Open shoes'],
                'accessories': ['Umbrella', 'Waterproof bag', 'Rain hat', 'Waterproof phone case'],
                'footwear': ['Waterproof boots', 'Non-slip soles', 'Quick-dry socks']
            },
            'activities': {
                'highly_recommended': ['Indoor exercise', 'Cozy café visits', 'Reading', 'Indoor hobbies'],
                'suitable': ['Museum visits', 'Shopping', 'Indoor sports', 'Cooking'],
                'avoid': ['Outdoor sports', 'Electronics outdoors', 'Long outdoor exposure'],
                'energy_level': 'Moderate - focus on indoor activities'
            },
            'spotify_moods': ['rainy day jazz', 'cozy coffee shop', 'indie folk', 'mellow acoustic'],
            'color_palette': ['#5DADE2', '#3498DB', '#2980B9', '#1B4F72', '#85C1E9'],
            'fun_facts': [
                'Rain increases negative ions which can boost mood',
                'The smell of rain is called petrichor',
                'Rain sounds can improve focus and relaxation'
            ]
        },
        'afternoon': {
            'health_safety': {
                'uv_warning': 'Minimal UV exposure',
                'air_quality': 'Excellent due to rain washing pollutants',
                'heat_stress': 'No risk, comfortable temperature',
                'safety_tips': ['Avoid outdoor electrical hazards', 'Drive carefully', 'Stay warm'],
                'health_benefits': ['Fresh air post-rain', 'Comfortable humidity', 'Stress relief from rain sounds']
            },
            'clothing': {
                'recommended': ['Layered rain gear', 'Waterproof clothing', 'Warm layers', 'Rain accessories'],
                'avoid': ['Light clothing', 'Non-waterproof items', 'White/light colors'],
                'accessories': ['Quality umbrella', 'Waterproof bag', 'Warm scarf', 'Rain cover'],
                'footwear': ['Waterproof shoes', 'Good tread', 'Warm socks', 'Quick-dry materials']
            },
            'activities': {
                'highly_recommended': ['Indoor shopping', 'Movie theaters', 'Museums', 'Cozy restaurants'],
                'suitable': ['Indoor sports', 'Libraries', 'Art galleries', 'Indoor entertainment'],
                'avoid': ['Outdoor events', 'Beach activities', 'Picnics'],
                'energy_level': 'Moderate - indoor focus recommended'
            },
            'spotify_moods': ['rainy afternoon', 'indie chill', 'acoustic covers', 'mellow hits'],
            'color_palette': ['#7FB3D3', '#5DADE2', '#3498DB', '#2980B9', '#AED6F1'],
            'fun_facts': [
                'Rainy afternoons are perfect for creativity',
                'Rain can reduce air temperature by 10-15 degrees',
                'The sound of rain is scientifically proven to aid concentration'
            ]
        },
        'evening': {
            'health_safety': {
                'uv_warning': 'No UV concern',
                'air_quality': 'Fresh and clean post-rain',
                'heat_stress': 'No risk, pleasant cool conditions',
                'safety_tips': ['Be cautious of wet surfaces', 'Use good lighting', 'Stay warm'],
                'health_benefits': ['Clean air breathing', 'Relaxing atmosphere', 'Natural cooling']
            },
            'clothing': {
                'recommended': ['Cozy indoor wear', 'Warm layers', 'Comfortable clothing', 'Slippers'],
                'avoid': ['Going out without rain gear', 'Light fabrics'],
                'accessories': ['Warm blanket', 'Hot beverage', 'Cozy socks', 'Comfortable layers'],
                'footwear': ['Indoor shoes', 'Warm socks', 'Comfortable slippers', 'Dry shoes']
            },
            'activities': {
                'highly_recommended': ['Home cooking', 'Reading', 'Movie watching', 'Board games'],
                'suitable': ['Indoor hobbies', 'Online activities', 'Video calls', 'Relaxation'],
                'avoid': ['Outdoor dining', 'Outdoor events', 'Travel if possible'],
                'energy_level': 'Low to moderate - perfect for relaxation'
            },
            'spotify_moods': ['rainy evening', 'cozy jazz', 'ambient relaxation', 'peaceful instrumentals'],
            'color_palette': ['#AED6F1', '#85C1E9', '#5DADE2', '#3498DB', '#2E86AB'],
            'fun_facts': [
                'Rainy evenings create the coziest atmosphere',
                'Rain sounds can lower cortisol levels',
                'Evening rain often brings beautiful clear skies the next day'
            ]
        },
        'night': {
            'health_safety': {
                'uv_warning': 'No UV concern',
                'air_quality': 'Excellent, fresh rain-washed air',
                'heat_stress': 'No risk, cool and comfortable',
                'safety_tips': ['Stay indoors for comfort', 'Keep warm', 'Enjoy the peaceful sounds'],
                'health_benefits': ['Perfect sleeping weather', 'Natural white noise', 'Fresh air']
            },
            'clothing': {
                'recommended': ['Cozy pajamas', 'Warm layers', 'Comfortable sleepwear', 'Warm socks'],
                'avoid': ['Light sleepwear if cold', 'Going outside unnecessarily'],
                'accessories': ['Extra blankets', 'Warm drinks', 'Cozy slippers', 'Comfortable pillows'],
                'footwear': ['Warm slippers', 'Cozy socks', 'Indoor shoes', 'Comfort priority']
            },
            'activities': {
                'highly_recommended': ['Sleeping', 'Reading in bed', 'Meditation', 'Quiet relaxation'],
                'suitable': ['Gentle stretching', 'Journaling', 'Quiet music', 'Rest'],
                'avoid': ['Stimulating activities', 'Going outside', 'Loud activities'],
                'energy_level': 'Very low - perfect for sleep'
            },
            'spotify_moods': ['rain sleep sounds', 'ambient night', 'peaceful meditation', 'sleep music'],
            'color_palette': ['#D6EAF8', '#AED6F1', '#85C1E9', '#5DADE2', '#2E8B57'],
            'fun_facts': [
                'Rain at night creates the best sleeping conditions',
                'Night rain sounds are nature\'s perfect white noise',
                'Rainy nights often have the most restful sleep quality'
            ]
        }
    }
}

def generate_cache_key(*args):
    key_string = ':'.join(str(arg) for arg in args)
    return hashlib.md5(key_string.encode()).hexdigest()

def should_use_cache(location_data, cache_type='weather'):
    if not redis_client:
        return False
    
    current_time = time.time()
    
    if cache_type in ['weather_current', 'weather_forecast', 'ultra_weather']:
        lat = location_data.get('lat', 0)
        lon = location_data.get('lon', 0)
        
        cache_key = generate_cache_key(cache_type, round(lat, 2), round(lon, 2))
        cached_time_key = f"{cache_key}:timestamp"
        
        try:
            cached_timestamp = redis_client.get(cached_time_key)
            if cached_timestamp:
                time_diff = current_time - float(cached_timestamp)
                if time_diff < TEMPORAL_THRESHOLD:
                    return True
        except Exception as e:
            logger.warning(f"Cache check error: {e}")
    
    return False

def get_from_cache(cache_key, cache_type):
    if not redis_client:
        return request_cache.get(cache_key)
    
    try:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            return json.loads(cached_data)
    except Exception as e:
        logger.warning(f"Cache get error: {e}")
        return request_cache.get(cache_key)
    
    return None

def set_cache(cache_key, data, cache_type):
    if redis_client:
        try:
            ttl = CACHE_TTLS.get(cache_type, 600)
            redis_client.setex(cache_key, ttl, json.dumps(data, default=str))
            redis_client.setex(f"{cache_key}:timestamp", ttl, str(time.time()))
            return
        except Exception as e:
            logger.warning(f"Redis cache set error: {e}")
    
    request_cache[cache_key] = data

def get_comprehensive_insights(weather_condition, temperature, time_period):
    base_data = COMPREHENSIVE_RECOMMENDATIONS.get(weather_condition, COMPREHENSIVE_RECOMMENDATIONS['Clear'])
    period_data = base_data.get(time_period, base_data['afternoon'])
    
    insights = period_data.copy()
    
    if temperature:
        if temperature > 30:
            insights['health_safety']['heat_stress'] = 'High risk - take precautions'
            insights['health_safety']['safety_tips'].extend(['Seek air conditioning', 'Limit outdoor time'])
            insights['clothing']['recommended'].extend(['Cooling towels', 'Light colors only'])
            insights['activities']['avoid'].extend(['Intense outdoor exercise', 'Prolonged sun exposure'])
        elif temperature < 5:
            insights['health_safety']['heat_stress'] = 'Cold stress risk'
            insights['health_safety']['safety_tips'].extend(['Layer properly', 'Protect extremities'])
            insights['clothing']['recommended'].extend(['Thermal layers', 'Winter accessories'])
            insights['activities']['highly_recommended'] = ['Indoor activities', 'Warm beverages']
    
    return insights

def calculate_best_time_detailed(forecast_data, weather_condition, current_temp):
    time_periods = ['morning', 'afternoon', 'evening', 'night']
    period_scores = {}
    
    base_temps = {
        'morning': current_temp - 2,
        'afternoon': current_temp + 3,
        'evening': current_temp,
        'night': current_temp - 4
    }
    
    for period in time_periods:
        score = 50
        estimated_temp = base_temps[period]
        
        comfort_score = 0
        if 18 <= estimated_temp <= 25:
            comfort_score = 30
        elif 15 <= estimated_temp <= 28:
            comfort_score = 20
        elif 10 <= estimated_temp <= 32:
            comfort_score = 10
        
        weather_score = 0
        if weather_condition == 'Clear':
            weather_score = 25
        elif weather_condition == 'Clouds':
            weather_score = 20
        elif weather_condition in ['Rain', 'Drizzle']:
            weather_score = 15 if period in ['evening', 'night'] else 10
        elif weather_condition == 'Snow':
            weather_score = 10
        
        period_bonus = {
            'morning': 10 if weather_condition == 'Clear' else 5,
            'afternoon': 15 if weather_condition in ['Clear', 'Clouds'] else 0,
            'evening': 20,
            'night': 10 if weather_condition in ['Rain', 'Snow'] else 5
        }
        
        activity_suitability = 0
        recommendations = COMPREHENSIVE_RECOMMENDATIONS.get(weather_condition, {}).get(period, {})
        activities = recommendations.get('activities', {})
        if len(activities.get('highly_recommended', [])) > 3:
            activity_suitability = 15
        
        total_score = score + comfort_score + weather_score + period_bonus[period] + activity_suitability
        
        reasons = []
        if comfort_score > 20:
            reasons.append(f"Optimal temperature ({estimated_temp:.1f}°C)")
        if weather_score > 15:
            reasons.append(f"Excellent weather conditions")
        if period_bonus[period] > 10:
            reasons.append(f"Ideal time of day for activities")
        if activity_suitability > 10:
            reasons.append("Many suitable activities available")
        
        period_scores[period] = {
            'score': min(100, max(0, total_score)),
            'temperature': estimated_temp,
            'reasons': reasons,
            'condition': weather_condition,
            'recommendations': recommendations
        }
    
    best_period = max(period_scores.keys(), key=lambda k: period_scores[k]['score'])
    
    time_ranges = {
        'morning': '6:00 AM - 12:00 PM',
        'afternoon': '12:00 PM - 6:00 PM',
        'evening': '6:00 PM - 10:00 PM',
        'night': '10:00 PM - 6:00 AM'
    }
    
    return {
        'best_period': best_period,
        'best_time_range': time_ranges[best_period],
        'score': period_scores[best_period]['score'],
        'why_best': period_scores[best_period]['reasons'],
        'expected_temperature': period_scores[best_period]['temperature'],
        'weather_condition': weather_condition,
        'detailed_breakdown': period_scores,
        'confidence': 'High' if period_scores[best_period]['score'] > 75 else 'Medium'
    }

def get_greeting():
    hour = datetime.utcnow().hour
    if hour < 6:
        return "Good Night"
    elif hour < 12:
        return "Good Morning"
    elif hour < 18:
        return "Good Afternoon"
    elif hour < 22:
        return "Good Evening"
    else:
        return "Good Night"

def get_moon_phase():
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day
    
    c = e = jd = b = 0
    
    if month < 3:
        year -= 1
        month += 12
    
    month += 1
    c = 365.25 * year
    e = 30.6 * month
    jd = c + e + day - 694039.09
    jd /= 29.5305882
    b = int(jd)
    jd -= b
    b = round(jd * 8)
    
    if b >= 8:
        b = 0
    
    phases = ['New Moon', 'Waxing Crescent', 'First Quarter', 'Waxing Gibbous',
              'Full Moon', 'Waning Gibbous', 'Last Quarter', 'Waning Crescent']
    emojis = ['🌑', '🌒', '🌓', '🌔', '🌕', '🌖', '🌗', '🌘']
    
    return {'phase': phases[b], 'emoji': emojis[b], 'illumination': round(jd * 100)}

def get_spotify_token():
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        return None
    
    cache_key = "spotify_token"
    cached_token = get_from_cache(cache_key, 'spotify')
    
    if cached_token and cached_token.get('expires_at', 0) > time.time():
        return cached_token.get('token')
    
    auth_string = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    auth_bytes = auth_string.encode('utf-8')
    auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')
    
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    
    try:
        response = requests.post(url, headers=headers, data=data, timeout=10)
        response.raise_for_status()
        token_data = response.json()
        
        token_cache = {
            'token': token_data.get('access_token'),
            'expires_at': time.time() + token_data.get('expires_in', 3600) - 60
        }
        
        set_cache(cache_key, token_cache, 'spotify')
        return token_cache['token']
    except Exception as e:
        logger.error(f"Spotify authentication failed: {e}")
        return None

def clean_expired_requests():
    current_time = time.time()
    expired_keys = [key for key, timestamp in active_location_requests.items() 
                   if current_time - timestamp > 30]
    for key in expired_keys:
        active_location_requests.pop(key, None)

@app.route('/api/location/search', methods=['GET'])
@limiter.limit("100 per hour")
async def search_locations():
    query = request.args.get('q', '').strip()
    
    if not query or len(query) < 2:
        return jsonify({
            'success': False,
            'error': 'Query must be at least 2 characters',
            'results': []
        }), 400
    
    try:
        # Check cache first
        cache_key = generate_cache_key('location_search', query.lower())
        cached_results = get_from_cache(cache_key, 'location_search')
        
        if cached_results:
            logger.info(f"Using cached search results for: {query}")
            return jsonify({
                'success': True,
                'query': query,
                'results': cached_results,
                'cache_hit': True,
                'timestamp': datetime.utcnow().isoformat()
            }), 200
        
        # Search for locations
        results = await location_service.search_location(query)
        
        if not results:
            return jsonify({
                'success': True,
                'query': query,
                'results': [],
                'message': 'No locations found',
                'timestamp': datetime.utcnow().isoformat()
            }), 200
        
        # Cache the results
        set_cache(cache_key, results, 'location_search')
        
        return jsonify({
            'success': True,
            'query': query,
            'results': results,
            'cache_hit': False,
            'timestamp': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Location search error: {e}")
        return jsonify({
            'success': False,
            'error': 'Search service unavailable',
            'code': 'SEARCH_ERROR'
        }), 503

@app.route('/api/location/details', methods=['POST'])
@limiter.limit("100 per hour")
async def get_location_details():
    data = request.get_json() or {}
    place_id = data.get('place_id')
    source = data.get('source', 'google')
    
    if not place_id:
        return jsonify({
            'success': False,
            'error': 'Place ID required'
        }), 400
    
    try:
        # Get detailed location from place ID
        location = await location_service.get_location_from_place_id(place_id, source)
        
        # Format the response
        response = {
            'success': True,
            'location': location.__dict__,
            'display_location': location_service.format_full_location(location),
            'accuracy_score': f"{location.accuracy:.0%}",
            'method': location.provider,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Location details error: {e}")
        return jsonify({
            'success': False,
            'error': 'Could not get location details',
            'code': 'DETAILS_ERROR'
        }), 503

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'service': 'Nimbus API',
        'version': '4.0.0',
        'status': 'operational',
        'environment': os.getenv('FLASK_ENV', 'production'),
        'features': [
            'Ultra-Accurate Location Detection',
            'AccuWeather Integration',
            'Comprehensive Time-Based Recommendations',
            'Smart Caching & Request Optimization',
            'Health & Safety Insights',
            'Clothing & Activity Recommendations',
            'Best Time Analysis',
            'Color Palettes & Fun Facts',
            'Production-Ready Performance'
        ],
        'cache_status': 'Redis' if redis_client else 'Memory',
        'deployment': 'Render Ready'
    }), 200

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'Nimbus API',
        'version': '4.0.0',
        'timestamp': datetime.utcnow().isoformat(),
        'cache_status': 'active' if redis_client else 'memory',
        'environment': os.getenv('FLASK_ENV', 'production')
    }), 200

@app.route('/api/location/auto', methods=['GET'])
@limiter.limit("500 per hour")
def auto_detect_location():
    ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    session_id = request.headers.get('X-Session-ID') or str(uuid.uuid4())
    
    clean_expired_requests()
    
    request_key = f"{ip_address}:{session_id}"
    if request_key in active_location_requests:
        return jsonify({
            'success': False,
            'error': 'Location request already in progress',
            'code': 'REQUEST_IN_PROGRESS'
        }), 429
    
    try:
        active_location_requests[request_key] = time.time()
        
        cache_key = generate_cache_key('location_ip', ip_address)
        
        cached_location = get_from_cache(cache_key, 'location_ip')
        if cached_location:
            logger.info(f"Using cached location for IP: {ip_address}")
            response_data = {
                'success': True,
                'location': cached_location['location'],
                'display_location': cached_location['display_location'],
                'greeting': get_greeting(),
                'moon_phase': get_moon_phase(),
                'session_id': session_id,
                'cache_hit': True,
                'timestamp': datetime.utcnow().isoformat()
            }
            return jsonify(response_data), 200
        
        try:
            logger.info(f"Processing enhanced IP location: {ip_address or 'auto'}")
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            location = loop.run_until_complete(
                location_service.get_location_from_ip_enhanced(ip_address, session_id)
            )
            loop.close()
            
            greeting = get_greeting()
            moon = get_moon_phase()
            full_location = location_service.format_full_location(location)
            
            location_data = {
                'lat': location.lat,
                'lon': location.lon,
                'accuracy': location.accuracy,
                'confidence': location.confidence,
                'provider': location.provider,
                'source_type': location.source_type,
                'city': location.city,
                'state': location.state,
                'country': location.country,
                'country_code': location.country_code,
                'suburb': location.suburb,
                'neighbourhood': location.neighbourhood,
                'road': location.road,
                'house_number': location.house_number,
                'zipcode': location.zipcode,
                'formatted_address': location.formatted_address
            }
            
            cache_data = {
                'location': location_data,
                'display_location': full_location
            }
            set_cache(cache_key, cache_data, 'location_ip')
            
            response_data = {
                'success': True,
                'location': location_data,
                'display_location': full_location,
                'greeting': greeting,
                'moon_phase': moon,
                'session_id': session_id,
                'accuracy_details': {
                    'method': 'Multi-provider consensus',
                    'confidence_score': f"{location.confidence:.2%}",
                    'accuracy_radius': f"{(1-location.accuracy)*100:.1f}km"
                },
                'cache_hit': False,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return jsonify(response_data), 200
            
        except Exception as e:
            logger.error(f"Location error: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'code': 'LOCATION_SERVICE_ERROR'
            }), 503
            
    finally:
        active_location_requests.pop(request_key, None)

@app.route('/api/location/ultra-accurate', methods=['POST'])
@limiter.limit("300 per hour")
def ultra_accurate_location():
    data = request.get_json() or {}
    browser_location = data.get('browser_location')
    session_id = request.headers.get('X-Session-ID') or str(uuid.uuid4())
    ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    cache_key = generate_cache_key('ultra_location', session_id, str(browser_location))
    cached_result = get_from_cache(cache_key, 'ultra_location')
    
    if cached_result:
        cached_result['cache_hit'] = True
        cached_result['timestamp'] = datetime.utcnow().isoformat()
        return jsonify(cached_result), 200
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        location = loop.run_until_complete(
            location_service.get_ultra_accurate_location(
                ip_address=ip_address,
                session_id=session_id,
                browser_location=browser_location
            )
        )
        loop.close()
        
        greeting = get_greeting()
        moon = get_moon_phase()
        full_location = location_service.format_full_location(location)
        
        response_data = {
            'success': True,
            'location': location.__dict__,
            'display_location': full_location,
            'greeting': greeting,
            'moon_phase': moon,
            'accuracy_score': f"{location.accuracy:.2%}",
            'confidence_score': f"{location.confidence:.2%}",
            'method': location.provider,
            'session_id': session_id,
            'cache_hit': False,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        set_cache(cache_key, response_data, 'ultra_location')
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"Ultra location error: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'code': 'ULTRA_LOCATION_ERROR'
        }), 503

@app.route('/api/weather/current', methods=['GET'])
@limiter.limit("500 per hour")
def get_current_weather():
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    
    if not lat or not lon:
        return jsonify({
            'success': False,
            'error': 'Coordinates required',
            'code': 'MISSING_COORDINATES'
        }), 400
    
    cache_key = generate_cache_key('weather_current', round(lat, 2), round(lon, 2))
    
    if should_use_cache({'lat': lat, 'lon': lon}, 'weather_current'):
        cached_weather = get_from_cache(cache_key, 'weather_current')
        if cached_weather:
            logger.info(f"Using cached weather for: {lat}, {lon}")
            cached_weather['cache_hit'] = True
            cached_weather['timestamp'] = datetime.utcnow().isoformat()
            return jsonify(cached_weather), 200
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        weather_data = loop.run_until_complete(
            weather_service.get_current_weather_enhanced(lat, lon)
        )
        loop.close()
        
        current_hour = datetime.now().hour
        if 6 <= current_hour < 12:
            time_period = 'morning'
        elif 12 <= current_hour < 18:
            time_period = 'afternoon'
        elif 18 <= current_hour < 22:
            time_period = 'evening'
        else:
            time_period = 'night'
        
        weather_condition = weather_data['weather']['condition']
        temperature = weather_data['weather']['temperature']['current']
        
        comprehensive_insights = get_comprehensive_insights(weather_condition, temperature, time_period)
        best_time_analysis = calculate_best_time_detailed(None, weather_condition, temperature)
        
        enhanced_response = {
            'success': True,
            'weather': weather_data['weather'],
            'air_quality': weather_data.get('air_quality'),
            'health_insights': weather_data.get('health_insights'),
            'current_time_period': time_period,
            'comprehensive_insights': {
                'health_safety': comprehensive_insights['health_safety'],
                'clothing_recommendations': comprehensive_insights['clothing'],
                'activity_suggestions': comprehensive_insights['activities'],
                'spotify_moods': comprehensive_insights['spotify_moods'],
                'color_palette': comprehensive_insights['color_palette'],
                'fun_facts': comprehensive_insights['fun_facts']
            },
            'best_time_today': best_time_analysis,
            'location': weather_data['location'],
            'cache_hit': False,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        set_cache(cache_key, enhanced_response, 'weather_current')
        return jsonify(enhanced_response), 200
        
    except Exception as e:
        logger.error(f"Weather API error: {e}")
        return jsonify({
            'success': False,
            'error': 'Weather service unavailable',
            'code': 'WEATHER_SERVICE_ERROR'
        }), 503

@app.route('/api/weather/ultra-analysis', methods=['GET'])
@limiter.limit("300 per hour")
def ultra_weather_analysis():
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    
    if not lat or not lon:
        return jsonify({
            'success': False,
            'error': 'Coordinates required',
            'code': 'MISSING_COORDINATES'
        }), 400
    
    cache_key = generate_cache_key('ultra_weather', round(lat, 2), round(lon, 2))
    
    if should_use_cache({'lat': lat, 'lon': lon}, 'ultra_weather'):
        cached_analysis = get_from_cache(cache_key, 'ultra_weather')
        if cached_analysis:
            logger.info(f"Using cached ultra weather analysis for: {lat}, {lon}")
            cached_analysis['cache_hit'] = True
            cached_analysis['timestamp'] = datetime.utcnow().isoformat()
            return jsonify(cached_analysis), 200
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        weather_analysis = loop.run_until_complete(
            weather_service.get_ultra_weather_analysis(lat, lon)
        )
        loop.close()
        
        weather_analysis['cache_hit'] = False
        set_cache(cache_key, weather_analysis, 'ultra_weather')
        
        return jsonify(weather_analysis), 200
        
    except Exception as e:
        logger.error(f"Ultra weather analysis error: {e}")
        return jsonify({
            'success': False,
            'error': 'Ultra weather analysis service unavailable',
            'code': 'ULTRA_WEATHER_ERROR'
        }), 503

@app.route('/api/weather/forecast', methods=['GET'])
@limiter.limit("300 per hour")
def get_forecast():
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    days = request.args.get('days', 7, type=int)
    
    if not lat or not lon:
        return jsonify({
            'success': False,
            'error': 'Coordinates required',
            'code': 'MISSING_COORDINATES'
        }), 400
    
    cache_key = generate_cache_key('weather_forecast', round(lat, 2), round(lon, 2), days)
    
    if should_use_cache({'lat': lat, 'lon': lon}, 'weather_forecast'):
        cached_forecast = get_from_cache(cache_key, 'weather_forecast')
        if cached_forecast:
            logger.info(f"Using cached forecast for: {lat}, {lon}")
            cached_forecast['cache_hit'] = True
            cached_forecast['timestamp'] = datetime.utcnow().isoformat()
            return jsonify(cached_forecast), 200
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        forecast_data = loop.run_until_complete(
            weather_service.get_forecast_enhanced(lat, lon, days)
        )
        loop.close()
        
        enhanced_forecast = []
        for day_forecast in forecast_data['forecast']:
            condition = day_forecast['condition']
            temp_avg = day_forecast['temperature']['avg']
            
            daily_insights = {}
            for period in ['morning', 'afternoon', 'evening', 'night']:
                daily_insights[period] = get_comprehensive_insights(condition, temp_avg, period)
            
            enhanced_day = {
                **day_forecast,
                'period_insights': daily_insights,
                'best_period_score': max([
                    calculate_best_time_detailed(None, condition, temp_avg)['detailed_breakdown'][period]['score']
                    for period in ['morning', 'afternoon', 'evening', 'night']
                ])
            }
            enhanced_forecast.append(enhanced_day)
        
        enhanced_response = {
            'success': True,
            'forecast': enhanced_forecast,
            'best_times': forecast_data.get('best_times', {}),
            'location': forecast_data['location'],
            'cache_hit': False,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        set_cache(cache_key, enhanced_response, 'weather_forecast')
        return jsonify(enhanced_response), 200
        
    except Exception as e:
        logger.error(f"Forecast API error: {e}")
        return jsonify({
            'success': False,
            'error': 'Forecast service unavailable',
            'code': 'FORECAST_SERVICE_ERROR'
        }), 503

@app.route('/api/insights/comprehensive', methods=['GET'])
@limiter.limit("300 per hour")
def get_comprehensive_insights_endpoint():
    weather = request.args.get('weather', 'Clear')
    temp = request.args.get('temp', type=float)
    time_period = request.args.get('time', 'afternoon')
    
    current_hour = datetime.now().hour
    if not time_period:
        if 6 <= current_hour < 12:
            time_period = 'morning'
        elif 12 <= current_hour < 18:
            time_period = 'afternoon'
        elif 18 <= current_hour < 22:
            time_period = 'evening'
        else:
            time_period = 'night'
    
    cache_key = generate_cache_key('insights', weather, temp or 20, time_period)
    cached_insights = get_from_cache(cache_key, 'insights')
    
    if cached_insights:
        cached_insights['cache_hit'] = True
        cached_insights['timestamp'] = datetime.utcnow().isoformat()
        return jsonify(cached_insights), 200
    
    insights = get_comprehensive_insights(weather, temp or 20, time_period)
    best_time = calculate_best_time_detailed(None, weather, temp or 20)
    
    response_data = {
        'success': True,
        'weather_condition': weather,
        'temperature': temp,
        'time_period': time_period,
        'health_safety': insights['health_safety'],
        'clothing_recommendations': insights['clothing'],
        'activity_suggestions': insights['activities'],
        'spotify_moods': insights['spotify_moods'],
        'color_palette': insights['color_palette'],
        'fun_facts': insights['fun_facts'],
        'best_time_today': best_time,
        'cache_hit': False,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    set_cache(cache_key, response_data, 'insights')
    return jsonify(response_data), 200

@app.route('/api/entertainment/spotify', methods=['GET'])
@limiter.limit("200 per hour")
def get_spotify_playlists():
    weather = request.args.get('weather', 'Clear')
    mood = request.args.get('mood', 'happy')
    time_period = request.args.get('time', 'afternoon')
    limit = request.args.get('limit', 6, type=int)
    
    cache_key = generate_cache_key('spotify', weather, mood, time_period, limit)
    cached_playlists = get_from_cache(cache_key, 'spotify')
    
    if cached_playlists:
        cached_playlists['cache_hit'] = True
        cached_playlists['timestamp'] = datetime.utcnow().isoformat()
        return jsonify(cached_playlists), 200
    
    recommendations = COMPREHENSIVE_RECOMMENDATIONS.get(weather, {}).get(time_period, {})
    spotify_moods = recommendations.get('spotify_moods', ['chill vibes'])
    
    query = random.choice(spotify_moods)
    
    token = get_spotify_token()
    
    if not token:
        response_data = {
            'success': False,
            'message': 'Spotify service unavailable',
            'weather': weather,
            'mood': mood,
            'time_period': time_period,
            'suggested_moods': spotify_moods
        }
        return jsonify(response_data), 200
    
    search_url = f"https://api.spotify.com/v1/search?q={query}&type=playlist&limit={limit}"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.get(search_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        playlists = []
        
        if data and 'playlists' in data and data['playlists'] and 'items' in data['playlists']:
            for item in data['playlists']['items'][:limit]:
                if item:
                    playlists.append({
                        'name': item.get('name', 'Unknown Playlist'),
                        'description': item.get('description', f'Perfect for {weather.lower()} {time_period}'),
                        'url': item.get('external_urls', {}).get('spotify', '#'),
                        'image': item.get('images', [{}])[0].get('url') if item.get('images') else None,
                        'tracks': item.get('tracks', {}).get('total', 0),
                        'owner': item.get('owner', {}).get('display_name', 'Spotify'),
                        'followers': item.get('followers', {}).get('total', 0)
                    })
        
        response_data = {
            'success': True,
            'weather': weather,
            'mood': mood,
            'time_period': time_period,
            'query_used': query,
            'suggested_moods': spotify_moods,
            'playlists': playlists,
            'total_found': len(playlists),
            'cache_hit': False,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        set_cache(cache_key, response_data, 'spotify')
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"Spotify API error: {e}")
        return jsonify({
            'success': False,
            'error': 'Spotify service error',
            'weather': weather,
            'mood': mood,
            'time_period': time_period
        }), 200

@app.route('/api/insights/fun-fact', methods=['GET'])
@limiter.limit("200 per hour")
def get_fun_fact():
    category = request.args.get('category', 'weather')
    
    selected_fact = random.choice(WEATHER_FUN_FACTS)
    
    return jsonify({
        'success': True,
        'fact': selected_fact,
        'category': category,
        'total_facts': len(WEATHER_FUN_FACTS),
        'timestamp': datetime.utcnow().isoformat(),
        'share_text': f"🌤️ Weather Fact: {selected_fact}"
    }), 200

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Endpoint not found',
        'code': 404
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.exception(f"Internal server error: {error}")
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'code': 500
    }), 500

@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({
        'success': False,
        'error': 'Rate limit exceeded',
        'code': 429,
        'retry_after': '60 seconds'
    }), 429

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') == 'development'
    
    logger.info("=" * 80)
    logger.info("🌤️  Nimbus API v1.2")
    logger.info("=" * 80)
    logger.info(f"Server: Running on port {port}")
    logger.info(f"Environment: {os.getenv('FLASK_ENV', 'production')}")
    logger.info(f"Cache: {'Redis Active' if redis_client else 'Memory Fallback'}")
    logger.info("✓ Ultra-Accurate Location Detection")
    logger.info("✓ AccuWeather Integration Ready")
    logger.info("✓ Production-Ready Performance")
    logger.info("=" * 80)
    
    app.run(host='0.0.0.0', port=port, debug=debug)
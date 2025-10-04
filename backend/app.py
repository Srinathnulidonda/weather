import os
import requests
import random
import logging
from datetime import datetime, timedelta
from functools import lru_cache, wraps
from flask import Flask, request, jsonify, session
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from collections import defaultdict
import base64
import hashlib
import time

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*", "supports_credentials": True}})

app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
app.config['JSON_SORT_KEYS'] = False
app.config['SESSION_TYPE'] = 'filesystem'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["500 per day", "150 per hour"],
    storage_uri="memory://"
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
IPGEOLOCATION_API_KEY = os.getenv('IPGEOLOCATION_API_KEY', '')

cache_store = {}

def cache_with_expiry(expiry_seconds=300):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            cache_hash = hashlib.md5(cache_key.encode()).hexdigest()
            
            if cache_hash in cache_store:
                cached_data, timestamp = cache_store[cache_hash]
                if time.time() - timestamp < expiry_seconds:
                    return cached_data
            
            result = func(*args, **kwargs)
            cache_store[cache_hash] = (result, time.time())
            return result
        return wrapper
    return decorator

WEATHER_CONDITION_MAP = {
    'Clear': {
        'playlist': 'sunny happy vibes upbeat',
        'playlist_ids': ['37i9dQZF1DXdPec7aLTmlC', '37i9dQZF1DX0UrRvztWcAU'],
        'sound': 'https://cdn.pixabay.com/audio/2022/03/22/audio_1e5d97d57a.mp3',
        'activities': ['Hiking', 'Picnic', 'Beach visit', 'Outdoor photography', 'Cycling', 'Running', 'Gardening', 'BBQ'],
        'mood': 'energetic',
        'color_palette': ['#FFD700', '#FFA500', '#87CEEB', '#00BFFF'],
        'emoji': '☀️',
        'clothing': ['Sunglasses', 'Light clothing', 'Sunscreen', 'Hat', 'Shorts'],
        'health_tips': ['Stay hydrated', 'Use SPF 30+ sunscreen', 'Avoid peak sun hours (10am-4pm)']
    },
    'Clouds': {
        'playlist': 'chill lounge relaxing ambient',
        'playlist_ids': ['37i9dQZF1DX4WYpdgoIcn6', '37i9dQZF1DWTwnEm1IYyoj'],
        'sound': 'https://cdn.pixabay.com/audio/2021/08/09/audio_0625c1539c.mp3',
        'activities': ['Museum visit', 'Shopping', 'Outdoor walk', 'Coffee shop', 'Reading', 'Urban exploration'],
        'mood': 'relaxed',
        'color_palette': ['#808080', '#A9A9A9', '#D3D3D3', '#778899'],
        'emoji': '☁️',
        'clothing': ['Light jacket', 'Comfortable shoes', 'Layers'],
        'health_tips': ['Perfect weather for outdoor activities', 'Good time for vitamin D', 'Stay active']
    },
    'Rain': {
        'playlist': 'rainy day lofi jazz cozy',
        'playlist_ids': ['37i9dQZF1DWXe9gFZP0gtP', '37i9dQZF1DX4PP3DA4J0N8'],
        'sound': 'https://cdn.pixabay.com/audio/2022/03/10/audio_c9054832ff.mp3',
        'activities': ['Movie marathon', 'Reading', 'Indoor cafe', 'Cooking', 'Art & crafts', 'Board games', 'Journaling'],
        'mood': 'cozy',
        'color_palette': ['#4682B4', '#5F9EA0', '#708090', '#2F4F4F'],
        'emoji': '🌧️',
        'clothing': ['Umbrella', 'Raincoat', 'Waterproof shoes', 'Rain boots'],
        'health_tips': ['Boost immune system', 'Stay warm and dry', 'Hot beverages recommended']
    },
    'Drizzle': {
        'playlist': 'rainy day jazz peaceful',
        'playlist_ids': ['37i9dQZF1DX4PP3DA4J0N8', '37i9dQZF1DX0SM0LYsmbMT'],
        'sound': 'https://cdn.pixabay.com/audio/2022/03/10/audio_d0d5b89a6c.mp3',
        'activities': ['Umbrella walk', 'Photography', 'Bookstore visit', 'Tea time', 'Journaling', 'Meditation'],
        'mood': 'contemplative',
        'color_palette': ['#B0C4DE', '#ADD8E6', '#87CEEB', '#6495ED'],
        'emoji': '🌦️',
        'clothing': ['Light rain jacket', 'Umbrella', 'Comfortable shoes'],
        'health_tips': ['Perfect for contemplation', 'Stay moderately active', 'Enjoy the calm']
    },
    'Thunderstorm': {
        'playlist': 'epic cinematic dramatic intense',
        'playlist_ids': ['37i9dQZF1DX4sWSpwq3LiO', '37i9dQZF1DX3Ogo9pFvBkY'],
        'sound': 'https://cdn.pixabay.com/audio/2021/08/04/audio_12b0c7443c.mp3',
        'activities': ['Stay indoors', 'Board games', 'Movie watching', 'Baking', 'Reading', 'Puzzle solving'],
        'mood': 'intense',
        'color_palette': ['#2F4F4F', '#36454F', '#343434', '#800080'],
        'emoji': '⛈️',
        'clothing': ['Stay indoors', 'Emergency kit ready'],
        'health_tips': ['Stay indoors', 'Avoid electrical devices', 'Keep emergency supplies ready']
    },
    'Snow': {
        'playlist': 'cozy winter christmas acoustic',
        'playlist_ids': ['37i9dQZF1DX4E3UdUs7fUx', '37i9dQZF1DX0Yxoavh5qJV'],
        'sound': 'https://cdn.pixabay.com/audio/2022/01/18/audio_12b2c26c8c.mp3',
        'activities': ['Build snowman', 'Hot chocolate', 'Winter photography', 'Sledding', 'Ice skating', 'Skiing'],
        'mood': 'peaceful',
        'color_palette': ['#FFFFFF', '#F0F8FF', '#E0FFFF', '#B0E0E6'],
        'emoji': '❄️',
        'clothing': ['Heavy coat', 'Gloves', 'Scarf', 'Winter boots', 'Thermal layers'],
        'health_tips': ['Layer up', 'Protect extremities', 'Stay warm and dry', 'Watch for ice']
    },
    'Mist': {
        'playlist': 'ambient soundscapes ethereal',
        'playlist_ids': ['37i9dQZF1DX3Ogo9pFvBkY', '37i9dQZF1DX4PP3DA4J0N8'],
        'sound': 'https://cdn.pixabay.com/audio/2021/10/07/audio_bb630cc098.mp3',
        'activities': ['Meditation', 'Yoga', 'Gentle walk', 'Spa day', 'Relaxation', 'Mindfulness'],
        'mood': 'mysterious',
        'color_palette': ['#F5F5F5', '#DCDCDC', '#C0C0C0', '#A9A9A9'],
        'emoji': '🌫️',
        'clothing': ['Light layers', 'Visibility clothing', 'Comfortable shoes'],
        'health_tips': ['Drive carefully', 'Use visibility aids', 'Stay aware of surroundings']
    },
    'Fog': {
        'playlist': 'mysterious calm ambient',
        'playlist_ids': ['37i9dQZF1DX3Ogo9pFvBkY', '37i9dQZF1DWZd79rJ6a7lp'],
        'sound': 'https://cdn.pixabay.com/audio/2021/10/07/audio_bb630cc098.mp3',
        'activities': ['Indoor activities', 'Reading', 'Puzzle solving', 'Tea ceremony', 'Creative writing'],
        'mood': 'calm',
        'color_palette': ['#E5E4E2', '#BCC6CC', '#98AFC7', '#6D7B8D'],
        'emoji': '🌫️',
        'clothing': ['Layers', 'Reflective gear', 'Warm clothing'],
        'health_tips': ['Reduce outdoor activities', 'Use air purifiers indoors', 'Stay hydrated']
    },
    'Haze': {
        'playlist': 'dreamy indie alternative',
        'playlist_ids': ['37i9dQZF1DX2sUQwD7tbmL', '37i9dQZF1DX3YSRoSdA634'],
        'sound': 'https://cdn.pixabay.com/audio/2021/08/09/audio_0625c1539c.mp3',
        'activities': ['Indoor photography', 'Creative writing', 'Music listening', 'Painting', 'Creative projects'],
        'mood': 'dreamy',
        'color_palette': ['#F0E68C', '#EEE8AA', '#FFE4B5', '#FFDAB9'],
        'emoji': '🌫️',
        'clothing': ['Mask recommended', 'Light layers', 'Eye protection'],
        'health_tips': ['Limit outdoor exposure', 'Use air filters', 'Stay hydrated', 'Protect respiratory health']
    },
    'Smoke': {
        'playlist': 'deep focus concentration',
        'playlist_ids': ['37i9dQZF1DWZeKCadgRdKQ', '37i9dQZF1DX3PFzdbtx1Us'],
        'sound': 'https://cdn.pixabay.com/audio/2021/10/07/audio_bb630cc098.mp3',
        'activities': ['Stay indoors', 'Air purification', 'Indoor exercise', 'Work from home', 'Rest'],
        'mood': 'focused',
        'color_palette': ['#696969', '#708090', '#778899', '#2F4F4F'],
        'emoji': '💨',
        'clothing': ['N95 mask', 'Stay indoors', 'Protective gear'],
        'health_tips': ['Stay indoors', 'Use air purifiers', 'Wear N95 masks if going out', 'Monitor air quality']
    },
    'Dust': {
        'playlist': 'desert blues atmospheric',
        'playlist_ids': ['37i9dQZF1DWZd79rJ6a7lp', '37i9dQZF1DX3Ogo9pFvBkY'],
        'sound': 'https://cdn.pixabay.com/audio/2021/08/09/audio_0625c1539c.mp3',
        'activities': ['Indoor activities', 'Museums', 'Indoor sports', 'Movie theaters'],
        'mood': 'cautious',
        'color_palette': ['#D2B48C', '#DEB887', '#F5DEB3', '#FFE4C4'],
        'emoji': '🌪️',
        'clothing': ['Mask', 'Eye protection', 'Cover exposed skin'],
        'health_tips': ['Wear protective masks', 'Seal windows', 'Use air purifiers', 'Stay hydrated']
    },
    'Tornado': {
        'playlist': 'intense classical epic',
        'playlist_ids': ['37i9dQZF1DX4sWSpwq3LiO', '37i9dQZF1DWZBCBq9YLx6e'],
        'sound': 'https://cdn.pixabay.com/audio/2021/08/04/audio_12b0c7443c.mp3',
        'activities': ['Seek shelter immediately', 'Emergency preparedness', 'Safety first'],
        'mood': 'urgent',
        'color_palette': ['#000000', '#2F4F4F', '#696969', '#8B0000'],
        'emoji': '🌪️',
        'clothing': ['Protective gear', 'Emergency supplies'],
        'health_tips': ['Seek shelter immediately', 'Stay in basement/interior room', 'Monitor emergency broadcasts']
    }
}

WEATHER_FUN_FACTS = [
    "The highest temperature ever recorded on Earth was 134°F (56.7°C) in Death Valley, California in 1913",
    "Lightning strikes the Earth about 100 times every second, totaling 8.6 million strikes per day",
    "Raindrops aren't tear-shaped—they're actually more like hamburger buns due to air resistance",
    "Antarctica is the world's largest desert, receiving less than 2 inches of precipitation annually",
    "The fastest wind speed ever recorded was 253 mph during Tropical Cyclone Olivia in 1996",
    "A single lightning bolt contains enough energy to toast 100,000 slices of bread",
    "The world's largest snowflake on record was 15 inches wide, observed in Montana in 1887",
    "Clouds appear white because they reflect sunlight from above in all directions",
    "Modern weather forecasting has a 5-day accuracy rate of approximately 90%",
    "Rainbows are actually full circles, but we typically see only half from ground level",
    "Fog is essentially a cloud that has formed at ground level",
    "There are over 2,000 thunderstorms occurring on Earth at any given moment",
    "The coldest temperature ever recorded was -128.6°F (-89.2°C) at Vostok Station, Antarctica",
    "A single cumulus cloud can weigh more than 1 million pounds due to water content",
    "Weather satellites orbit Earth at speeds of about 17,000 mph",
    "The hottest place on Earth is the Lut Desert in Iran, reaching 159.3°F",
    "A single hurricane can release energy equivalent to 10,000 nuclear bombs",
    "The wettest place on Earth is Mawsynram, India, receiving 467 inches of rain annually",
    "Snowflakes always have six sides due to the molecular structure of ice crystals",
    "The largest hailstone ever recorded was 8 inches in diameter and weighed nearly 2 pounds",
    "Tornadoes can have wind speeds exceeding 300 mph",
    "The eye of a hurricane is typically 20-40 miles wide and completely calm",
    "Diamond dust is a type of precipitation that occurs when ice crystals fall from a clear sky",
    "Ball lightning is a rare weather phenomenon that scientists still don't fully understand",
    "The smell of rain is called 'petrichor' and is caused by oils released from plants and bacteria",
    "Watermelon snow is a real phenomenon caused by algae that makes snow appear pink or red",
    "A full rainbow is actually a complete circle, but ground observers only see the arc",
    "Mammatus clouds look like pouches hanging from the sky and often appear before severe storms",
    "The hottest temperature ever recorded in inhabited areas was 129.2°F in Kuwait in 2016",
    "Virga is rain that evaporates before reaching the ground, creating curtain-like formations"
]

GLOBAL_CITIES = [
    'Tokyo,JP', 'London,UK', 'Paris,FR', 'New York,US', 'Sydney,AU',
    'Dubai,AE', 'Singapore,SG', 'Mumbai,IN', 'Toronto,CA', 'Berlin,DE',
    'Rome,IT', 'Barcelona,ES', 'Rio de Janeiro,BR', 'Cairo,EG', 'Bangkok,TH',
    'Istanbul,TR', 'Seoul,KR', 'Mexico City,MX', 'Moscow,RU', 'Los Angeles,US',
    'Amsterdam,NL', 'Vienna,AT', 'Prague,CZ', 'Buenos Aires,AR', 'Cape Town,ZA',
    'Beijing,CN', 'Hong Kong,HK', 'Lisbon,PT', 'Dublin,IE', 'Copenhagen,DK',
    'Stockholm,SE', 'Oslo,NO', 'Helsinki,FI', 'Athens,GR', 'Zurich,CH',
    'Brussels,BE', 'Warsaw,PL', 'Budapest,HU', 'Kuala Lumpur,MY', 'Jakarta,ID',
    'Manila,PH', 'Hanoi,VN', 'Tehran,IR', 'Baghdad,IQ', 'Riyadh,SA',
    'Lima,PE', 'Bogota,CO', 'Santiago,CL', 'Caracas,VE', 'Havana,CU'
]

CURATED_SPOTIFY_PLAYLISTS = {
    'sunny': [
        {'name': 'Sunny Day Vibes', 'query': 'sunny happy upbeat pop', 'description': 'Perfect for bright sunny days'},
        {'name': 'Beach Essentials', 'query': 'beach tropical summer', 'description': 'Summer beach anthems'},
        {'name': 'Feel Good Energy', 'query': 'feel good energy positive', 'description': 'Uplifting energy boosters'}
    ],
    'rainy': [
        {'name': 'Rainy Day Lofi', 'query': 'lofi hip hop chill beats', 'description': 'Chill beats for rainy days'},
        {'name': 'Jazz in the Rain', 'query': 'jazz piano relaxing', 'description': 'Smooth jazz for stormy weather'},
        {'name': 'Cozy Acoustic', 'query': 'acoustic indie folk', 'description': 'Warm acoustic melodies'}
    ],
    'cloudy': [
        {'name': 'Chill Vibes', 'query': 'chill vibes ambient', 'description': 'Relaxing ambient sounds'},
        {'name': 'Downtempo Electronica', 'query': 'downtempo electronic chill', 'description': 'Electronic chill music'},
        {'name': 'Indie Essentials', 'query': 'indie alternative rock', 'description': 'Indie rock favorites'}
    ],
    'snow': [
        {'name': 'Winter Wonderland', 'query': 'winter christmas acoustic', 'description': 'Cozy winter sounds'},
        {'name': 'Fireside Sessions', 'query': 'acoustic guitar instrumental', 'description': 'Warm acoustic melodies'},
        {'name': 'Peaceful Piano', 'query': 'peaceful piano classical', 'description': 'Calm piano compositions'}
    ],
    'storm': [
        {'name': 'Epic Soundtracks', 'query': 'epic cinematic orchestral', 'description': 'Dramatic orchestral music'},
        {'name': 'Intense Focus', 'query': 'intense focus concentration', 'description': 'Deep concentration music'},
        {'name': 'Dark Academia', 'query': 'classical dark orchestral', 'description': 'Dark classical themes'}
    ]
}

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

@lru_cache(maxsize=10)
def get_spotify_token():
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        logger.warning("Spotify credentials not configured")
        return None
    
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
        return token_data.get('access_token')
    except Exception as e:
        logger.error(f"Spotify authentication failed: {e}")
        return None

@cache_with_expiry(expiry_seconds=300)
def get_location_from_ip(ip_address=None):
    try:
        if IPGEOLOCATION_API_KEY:
            url = f"https://api.ipgeolocation.io/ipgeo?apiKey={IPGEOLOCATION_API_KEY}"
            if ip_address:
                url += f"&ip={ip_address}"
            response = requests.get(url, timeout=5)
            data = response.json()
            return {
                'city': data.get('city', 'Unknown'),
                'country': data.get('country_name', 'Unknown'),
                'lat': float(data.get('latitude', 0)),
                'lon': float(data.get('longitude', 0)),
                'timezone': data.get('time_zone', {}).get('name', 'UTC'),
                'state': data.get('state_prov', ''),
                'zipcode': data.get('zipcode', '')
            }
        else:
            url = f"http://ip-api.com/json/{ip_address}" if ip_address else "http://ip-api.com/json/"
            response = requests.get(url, timeout=5)
            data = response.json()
            return {
                'city': data.get('city', 'Unknown'),
                'country': data.get('country', 'Unknown'),
                'lat': data.get('lat', 0),
                'lon': data.get('lon', 0),
                'timezone': data.get('timezone', 'UTC'),
                'state': data.get('regionName', ''),
                'zipcode': data.get('zip', '')
            }
    except Exception as e:
        logger.error(f"IP geolocation failed: {e}")
        return {
            'city': 'New York',
            'country': 'USA',
            'lat': 40.7128,
            'lon': -74.0060,
            'timezone': 'America/New_York',
            'state': 'New York',
            'zipcode': '10001'
        }

def get_greeting(timezone='UTC'):
    try:
        hour = datetime.utcnow().hour
        
        greetings = {
            (0, 5): ["Good Night", "Sleep Well", "Sweet Dreams"],
            (5, 12): ["Good Morning", "Rise and Shine", "Morning Sunshine"],
            (12, 17): ["Good Afternoon", "Have a Great Day", "Afternoon Delight"],
            (17, 21): ["Good Evening", "Evening Greetings", "Pleasant Evening"],
            (21, 24): ["Good Night", "Evening Relaxation", "Peaceful Night"]
        }
        
        for (start, end), messages in greetings.items():
            if start <= hour < end:
                return random.choice(messages)
        
        return "Hello"
    except:
        return "Hello"

def calculate_uv_index(lat, lon):
    try:
        url = f"https://api.openweathermap.org/data/2.5/uvi?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
        response = requests.get(url, timeout=5)
        data = response.json()
        uv_value = data.get('value', 0)
        
        if uv_value < 3:
            level = 'Low'
            advice = 'No protection required'
        elif uv_value < 6:
            level = 'Moderate'
            advice = 'Protection required'
        elif uv_value < 8:
            level = 'High'
            advice = 'Protection required'
        elif uv_value < 11:
            level = 'Very High'
            advice = 'Extra protection required'
        else:
            level = 'Extreme'
            advice = 'Avoid sun exposure'
        
        return {'value': uv_value, 'level': level, 'advice': advice}
    except:
        return None

def get_air_quality(lat, lon):
    try:
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
        response = requests.get(url, timeout=5)
        data = response.json()
        aqi = data['list'][0]['main']['aqi']
        components = data['list'][0]['components']
        
        aqi_levels = {
            1: {'level': 'Good', 'color': '#00e400', 'advice': 'Air quality is perfect. Great day for outdoor activities!'},
            2: {'level': 'Fair', 'color': '#ffff00', 'advice': 'Air quality is acceptable. Sensitive groups should limit prolonged outdoor exertion.'},
            3: {'level': 'Moderate', 'color': '#ff7e00', 'advice': 'Members of sensitive groups may experience health effects.'},
            4: {'level': 'Poor', 'color': '#ff0000', 'advice': 'Everyone may begin to experience health effects.'},
            5: {'level': 'Very Poor', 'color': '#8f3f97', 'advice': 'Health alert! Everyone may experience more serious health effects.'}
        }
        
        aqi_info = aqi_levels.get(aqi, aqi_levels[1])
        
        return {
            'aqi': aqi,
            'level': aqi_info['level'],
            'color': aqi_info['color'],
            'advice': aqi_info['advice'],
            'components': {
                'pm2_5': round(components.get('pm2_5', 0), 2),
                'pm10': round(components.get('pm10', 0), 2),
                'o3': round(components.get('o3', 0), 2),
                'no2': round(components.get('no2', 0), 2),
                'co': round(components.get('co', 0), 2),
                'so2': round(components.get('so2', 0), 2)
            }
        }
    except Exception as e:
        logger.error(f"Air quality fetch failed: {e}")
        return None

def calculate_weather_score(data):
    score = 50
    
    temp = data.get('temperature', {}).get('current', 20)
    if 18 <= temp <= 25:
        score += 20
    elif 15 <= temp <= 30:
        score += 10
    else:
        score -= 10
    
    humidity = data.get('details', {}).get('humidity', 50)
    if 30 <= humidity <= 60:
        score += 15
    elif humidity > 80:
        score -= 10
    
    wind_speed = data.get('details', {}).get('wind', {}).get('speed', 0)
    if wind_speed < 5:
        score += 10
    elif wind_speed > 15:
        score -= 10
    
    weather_main = data.get('weather', {}).get('main', 'Clear')
    if weather_main == 'Clear':
        score += 15
    elif weather_main in ['Rain', 'Thunderstorm', 'Snow']:
        score -= 15
    
    return max(0, min(100, score))

def get_best_time_today(forecast_data):
    if not forecast_data or 'list' not in forecast_data:
        return None
    
    best_time = None
    best_score = 0
    
    for item in forecast_data['list'][:8]:
        temp = item['main']['temp']
        weather = item['weather'][0]['main']
        wind = item['wind']['speed']
        
        score = 50
        if 18 <= temp <= 25:
            score += 30
        if weather == 'Clear':
            score += 20
        if wind < 5:
            score += 10
        
        if score > best_score:
            best_score = score
            best_time = {
                'time': datetime.fromtimestamp(item['dt']).strftime('%I:%M %p'),
                'temperature': round(temp, 1),
                'weather': weather,
                'score': score
            }
    
    return best_time

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'Weather Visualizer API Pro',
        'version': '2.0.0',
        'timestamp': datetime.utcnow().isoformat(),
        'features': ['Weather', 'Forecast', 'Air Quality', 'UV Index', 'Spotify', 'Moon Phase', 'Activities']
    }), 200

@app.route('/api/location/auto', methods=['GET'])
@limiter.limit("100 per hour")
def auto_detect_location():
    ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    location = get_location_from_ip(ip_address)
    greeting = get_greeting(location.get('timezone', 'UTC'))
    
    moon = get_moon_phase()
    
    return jsonify({
        'success': True,
        'location': location,
        'greeting': greeting,
        'moon_phase': moon,
        'timestamp': datetime.utcnow().isoformat()
    }), 200

@app.route('/api/weather/current', methods=['GET'])
@limiter.limit("150 per hour")
def get_current_weather():
    if not OPENWEATHER_API_KEY:
        return jsonify({'error': 'Weather service not configured', 'success': False}), 500
    
    city = request.args.get('city')
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    units = request.args.get('units', 'metric')
    
    if not city and not (lat and lon):
        ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        location = get_location_from_ip(ip_address)
        lat, lon = location['lat'], location['lon']
    
    if lat and lon:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units={units}&appid={OPENWEATHER_API_KEY}"
    else:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&units={units}&appid={OPENWEATHER_API_KEY}"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        weather_main = data['weather'][0]['main']
        weather_config = WEATHER_CONDITION_MAP.get(weather_main, WEATHER_CONDITION_MAP['Clear'])
        
        uv_index = calculate_uv_index(data['coord']['lat'], data['coord']['lon'])
        air_quality = get_air_quality(data['coord']['lat'], data['coord']['lon'])
        
        result = {
            'success': True,
            'location': {
                'name': data.get('name'),
                'country': data['sys'].get('country'),
                'coordinates': {
                    'lat': data['coord'].get('lat'),
                    'lon': data['coord'].get('lon')
                },
                'timezone': data.get('timezone')
            },
            'weather': {
                'main': weather_main,
                'description': data['weather'][0]['description'].title(),
                'icon': data['weather'][0]['icon'],
                'mood': weather_config['mood'],
                'emoji': weather_config['emoji']
            },
            'temperature': {
                'current': round(data['main']['temp'], 1),
                'feels_like': round(data['main']['feels_like'], 1),
                'min': round(data['main']['temp_min'], 1),
                'max': round(data['main']['temp_max'], 1),
                'unit': '°C' if units == 'metric' else '°F'
            },
            'details': {
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'visibility': round(data.get('visibility', 0) / 1000, 1),
                'wind': {
                    'speed': data['wind']['speed'],
                    'deg': data['wind'].get('deg'),
                    'gust': data['wind'].get('gust'),
                    'unit': 'm/s' if units == 'metric' else 'mph'
                },
                'clouds': data['clouds']['all'],
                'uv_index': uv_index,
                'air_quality': air_quality
            },
            'precipitation': {
                'rain_1h': data.get('rain', {}).get('1h', 0),
                'rain_3h': data.get('rain', {}).get('3h', 0),
                'snow_1h': data.get('snow', {}).get('1h', 0),
                'snow_3h': data.get('snow', {}).get('3h', 0)
            },
            'sun': {
                'sunrise': datetime.fromtimestamp(data['sys']['sunrise']).isoformat(),
                'sunset': datetime.fromtimestamp(data['sys']['sunset']).isoformat()
            },
            'color_palette': weather_config['color_palette'],
            'clothing_recommendations': weather_config['clothing'],
            'health_tips': weather_config['health_tips'],
            'timestamp': datetime.fromtimestamp(data['dt']).isoformat()
        }
        
        result['weather_score'] = calculate_weather_score(result)
        
        return jsonify(result), 200
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Weather API request failed: {e}")
        return jsonify({'error': 'Weather service unavailable', 'success': False}), 503

@app.route('/api/weather/forecast', methods=['GET'])
@limiter.limit("150 per hour")
def get_forecast():
    if not OPENWEATHER_API_KEY:
        return jsonify({'error': 'Weather service not configured', 'success': False}), 500
    
    city = request.args.get('city')
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    units = request.args.get('units', 'metric')
    days = request.args.get('days', 7, type=int)
    include_hourly = request.args.get('hourly', 'false').lower() == 'true'
    
    if not city and not (lat and lon):
        ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        location = get_location_from_ip(ip_address)
        lat, lon = location['lat'], location['lon']
    
    if city and not (lat and lon):
        geo_url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={OPENWEATHER_API_KEY}"
        try:
            geo_response = requests.get(geo_url, timeout=5)
            geo_data = geo_response.json()
            if geo_data:
                lat, lon = geo_data[0]['lat'], geo_data[0]['lon']
        except:
            pass
    
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units={units}&appid={OPENWEATHER_API_KEY}"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        daily_forecast = {}
        hourly_forecast = []
        
        for item in data['list'][:min(40, days * 8)]:
            dt = datetime.fromtimestamp(item['dt'])
            date_key = dt.date().isoformat()
            
            if include_hourly:
                weather_main = item['weather'][0]['main']
                weather_config = WEATHER_CONDITION_MAP.get(weather_main, WEATHER_CONDITION_MAP['Clear'])
                
                hourly_forecast.append({
                    'datetime': dt.isoformat(),
                    'temperature': round(item['main']['temp'], 1),
                    'feels_like': round(item['main']['feels_like'], 1),
                    'weather': {
                        'main': weather_main,
                        'description': item['weather'][0]['description'].title(),
                        'icon': item['weather'][0]['icon'],
                        'emoji': weather_config['emoji']
                    },
                    'humidity': item['main']['humidity'],
                    'wind_speed': item['wind']['speed'],
                    'precipitation_probability': round(item.get('pop', 0) * 100),
                    'clouds': item['clouds']['all']
                })
            
            if date_key not in daily_forecast:
                daily_forecast[date_key] = {
                    'date': date_key,
                    'temps': [],
                    'weather': item['weather'][0]['main'],
                    'description': item['weather'][0]['description'].title(),
                    'icon': item['weather'][0]['icon'],
                    'humidity': [],
                    'wind_speed': [],
                    'pop': [],
                    'pressure': []
                }
            
            daily_forecast[date_key]['temps'].append(item['main']['temp'])
            daily_forecast[date_key]['humidity'].append(item['main']['humidity'])
            daily_forecast[date_key]['wind_speed'].append(item['wind']['speed'])
            daily_forecast[date_key]['pop'].append(item.get('pop', 0) * 100)
            daily_forecast[date_key]['pressure'].append(item['main']['pressure'])
        
        daily_summary = []
        for date, forecast in list(daily_forecast.items())[:days]:
            weather_config = WEATHER_CONDITION_MAP.get(forecast['weather'], WEATHER_CONDITION_MAP['Clear'])
            
            daily_summary.append({
                'date': date,
                'day_name': datetime.fromisoformat(date).strftime('%A'),
                'temperature': {
                    'min': round(min(forecast['temps']), 1),
                    'max': round(max(forecast['temps']), 1),
                    'avg': round(sum(forecast['temps']) / len(forecast['temps']), 1)
                },
                'weather': {
                    'main': forecast['weather'],
                    'description': forecast['description'],
                    'icon': forecast['icon'],
                    'emoji': weather_config['emoji']
                },
                'humidity': round(sum(forecast['humidity']) / len(forecast['humidity'])),
                'wind_speed': round(sum(forecast['wind_speed']) / len(forecast['wind_speed']), 1),
                'precipitation_probability': round(max(forecast['pop'])),
                'pressure': round(sum(forecast['pressure']) / len(forecast['pressure']))
            })
        
        city_data = data.get('city', {})
        coord_data = city_data.get('coord', {})
        
        result = {
            'success': True,
            'location': {
                'name': city_data.get('name', 'Unknown'),
                'country': city_data.get('country', 'Unknown'),
                'coordinates': {
                    'lat': coord_data.get('lat', lat),
                    'lon': coord_data.get('lon', lon)
                }
            },
            'daily': daily_summary,
            'best_time_today': get_best_time_today(data),
            'unit': '°C' if units == 'metric' else '°F'
        }
        
        if include_hourly:
            result['hourly'] = hourly_forecast
        
        return jsonify(result), 200
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Forecast API request failed: {e}")
        return jsonify({'error': 'Forecast service unavailable', 'success': False}), 503

@app.route('/api/weather/alerts', methods=['GET'])
@limiter.limit("100 per hour")
def get_weather_alerts():
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    
    if not lat or not lon:
        ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        location = get_location_from_ip(ip_address)
        lat, lon = location['lat'], location['lon']
    
    try:
        url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude=minutely,hourly,daily&appid={OPENWEATHER_API_KEY}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        alerts = data.get('alerts', [])
        
        formatted_alerts = []
        for alert in alerts:
            formatted_alerts.append({
                'event': alert.get('event'),
                'severity': 'high' if 'warning' in alert.get('event', '').lower() else 'medium',
                'start': datetime.fromtimestamp(alert.get('start')).isoformat(),
                'end': datetime.fromtimestamp(alert.get('end')).isoformat(),
                'description': alert.get('description'),
                'sender': alert.get('sender_name'),
                'tags': alert.get('tags', [])
            })
        
        return jsonify({
            'success': True,
            'location': {'lat': lat, 'lon': lon},
            'alerts': formatted_alerts,
            'count': len(formatted_alerts),
            'has_alerts': len(formatted_alerts) > 0
        }), 200
        
    except:
        return jsonify({
            'success': True,
            'alerts': [],
            'count': 0,
            'has_alerts': False
        }), 200

@app.route('/api/weather/compare', methods=['GET'])
@limiter.limit("50 per hour")
def compare_cities():
    cities = request.args.get('cities', '').split(',')
    units = request.args.get('units', 'metric')
    
    if len(cities) > 5:
        return jsonify({'error': 'Maximum 5 cities allowed', 'success': False}), 400
    
    comparison = []
    
    for city in cities[:5]:
        city = city.strip()
        if not city:
            continue
        
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&units={units}&appid={OPENWEATHER_API_KEY}"
        
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            
            weather_config = WEATHER_CONDITION_MAP.get(data['weather'][0]['main'], WEATHER_CONDITION_MAP['Clear'])
            
            comparison.append({
                'city': data['name'],
                'country': data['sys']['country'],
                'temperature': round(data['main']['temp'], 1),
                'feels_like': round(data['main']['feels_like'], 1),
                'weather': {
                    'main': data['weather'][0]['main'],
                    'description': data['weather'][0]['description'].title(),
                    'emoji': weather_config['emoji']
                },
                'humidity': data['main']['humidity'],
                'wind_speed': data['wind']['speed']
            })
        except:
            continue
    
    return jsonify({
        'success': True,
        'comparison': comparison,
        'count': len(comparison)
    }), 200

@app.route('/api/weather/explore', methods=['GET'])
@limiter.limit("50 per hour")
def explore_random_weather():
    count = request.args.get('count', 4, type=int)
    count = min(count, 8)
    units = request.args.get('units', 'metric')
    
    cities = random.sample(GLOBAL_CITIES, count)
    results = []
    
    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&units={units}&appid={OPENWEATHER_API_KEY}"
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            
            weather_config = WEATHER_CONDITION_MAP.get(data['weather'][0]['main'], WEATHER_CONDITION_MAP['Clear'])
            
            results.append({
                'city': data['name'],
                'country': data['sys']['country'],
                'temperature': round(data['main']['temp'], 1),
                'feels_like': round(data['main']['feels_like'], 1),
                'weather': {
                    'main': data['weather'][0]['main'],
                    'description': data['weather'][0]['description'].title(),
                    'icon': data['weather'][0]['icon'],
                    'emoji': weather_config['emoji']
                },
                'coordinates': {
                    'lat': data['coord']['lat'],
                    'lon': data['coord']['lon']
                },
                'local_time': datetime.utcnow().isoformat()
            })
        except:
            continue
    
    return jsonify({
        'success': True,
        'cities': results,
        'count': len(results)
    }), 200

@app.route('/api/insights/fun-fact', methods=['GET'])
def get_fun_fact():
    category = request.args.get('category', 'all')
    
    return jsonify({
        'success': True,
        'fact': random.choice(WEATHER_FUN_FACTS),
        'category': 'weather',
        'total_facts': len(WEATHER_FUN_FACTS),
        'timestamp': datetime.utcnow().isoformat()
    }), 200

@app.route('/api/insights/activities', methods=['GET'])
def get_activity_suggestions():
    weather = request.args.get('weather', 'Clear')
    temp = request.args.get('temp', type=float)
    
    weather_config = WEATHER_CONDITION_MAP.get(weather, WEATHER_CONDITION_MAP['Clear'])
    activities = weather_config['activities'].copy()
    
    if temp:
        if temp > 30:
            activities.extend(['Swimming', 'Water park', 'Indoor AC activities', 'Ice cream hunt', 'Beach volleyball'])
        elif temp > 25:
            activities.extend(['Outdoor sports', 'Park picnic', 'Cycling'])
        elif temp < 5:
            activities.extend(['Indoor activities', 'Hot beverages', 'Cozy indoor time', 'Winter sports'])
        elif temp < 15:
            activities.extend(['Brisk walk', 'Layered outdoor activities'])
    
    activities = list(set(activities))
    suggested = random.sample(activities, min(5, len(activities)))
    
    return jsonify({
        'success': True,
        'weather': weather,
        'temperature': temp,
        'mood': weather_config['mood'],
        'suggested_activities': suggested,
        'all_activities': activities,
        'emoji': weather_config['emoji']
    }), 200

@app.route('/api/entertainment/spotify', methods=['GET'])
@limiter.limit("50 per hour")
def get_spotify_playlists():
    weather = request.args.get('weather', 'Clear')
    limit = request.args.get('limit', 5, type=int)
    
    weather_config = WEATHER_CONDITION_MAP.get(weather, WEATHER_CONDITION_MAP['Clear'])
    
    weather_category = 'sunny'
    if weather in ['Rain', 'Drizzle']:
        weather_category = 'rainy'
    elif weather in ['Clouds', 'Mist', 'Fog', 'Haze']:
        weather_category = 'cloudy'
    elif weather == 'Snow':
        weather_category = 'snow'
    elif weather in ['Thunderstorm', 'Tornado']:
        weather_category = 'storm'
    
    curated = CURATED_SPOTIFY_PLAYLISTS.get(weather_category, CURATED_SPOTIFY_PLAYLISTS['sunny'])
    
    token = get_spotify_token()
    
    if not token:
        return jsonify({
            'success': False,
            'message': 'Spotify service unavailable',
            'weather': weather,
            'mood': weather_config['mood'],
            'curated_suggestions': curated
        }), 200
    
    all_playlists = []
    
    for playlist_info in curated:
        search_url = f"https://api.spotify.com/v1/search?q={playlist_info['query']}&type=playlist&limit=2"
        headers = {"Authorization": f"Bearer {token}"}
        
        try:
            response = requests.get(search_url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data and 'playlists' in data and data['playlists'] and 'items' in data['playlists']:
                for item in data['playlists']['items'][:2]:
                    all_playlists.append({
                        'name': item['name'],
                        'description': item.get('description', playlist_info['description']),
                        'url': item['external_urls']['spotify'],
                        'image': item['images'][0]['url'] if item['images'] else None,
                        'tracks': item['tracks']['total'],
                        'owner': item['owner']['display_name'],
                        'category': weather_category
                    })
        except Exception as e:
            logger.error(f"Spotify search error for {playlist_info['query']}: {e}")
            continue
    
    if not all_playlists:
        return jsonify({
            'success': False,
            'message': 'No playlists found',
            'weather': weather,
            'mood': weather_config['mood'],
            'curated_suggestions': curated
        }), 200
    
    return jsonify({
        'success': True,
        'weather': weather,
        'mood': weather_config['mood'],
        'category': weather_category,
        'playlists': all_playlists[:limit],
        'total_found': len(all_playlists)
    }), 200

@app.route('/api/entertainment/sounds', methods=['GET'])
def get_ambient_sounds():
    weather = request.args.get('weather', 'Clear')
    
    weather_config = WEATHER_CONDITION_MAP.get(weather, WEATHER_CONDITION_MAP['Clear'])
    
    all_sounds = {k: {
        'url': v['sound'],
        'mood': v['mood'],
        'emoji': v['emoji']
    } for k, v in WEATHER_CONDITION_MAP.items()}
    
    return jsonify({
        'success': True,
        'weather': weather,
        'mood': weather_config['mood'],
        'primary_sound': {
            'url': weather_config['sound'],
            'description': f"Ambient {weather.lower()} sounds for relaxation"
        },
        'all_sounds': all_sounds,
        'emoji': weather_config['emoji']
    }), 200

@app.route('/api/analytics/summary', methods=['GET'])
@limiter.limit("30 per hour")
def get_weather_summary():
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    units = request.args.get('units', 'metric')
    
    if not lat or not lon:
        ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        location = get_location_from_ip(ip_address)
        lat, lon = location['lat'], location['lon']
    
    try:
        current_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units={units}&appid={OPENWEATHER_API_KEY}"
        current_response = requests.get(current_url, timeout=10)
        current_data = current_response.json()
        
        forecast_url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units={units}&appid={OPENWEATHER_API_KEY}"
        forecast_response = requests.get(forecast_url, timeout=10)
        forecast_data = forecast_response.json()
        
        weather_main = current_data['weather'][0]['main']
        weather_config = WEATHER_CONDITION_MAP.get(weather_main, WEATHER_CONDITION_MAP['Clear'])
        
        upcoming_temps = [item['main']['temp'] for item in forecast_data['list'][:8]]
        upcoming_conditions = [item['weather'][0]['main'] for item in forecast_data['list'][:8]]
        
        uv_index = calculate_uv_index(lat, lon)
        air_quality = get_air_quality(lat, lon)
        moon = get_moon_phase()
        
        summary = {
            'success': True,
            'location': {
                'name': current_data['name'],
                'country': current_data['sys']['country']
            },
            'current': {
                'temperature': round(current_data['main']['temp'], 1),
                'condition': weather_main,
                'description': current_data['weather'][0]['description'].title(),
                'emoji': weather_config['emoji']
            },
            'today_forecast': {
                'high': round(max(upcoming_temps), 1),
                'low': round(min(upcoming_temps), 1),
                'avg': round(sum(upcoming_temps) / len(upcoming_temps), 1)
            },
            'insights': {
                'mood': weather_config['mood'],
                'activity': random.choice(weather_config['activities']),
                'fun_fact': random.choice(WEATHER_FUN_FACTS),
                'color_palette': weather_config['color_palette']
            },
            'health': {
                'uv_index': uv_index,
                'air_quality': air_quality,
                'clothing': weather_config['clothing'],
                'tips': weather_config['health_tips']
            },
            'astronomy': {
                'moon_phase': moon,
                'sunrise': datetime.fromtimestamp(current_data['sys']['sunrise']).strftime('%I:%M %p'),
                'sunset': datetime.fromtimestamp(current_data['sys']['sunset']).strftime('%I:%M %p')
            },
            'upcoming_changes': len(set(upcoming_conditions)) > 1,
            'weather_score': calculate_weather_score({
                'temperature': {'current': current_data['main']['temp']},
                'details': {
                    'humidity': current_data['main']['humidity'],
                    'wind': {'speed': current_data['wind']['speed']}
                },
                'weather': {'main': weather_main}
            }),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        return jsonify(summary), 200
        
    except Exception as e:
        logger.error(f"Summary generation failed: {e}")
        return jsonify({'error': 'Summary unavailable', 'success': False}), 503

@app.route('/api/preferences/save', methods=['POST'])
def save_preferences():
    data = request.get_json()
    
    if 'session' not in globals():
        session['preferences'] = {}
    
    session['preferences'] = {
        'units': data.get('units', 'metric'),
        'favorite_locations': data.get('favorite_locations', []),
        'theme': data.get('theme', 'auto'),
        'notifications': data.get('notifications', True)
    }
    
    return jsonify({
        'success': True,
        'message': 'Preferences saved',
        'preferences': session.get('preferences', {})
    }), 200

@app.route('/api/preferences/get', methods=['GET'])
def get_preferences():
    preferences = session.get('preferences', {
        'units': 'metric',
        'favorite_locations': [],
        'theme': 'auto',
        'notifications': True
    })
    
    return jsonify({
        'success': True,
        'preferences': preferences
    }), 200

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Endpoint not found',
        'code': 404,
        'available_endpoints': [
            '/api/location/auto',
            '/api/weather/current',
            '/api/weather/forecast',
            '/api/weather/alerts',
            '/api/weather/explore',
            '/api/weather/compare',
            '/api/insights/fun-fact',
            '/api/insights/activities',
            '/api/entertainment/spotify',
            '/api/entertainment/sounds',
            '/api/analytics/summary'
        ]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'code': 500
    }), 500

@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({
        'success': False,
        'error': 'Rate limit exceeded. Please try again later.',
        'code': 429
    }), 429

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') == 'development'
    app.run(host='0.0.0.0', port=port, debug=debug)
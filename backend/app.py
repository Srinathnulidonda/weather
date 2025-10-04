import os
import requests
import random
import logging
from datetime import datetime, timedelta
from functools import lru_cache
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import base64

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'production-secret-key')
app.config['JSON_SORT_KEYS'] = False

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["300 per day", "100 per hour"],
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

WEATHER_CONDITION_MAP = {
    'Clear': {
        'playlist': 'Happy Vibes',
        'sound': 'https://cdn.pixabay.com/audio/2022/03/birds-chirping.mp3',
        'activities': ['Hiking', 'Picnic', 'Beach visit', 'Outdoor photography', 'Cycling', 'Running'],
        'mood': 'energetic'
    },
    'Clouds': {
        'playlist': 'Chill Lounge',
        'sound': 'https://cdn.pixabay.com/audio/2021/08/wind-gentle.mp3',
        'activities': ['Museum visit', 'Shopping', 'Outdoor walk', 'Coffee shop', 'Reading'],
        'mood': 'relaxed'
    },
    'Rain': {
        'playlist': 'Lo-Fi Beats',
        'sound': 'https://cdn.pixabay.com/audio/2022/03/rain-moderate.mp3',
        'activities': ['Movie marathon', 'Reading', 'Indoor cafe', 'Cooking', 'Art & crafts', 'Board games'],
        'mood': 'cozy'
    },
    'Drizzle': {
        'playlist': 'Rainy Day Jazz',
        'sound': 'https://cdn.pixabay.com/audio/2022/03/rain-light.mp3',
        'activities': ['Umbrella walk', 'Photography', 'Bookstore visit', 'Tea time', 'Journaling'],
        'mood': 'contemplative'
    },
    'Thunderstorm': {
        'playlist': 'Epic Cinematic',
        'sound': 'https://cdn.pixabay.com/audio/2021/08/thunder-storm.mp3',
        'activities': ['Stay indoors', 'Board games', 'Movie watching', 'Baking', 'Reading'],
        'mood': 'intense'
    },
    'Snow': {
        'playlist': 'Cozy Winter',
        'sound': 'https://cdn.pixabay.com/audio/2022/01/fireplace-crackling.mp3',
        'activities': ['Build snowman', 'Hot chocolate', 'Winter photography', 'Sledding', 'Ice skating'],
        'mood': 'peaceful'
    },
    'Mist': {
        'playlist': 'Ambient Soundscapes',
        'sound': 'https://cdn.pixabay.com/audio/2021/10/forest-ambience.mp3',
        'activities': ['Meditation', 'Yoga', 'Gentle walk', 'Spa day', 'Relaxation'],
        'mood': 'mysterious'
    },
    'Fog': {
        'playlist': 'Mystery & Calm',
        'sound': 'https://cdn.pixabay.com/audio/2021/10/ocean-waves.mp3',
        'activities': ['Indoor activities', 'Reading', 'Puzzle solving', 'Tea ceremony'],
        'mood': 'calm'
    },
    'Haze': {
        'playlist': 'Dreamy Indie',
        'sound': 'https://cdn.pixabay.com/audio/2021/08/wind-gentle.mp3',
        'activities': ['Indoor photography', 'Creative writing', 'Music listening', 'Painting'],
        'mood': 'dreamy'
    },
    'Smoke': {
        'playlist': 'Deep Focus',
        'sound': 'https://cdn.pixabay.com/audio/2021/10/forest-ambience.mp3',
        'activities': ['Stay indoors', 'Air purification', 'Indoor exercise', 'Work from home'],
        'mood': 'focused'
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
    "The fear of weather is called 'meteorophobia' and affects millions worldwide",
    "Snowflakes can take up to an hour to fall from cloud to ground",
    "The Earth's atmosphere weighs about 5.5 quadrillion tons",
    "A hurricane releases energy equivalent to 10,000 nuclear bombs",
    "Weather satellites orbit Earth at speeds of about 17,000 mph",
    "The hottest place on Earth is the Lut Desert in Iran, reaching 159.3°F"
]

GLOBAL_CITIES = [
    'Tokyo,JP', 'London,UK', 'Paris,FR', 'New York,US', 'Sydney,AU',
    'Dubai,AE', 'Singapore,SG', 'Mumbai,IN', 'Toronto,CA', 'Berlin,DE',
    'Rome,IT', 'Barcelona,ES', 'Rio de Janeiro,BR', 'Cairo,EG', 'Bangkok,TH',
    'Istanbul,TR', 'Seoul,KR', 'Mexico City,MX', 'Moscow,RU', 'Los Angeles,US',
    'Amsterdam,NL', 'Vienna,AT', 'Prague,CZ', 'Buenos Aires,AR', 'Cape Town,ZA',
    'Beijing,CN', 'Hong Kong,HK', 'Lisbon,PT', 'Dublin,IE', 'Copenhagen,DK'
]

@lru_cache(maxsize=128)
def get_spotify_token():
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
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
        return response.json().get('access_token')
    except Exception as e:
        logger.error(f"Spotify authentication failed: {e}")
        return None

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
                'timezone': data.get('time_zone', {}).get('name', 'UTC')
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
                'timezone': data.get('timezone', 'UTC')
            }
    except Exception as e:
        logger.error(f"IP geolocation failed: {e}")
        return {
            'city': 'New York',
            'country': 'USA',
            'lat': 40.7128,
            'lon': -74.0060,
            'timezone': 'America/New_York'
        }

def get_greeting(timezone='UTC'):
    try:
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
    except:
        return "Hello"

def calculate_uv_index(lat, lon):
    try:
        url = f"https://api.openweathermap.org/data/2.5/uvi?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
        response = requests.get(url, timeout=5)
        data = response.json()
        return data.get('value', 0)
    except:
        return None

def get_air_quality(lat, lon):
    try:
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
        response = requests.get(url, timeout=5)
        data = response.json()
        aqi = data['list'][0]['main']['aqi']
        components = data['list'][0]['components']
        
        aqi_levels = {1: 'Good', 2: 'Fair', 3: 'Moderate', 4: 'Poor', 5: 'Very Poor'}
        
        return {
            'aqi': aqi,
            'level': aqi_levels.get(aqi, 'Unknown'),
            'components': {
                'pm2_5': components.get('pm2_5'),
                'pm10': components.get('pm10'),
                'o3': components.get('o3'),
                'no2': components.get('no2')
            }
        }
    except Exception as e:
        logger.error(f"Air quality fetch failed: {e}")
        return None

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'Weather Visualizer API',
        'version': '1.0.0',
        'timestamp': datetime.utcnow().isoformat()
    }), 200

@app.route('/api/location/auto', methods=['GET'])
@limiter.limit("100 per hour")
def auto_detect_location():
    ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    location = get_location_from_ip(ip_address)
    greeting = get_greeting(location.get('timezone', 'UTC'))
    
    return jsonify({
        'success': True,
        'location': location,
        'greeting': greeting,
        'timestamp': datetime.utcnow().isoformat()
    }), 200

@app.route('/api/weather/current', methods=['GET'])
@limiter.limit("100 per hour")
def get_current_weather():
    if not OPENWEATHER_API_KEY:
        return jsonify({'error': 'Weather service not configured'}), 500
    
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
                'mood': weather_config['mood']
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
            'timestamp': datetime.fromtimestamp(data['dt']).isoformat()
        }
        
        return jsonify(result), 200
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Weather API request failed: {e}")
        return jsonify({'error': 'Weather service unavailable', 'success': False}), 503

@app.route('/api/weather/forecast', methods=['GET'])
@limiter.limit("100 per hour")
def get_forecast():
    if not OPENWEATHER_API_KEY:
        return jsonify({'error': 'Weather service not configured'}), 500
    
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
                hourly_forecast.append({
                    'datetime': dt.isoformat(),
                    'temperature': round(item['main']['temp'], 1),
                    'feels_like': round(item['main']['feels_like'], 1),
                    'weather': {
                        'main': item['weather'][0]['main'],
                        'description': item['weather'][0]['description'].title(),
                        'icon': item['weather'][0]['icon']
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
            daily_summary.append({
                'date': date,
                'temperature': {
                    'min': round(min(forecast['temps']), 1),
                    'max': round(max(forecast['temps']), 1),
                    'avg': round(sum(forecast['temps']) / len(forecast['temps']), 1)
                },
                'weather': {
                    'main': forecast['weather'],
                    'description': forecast['description'],
                    'icon': forecast['icon']
                },
                'humidity': round(sum(forecast['humidity']) / len(forecast['humidity'])),
                'wind_speed': round(sum(forecast['wind_speed']) / len(forecast['wind_speed']), 1),
                'precipitation_probability': round(max(forecast['pop'])),
                'pressure': round(sum(forecast['pressure']) / len(forecast['pressure']))
            })
        
        result = {
            'success': True,
            'location': {
                'name': data['city']['name'],
                'country': data['city']['country'],
                'coordinates': {
                    'lat': data['city']['coord']['lat'],
                    'lon': data['city']['coord']['lon']
                }
            },
            'daily': daily_summary,
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
            'count': len(formatted_alerts)
        }), 200
        
    except:
        return jsonify({
            'success': True,
            'alerts': [],
            'count': 0
        }), 200

@app.route('/api/weather/explore', methods=['GET'])
@limiter.limit("50 per hour")
def explore_random_weather():
    count = request.args.get('count', 1, type=int)
    count = min(count, 5)
    units = request.args.get('units', 'metric')
    
    cities = random.sample(GLOBAL_CITIES, count)
    results = []
    
    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&units={units}&appid={OPENWEATHER_API_KEY}"
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            
            results.append({
                'city': data['name'],
                'country': data['sys']['country'],
                'temperature': round(data['main']['temp'], 1),
                'feels_like': round(data['main']['feels_like'], 1),
                'weather': {
                    'main': data['weather'][0]['main'],
                    'description': data['weather'][0]['description'].title(),
                    'icon': data['weather'][0]['icon']
                },
                'coordinates': {
                    'lat': data['coord']['lat'],
                    'lon': data['coord']['lon']
                }
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
    return jsonify({
        'success': True,
        'fact': random.choice(WEATHER_FUN_FACTS),
        'category': 'weather',
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
            activities.extend(['Swimming', 'Water park', 'Indoor AC activities', 'Ice cream'])
        elif temp < 5:
            activities.extend(['Indoor activities', 'Hot beverages', 'Cozy indoor time'])
    
    activities = list(set(activities))
    suggested = random.sample(activities, min(4, len(activities)))
    
    return jsonify({
        'success': True,
        'weather': weather,
        'temperature': temp,
        'mood': weather_config['mood'],
        'suggested_activities': suggested,
        'all_activities': activities
    }), 200

@app.route('/api/entertainment/spotify', methods=['GET'])
@limiter.limit("50 per hour")
def get_spotify_playlists():
    weather = request.args.get('weather', 'Clear')
    limit = request.args.get('limit', 5, type=int)
    
    weather_config = WEATHER_CONDITION_MAP.get(weather, WEATHER_CONDITION_MAP['Clear'])
    playlist_query = weather_config['playlist']
    
    token = get_spotify_token()
    
    if not token:
        return jsonify({
            'success': False,
            'message': 'Spotify service unavailable',
            'weather': weather,
            'mood': playlist_query
        }), 503
    
    search_url = f"https://api.spotify.com/v1/search?q={playlist_query}&type=playlist&limit={limit}"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.get(search_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        playlists = []
        for item in data['playlists']['items']:
            playlists.append({
                'name': item['name'],
                'description': item.get('description', ''),
                'url': item['external_urls']['spotify'],
                'image': item['images'][0]['url'] if item['images'] else None,
                'tracks': item['tracks']['total'],
                'owner': item['owner']['display_name']
            })
        
        return jsonify({
            'success': True,
            'weather': weather,
            'mood': weather_config['mood'],
            'query': playlist_query,
            'playlists': playlists
        }), 200
        
    except Exception as e:
        logger.error(f"Spotify API error: {e}")
        return jsonify({
            'success': False,
            'error': 'Spotify service error',
            'weather': weather
        }), 503

@app.route('/api/entertainment/sounds', methods=['GET'])
def get_ambient_sounds():
    weather = request.args.get('weather', 'Clear')
    
    weather_config = WEATHER_CONDITION_MAP.get(weather, WEATHER_CONDITION_MAP['Clear'])
    
    all_sounds = {k: v['sound'] for k, v in WEATHER_CONDITION_MAP.items()}
    
    return jsonify({
        'success': True,
        'weather': weather,
        'mood': weather_config['mood'],
        'primary_sound': weather_config['sound'],
        'all_sounds': all_sounds,
        'description': f"Ambient {weather.lower()} sounds for relaxation"
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
        
        summary = {
            'success': True,
            'location': {
                'name': current_data['name'],
                'country': current_data['sys']['country']
            },
            'current': {
                'temperature': round(current_data['main']['temp'], 1),
                'condition': weather_main,
                'description': current_data['weather'][0]['description'].title()
            },
            'today_forecast': {
                'high': round(max(upcoming_temps), 1),
                'low': round(min(upcoming_temps), 1),
                'avg': round(sum(upcoming_temps) / len(upcoming_temps), 1)
            },
            'insights': {
                'mood': weather_config['mood'],
                'activity': random.choice(weather_config['activities']),
                'fun_fact': random.choice(WEATHER_FUN_FACTS)
            },
            'upcoming_changes': len(set(upcoming_conditions)) > 1,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        return jsonify(summary), 200
        
    except Exception as e:
        logger.error(f"Summary generation failed: {e}")
        return jsonify({'error': 'Summary unavailable', 'success': False}), 503

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Endpoint not found',
        'code': 404
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
        'error': 'Rate limit exceeded',
        'code': 429
    }), 429

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') == 'development'
    app.run(host='0.0.0.0', port=port, debug=debug)
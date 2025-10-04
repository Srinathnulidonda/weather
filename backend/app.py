#backend/app.py
import os
from dotenv import load_dotenv

load_dotenv()

import requests
import logging
import time
import random
import hashlib
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, Callable
from functools import wraps
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import base64

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*", "supports_credentials": True}})

app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'production-secret-key-change-in-production')
app.config['JSON_SORT_KEYS'] = False

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["1000 per day", "200 per hour"],
    storage_uri="memory://"
)

# Fixed logging configuration for Windows Unicode support
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('weather_api.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
IPGEOLOCATION_API_KEY = os.getenv('IPGEOLOCATION_API_KEY', '')
GEOCODE_API_KEY = os.getenv('GEOCODE_API_KEY', '')
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY', '')

if not OPENWEATHER_API_KEY:
    logger.critical("OPENWEATHER_API_KEY not configured!")
    raise RuntimeError("OPENWEATHER_API_KEY environment variable is required")

if not GOOGLE_MAPS_API_KEY:
    logger.warning("GOOGLE_MAPS_API_KEY not configured! Falling back to other providers.")

class LocationServiceError(Exception):
    pass

class LocationService:
    CACHE_DURATION = 300
    REQUEST_TIMEOUT = 15
    MAX_RETRIES = 2
    RETRY_DELAY = 1
    
    def __init__(self, openweather_api_key: Optional[str] = None, 
                 geocode_api_key: Optional[str] = None,
                 ipgeolocation_api_key: Optional[str] = None,
                 google_maps_api_key: Optional[str] = None):
        self.openweather_api_key = openweather_api_key
        self.geocode_api_key = geocode_api_key
        self.ipgeolocation_api_key = ipgeolocation_api_key
        self.google_maps_api_key = google_maps_api_key
        self._cache: Dict[str, Tuple[Dict, float]] = {}
        self._stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'google_api_calls': 0
        }
        if google_maps_api_key:
            logger.info("LocationService initialized - GOOGLE MAPS MODE (100% Accuracy)")
        else:
            logger.info("LocationService initialized - High Accuracy Mode")
    
    def get_location_from_coordinates(self, latitude: float, longitude: float, use_cache: bool = False) -> Dict:
        self._stats['total_requests'] += 1
        
        if not self._validate_coordinates(latitude, longitude):
            raise LocationServiceError(f"Invalid coordinates: lat={latitude}, lon={longitude}")
        
        if use_cache:
            cache_key = self._generate_cache_key('coords', latitude, longitude)
            cached_data = self._get_from_cache(cache_key)
            if cached_data:
                self._stats['cache_hits'] += 1
                logger.info(f"Cache hit for coordinates: {latitude}, {longitude}")
                return cached_data
        
        logger.info(f"Fetching fresh location data for coordinates: {latitude}, {longitude}")
        
        if self.google_maps_api_key:
            try:
                logger.info("Attempting Google Geocoding API (Priority #1 - 100% Accurate)...")
                location_data = self._reverse_geocode_google(latitude, longitude)
                
                if location_data and self._validate_location_data(location_data):
                    location_data['source'] = 'Google Geocoding API'
                    location_data['method'] = 'gps-coordinates'
                    location_data['accuracy'] = '100% - Premium'
                    location_data['timestamp'] = datetime.now(timezone.utc).isoformat()
                    
                    if use_cache:
                        cache_key = self._generate_cache_key('coords', latitude, longitude)
                        self._add_to_cache(cache_key, location_data)
                    
                    self._stats['successful_requests'] += 1
                    self._stats['google_api_calls'] += 1
                    
                    full_location = self._format_full_location(location_data)
                    logger.info(f"SUCCESS: Google API returned: {full_location}")
                    logger.info(f"Details - Road: {location_data.get('road')}, Suburb: {location_data.get('suburb')}, City: {location_data.get('city')}")
                    
                    return location_data
            except Exception as e:
                logger.warning(f"Google Geocoding API failed: {str(e)}")
        
        providers = [
            ('Nominatim-OSM', self._reverse_geocode_nominatim),
            ('BigDataCloud', self._reverse_geocode_bigdatacloud),
            ('LocationIQ', self._reverse_geocode_locationiq),
        ]
        
        last_error = None
        
        for provider_name, provider_func in providers:
            try:
                logger.info(f"Trying fallback: {provider_name}...")
                location_data = provider_func(latitude, longitude)
                
                if location_data and self._validate_location_data(location_data):
                    location_data['source'] = provider_name
                    location_data['method'] = 'gps-coordinates'
                    location_data['accuracy'] = 'high'
                    location_data['timestamp'] = datetime.now(timezone.utc).isoformat()
                    
                    if use_cache:
                        cache_key = self._generate_cache_key('coords', latitude, longitude)
                        self._add_to_cache(cache_key, location_data)
                    
                    self._stats['successful_requests'] += 1
                    
                    full_location = self._format_full_location(location_data)
                    logger.info(f"SUCCESS: {provider_name} returned: {full_location}")
                    
                    return location_data
                    
            except Exception as e:
                last_error = e
                logger.warning(f"{provider_name} failed: {str(e)}")
                continue
        
        self._stats['failed_requests'] += 1
        error_msg = f"All reverse geocoding providers failed. Last error: {str(last_error)}"
        logger.error(error_msg)
        raise LocationServiceError(error_msg)
    
    def _reverse_geocode_google(self, lat: float, lon: float) -> Dict:
        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={self.google_maps_api_key}&result_type=street_address|route|neighborhood|locality|sublocality"
        
        headers = {
            'Accept': 'application/json'
        }
        
        response = requests.get(url, headers=headers, timeout=self.REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status') != 'OK':
            raise LocationServiceError(f"Google API error: {data.get('status')} - {data.get('error_message', 'Unknown error')}")
        
        if not data.get('results'):
            raise LocationServiceError("No results from Google Geocoding API")
        
        result = data['results'][0]
        address_components = result.get('address_components', [])
        
        location_data = {
            'road': '',
            'house_number': '',
            'suburb': '',
            'neighbourhood': '',
            'locality': '',
            'city': '',
            'district': '',
            'state': '',
            'country': '',
            'country_code': '',
            'zipcode': '',
            'lat': lat,
            'lon': lon,
            'formatted_address': result.get('formatted_address', ''),
            'place_id': result.get('place_id', '')
        }
        
        for component in address_components:
            types = component.get('types', [])
            long_name = component.get('long_name', '')
            short_name = component.get('short_name', '')
            
            if 'street_number' in types:
                location_data['house_number'] = long_name
            elif 'route' in types:
                location_data['road'] = long_name
            elif 'neighborhood' in types:
                location_data['neighbourhood'] = long_name
            elif 'sublocality' in types or 'sublocality_level_1' in types:
                location_data['suburb'] = long_name
            elif 'sublocality_level_2' in types and not location_data['neighbourhood']:
                location_data['neighbourhood'] = long_name
            elif 'locality' in types:
                location_data['city'] = long_name
                location_data['locality'] = long_name
            elif 'administrative_area_level_2' in types:
                location_data['district'] = long_name
            elif 'administrative_area_level_1' in types:
                location_data['state'] = long_name
            elif 'country' in types:
                location_data['country'] = long_name
                location_data['country_code'] = short_name
            elif 'postal_code' in types:
                location_data['zipcode'] = long_name
        
        if not location_data['city'] and location_data['district']:
            location_data['city'] = location_data['district']
        
        if not location_data['locality']:
            location_data['locality'] = location_data.get('suburb') or location_data.get('neighbourhood') or ''
        
        logger.info(f"Google API parsed: road={location_data['road']}, suburb={location_data['suburb']}, neighbourhood={location_data['neighbourhood']}, city={location_data['city']}")
        
        if not location_data['city'] and not location_data['suburb'] and not location_data['locality']:
            raise LocationServiceError("Insufficient location data from Google API")
        
        return location_data
    
    def _format_full_location(self, location_data: Dict) -> str:
        parts = []
        
        if location_data.get('house_number') and location_data.get('road'):
            parts.append(f"{location_data['house_number']} {location_data['road']}")
        elif location_data.get('road'):
            parts.append(location_data['road'])
        
        if location_data.get('suburb'):
            parts.append(location_data['suburb'])
        elif location_data.get('neighbourhood'):
            parts.append(location_data['neighbourhood'])
        elif location_data.get('locality') and location_data.get('locality') != location_data.get('city'):
            parts.append(location_data['locality'])
        
        if location_data.get('city') and location_data.get('city') not in parts:
            parts.append(location_data['city'])
        
        if location_data.get('state') and location_data.get('state') not in parts:
            parts.append(location_data['state'])
        
        if location_data.get('country'):
            parts.append(location_data['country'])
        
        return ', '.join(parts) if parts else location_data.get('formatted_address', 'Unknown Location')
    
    def _reverse_geocode_nominatim(self, lat: float, lon: float) -> Dict:
        url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&addressdetails=1&zoom=18&accept-language=en"
        headers = {
            'User-Agent': 'SkyVibeWeatherApp/2.0 (Production)',
            'Accept-Language': 'en'
        }
        
        time.sleep(1)
        
        response = requests.get(url, headers=headers, timeout=self.REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        address = data.get('address', {})
        
        road = address.get('road', '')
        suburb = (address.get('suburb') or address.get('residential') or 
                 address.get('neighbourhood') or address.get('quarter') or
                 address.get('hamlet'))
        
        neighbourhood = (address.get('neighbourhood') or address.get('quarter') or
                        address.get('suburb'))
        
        locality = (address.get('locality') or address.get('village') or 
                   address.get('town') or address.get('hamlet'))
        
        city = (address.get('city') or address.get('municipality') or 
               address.get('county') or locality or suburb)
        
        district = (address.get('state_district') or address.get('county'))
        
        state = address.get('state', '')
        country = address.get('country', '')
        
        if not city and not suburb and not locality:
            raise LocationServiceError("No valid location found in Nominatim response")
        
        return {
            'road': road,
            'suburb': suburb or '',
            'neighbourhood': neighbourhood or '',
            'locality': locality or '',
            'city': city or 'Unknown',
            'district': district or '',
            'state': state,
            'country': country,
            'country_code': address.get('country_code', '').upper(),
            'lat': lat,
            'lon': lon,
            'zipcode': address.get('postcode', ''),
            'house_number': address.get('house_number', '')
        }
    
    def _reverse_geocode_bigdatacloud(self, lat: float, lon: float) -> Dict:
        url = f"https://api.bigdatacloud.net/data/reverse-geocode-client?latitude={lat}&longitude={lon}&localityLanguage=en"
        
        response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        locality_info = data.get('localityInfo', {})
        administrative = locality_info.get('administrative', [])
        
        suburb = ''
        neighbourhood = ''
        district = ''
        
        for admin in administrative:
            order = admin.get('order', 0)
            name = admin.get('name', '')
            
            if order == 8 and not suburb:
                suburb = name
            elif order == 7 and not neighbourhood:
                neighbourhood = name
            elif order == 6 and not neighbourhood:
                neighbourhood = name
            elif order == 5 and not district:
                district = name
        
        city = (data.get('city') or data.get('locality') or 
               data.get('principalSubdivision'))
        
        locality = data.get('locality', '')
        
        if not city and not suburb and not neighbourhood:
            raise LocationServiceError("No valid location found in BigDataCloud response")
        
        return {
            'road': '',
            'suburb': suburb,
            'neighbourhood': neighbourhood,
            'locality': locality,
            'city': city or 'Unknown',
            'district': district or data.get('principalSubdivision', ''),
            'state': data.get('principalSubdivision', ''),
            'country': data.get('countryName', ''),
            'country_code': data.get('countryCode', ''),
            'lat': lat,
            'lon': lon,
            'zipcode': data.get('postcode', '')
        }
    
    def _reverse_geocode_locationiq(self, lat: float, lon: float) -> Dict:
        url = f"https://us1.locationiq.com/v1/reverse.php?key=pk.0f147952a41c555c5b3d3b5bc3a8d32b&lat={lat}&lon={lon}&format=json&zoom=18"
        
        try:
            response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            address = data.get('address', {})
            
            return {
                'road': address.get('road', ''),
                'suburb': address.get('suburb', ''),
                'neighbourhood': address.get('neighbourhood', ''),
                'locality': address.get('locality', ''),
                'city': address.get('city') or address.get('town', ''),
                'district': address.get('state_district', ''),
                'state': address.get('state', ''),
                'country': address.get('country', ''),
                'country_code': address.get('country_code', '').upper(),
                'lat': lat,
                'lon': lon,
                'zipcode': address.get('postcode', '')
            }
        except:
            raise LocationServiceError("LocationIQ failed")
    
    def get_location_from_ip(self, ip_address: Optional[str] = None) -> Dict:
        self._stats['total_requests'] += 1
        
        if ip_address and self._is_private_ip(ip_address):
            logger.info(f"Private IP detected ({ip_address}), using auto-detection")
            ip_address = None
        
        # Enhanced provider list with better accuracy providers first
        providers = []
        
        # Premium providers first (if API keys available)
        if self.ipgeolocation_api_key:
            providers.append(('IPGeolocation.io', lambda: self._ipgeolocation_io(ip_address)))
        
        # Free providers with good accuracy
        providers.extend([
            ('IP-API.com', lambda: self._ip_api_com(ip_address)),
            ('IPInfo.io', lambda: self._ipinfo_io(ip_address)),
            ('IPAPI.co', lambda: self._ipapi_co(ip_address)),
        ])
        
        last_error = None
        best_result = None
        best_accuracy = 0
        
        for provider_name, provider_func in providers:
            try:
                logger.info(f"Attempting IP geolocation with {provider_name}")
                location_data = provider_func()
                
                if location_data and self._validate_location_data(location_data):
                    # Calculate accuracy score based on detail level
                    accuracy_score = self._calculate_location_accuracy(location_data)
                    
                    try:
                        if self.google_maps_api_key:
                            logger.info("Enhancing IP location with Google Geocoding API...")
                            detailed_location = self._reverse_geocode_google(
                                location_data['latitude'], 
                                location_data['longitude']
                            )
                            self._stats['google_api_calls'] += 1
                        else:
                            detailed_location = self.get_location_from_coordinates(
                                location_data['latitude'],
                                location_data['longitude'],
                                use_cache=False
                            )
                        
                        location_data.update({
                            'road': detailed_location.get('road', ''),
                            'house_number': detailed_location.get('house_number', ''),
                            'city': detailed_location.get('city', location_data.get('city')),
                            'state': detailed_location.get('state', location_data.get('state')),
                            'suburb': detailed_location.get('suburb', ''),
                            'neighbourhood': detailed_location.get('neighbourhood', ''),
                            'locality': detailed_location.get('locality', ''),
                            'district': detailed_location.get('district', ''),
                            'formatted_address': detailed_location.get('formatted_address', ''),
                            'place_id': detailed_location.get('place_id', ''),
                        })
                        
                        if self.google_maps_api_key:
                            location_data['accuracy'] = '100% - Premium Enhanced'
                            location_data['source'] = f"{provider_name} + Google Geocoding API"
                        
                    except Exception as e:
                        logger.warning(f"Location enhancement failed: {e}")
                    
                    location_data['source'] = location_data.get('source', provider_name)
                    location_data['method'] = 'ip-geolocation'
                    location_data['accuracy'] = location_data.get('accuracy', f'medium ({accuracy_score}% confident)')
                    location_data['timestamp'] = datetime.now(timezone.utc).isoformat()
                    
                    if 'latitude' in location_data:
                        location_data['lat'] = location_data.pop('latitude')
                    if 'longitude' in location_data:
                        location_data['lon'] = location_data.pop('longitude')
                    
                    # Keep the best result based on accuracy
                    if accuracy_score > best_accuracy:
                        best_accuracy = accuracy_score
                        best_result = location_data
                    
                    # If we get high accuracy result, use it immediately
                    if accuracy_score >= 80:
                        self._stats['successful_requests'] += 1
                        full_location = self._format_full_location(location_data)
                        logger.info(f"High accuracy result from {provider_name}: {full_location}")
                        return location_data
                    
            except Exception as e:
                last_error = e
                logger.warning(f"{provider_name} failed: {str(e)}")
                continue
        
        # Return best result if we have one
        if best_result:
            self._stats['successful_requests'] += 1
            full_location = self._format_full_location(best_result)
            logger.info(f"Best available result: {full_location} (Accuracy: {best_accuracy}%)")
            logger.warning("IP geolocation may show ISP location instead of exact location. For best accuracy, use GPS coordinates.")
            return best_result
        
        self._stats['failed_requests'] += 1
        error_msg = f"All IP geolocation providers failed. Last error: {str(last_error)}"
        logger.error(error_msg)
        raise LocationServiceError(error_msg)
    
    def _calculate_location_accuracy(self, location_data: Dict) -> int:
        """Calculate accuracy score based on available location details"""
        score = 0
        
        # Base score for having coordinates
        if location_data.get('latitude') and location_data.get('longitude'):
            score += 20
        
        # City information
        if location_data.get('city'):
            score += 30
        
        # State/region information
        if location_data.get('state') or location_data.get('region'):
            score += 20
        
        # More detailed location info
        if location_data.get('district'):
            score += 10
        if location_data.get('zipcode') or location_data.get('postal'):
            score += 10
        if location_data.get('timezone'):
            score += 10
        
        return min(score, 100)
    
    def _ipgeolocation_io(self, ip_address: Optional[str]) -> Dict:
        url = f"https://api.ipgeolocation.io/ipgeo?apiKey={self.ipgeolocation_api_key}"
        if ip_address:
            url += f"&ip={ip_address}"
        
        response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        return {
            'city': data.get('city'),
            'district': data.get('district', ''),
            'state': data.get('state_prov', ''),
            'country': data.get('country_name'),
            'latitude': float(data.get('latitude')),
            'longitude': float(data.get('longitude')),
            'timezone': data.get('time_zone', {}).get('name', 'UTC') if isinstance(data.get('time_zone'), dict) else 'UTC',
            'zipcode': data.get('zipcode', '')
        }
    
    def _ipinfo_io(self, ip_address: Optional[str]) -> Dict:
        url = f"https://ipinfo.io/{ip_address}/json" if ip_address else "https://ipinfo.io/json"
        
        response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        if 'loc' not in data:
            raise LocationServiceError("No location data from IPInfo.io")
        
        loc = data['loc'].split(',')
        
        return {
            'city': data.get('city'),
            'state': data.get('region', ''),
            'country': data.get('country'),
            'latitude': float(loc[0]),
            'longitude': float(loc[1]),
            'timezone': data.get('timezone', 'UTC'),
            'zipcode': data.get('postal', '')
        }
    
    def _ipapi_co(self, ip_address: Optional[str]) -> Dict:
        url = f"https://ipapi.co/{ip_address}/json/" if ip_address else "https://ipapi.co/json/"
        
        response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        if 'error' in data:
            raise LocationServiceError(f"IPAPI.co error: {data.get('reason')}")
        
        return {
            'city': data.get('city'),
            'state': data.get('region', ''),
            'country': data.get('country_name'),
            'latitude': float(data.get('latitude')),
            'longitude': float(data.get('longitude')),
            'timezone': data.get('timezone', 'UTC'),
            'zipcode': data.get('postal', '')
        }
    
    def _ip_api_com(self, ip_address: Optional[str]) -> Dict:
        fields = "status,message,country,countryCode,region,regionName,city,district,zip,lat,lon,timezone"
        url = f"http://ip-api.com/json/{ip_address}?fields={fields}" if ip_address else f"http://ip-api.com/json/?fields={fields}"
        
        response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status') == 'fail':
            raise LocationServiceError(f"IP-API.com error: {data.get('message')}")
        
        return {
            'city': data.get('city'),
            'district': data.get('district', ''),
            'state': data.get('regionName', ''),
            'country': data.get('country'),
            'latitude': float(data.get('lat')),
            'longitude': float(data.get('lon')),
            'timezone': data.get('timezone', 'UTC'),
            'zipcode': data.get('zip', '')
        }
    
    def _validate_coordinates(self, lat: float, lon: float) -> bool:
        try:
            lat = float(lat)
            lon = float(lon)
            return -90 <= lat <= 90 and -180 <= lon <= 180
        except (TypeError, ValueError):
            return False
    
    def _validate_location_data(self, data: Dict) -> bool:
        if 'city' in data and data['city'] and data['city'] != 'Unknown':
            return True
        if 'suburb' in data and data['suburb']:
            return True
        if 'locality' in data and data['locality']:
            return True
        if 'neighbourhood' in data and data['neighbourhood']:
            return True
        
        logger.warning(f"Location validation failed: {data}")
        return False
    
    def _is_private_ip(self, ip: str) -> bool:
        private_ranges = ['127.', '10.', '172.16.', '172.17.', '172.18.', '172.19.',
                         '172.20.', '172.21.', '172.22.', '172.23.', '172.24.',
                         '172.25.', '172.26.', '172.27.', '172.28.', '172.29.',
                         '172.30.', '172.31.', '192.168.', '::1', 'localhost']
        return any(ip.startswith(prefix) for prefix in private_ranges)
    
    def _generate_cache_key(self, prefix: str, *args) -> str:
        key_string = f"{prefix}:{'_'.join(str(arg) for arg in args)}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _get_from_cache(self, key: str) -> Optional[Dict]:
        if key in self._cache:
            data, timestamp = self._cache[key]
            if time.time() - timestamp < self.CACHE_DURATION:
                return data
            else:
                del self._cache[key]
        return None
    
    def _add_to_cache(self, key: str, data: Dict) -> None:
        self._cache[key] = (data, time.time())
    
    def clear_cache(self) -> None:
        self._cache.clear()
        logger.info("Location cache cleared")
    
    def get_stats(self) -> Dict:
        cache_hit_rate = ((self._stats['cache_hits'] / self._stats['total_requests'] * 100) 
                         if self._stats['total_requests'] > 0 else 0)
        
        return {
            **self._stats,
            'cache_hit_rate': f"{cache_hit_rate:.2f}%",
            'cached_entries': len(self._cache)
        }

location_service = LocationService(
    openweather_api_key=OPENWEATHER_API_KEY,
    geocode_api_key=GEOCODE_API_KEY,
    ipgeolocation_api_key=IPGEOLOCATION_API_KEY,
    google_maps_api_key=GOOGLE_MAPS_API_KEY
)

WEATHER_CONDITION_MAP = {
    'Clear': {
        'playlist': 'happy pop upbeat',
        'sound': 'https://cdn.pixabay.com/audio/2022/03/22/audio_1e5d97d57a.mp3',
        'activities': ['Hiking', 'Picnic', 'Beach visit', 'Outdoor photography', 'Cycling', 'Running'],
        'mood': 'energetic',
        'color_palette': ['#FFD700', '#FFA500', '#87CEEB', '#00BFFF'],
        'emoji': '☀️',
        'clothing': ['Sunglasses', 'Light clothing', 'Sunscreen', 'Hat'],
        'health_tips': ['Stay hydrated', 'Use SPF 30+ sunscreen', 'Avoid peak sun hours']
    },
    'Clouds': {
        'playlist': 'chill vibes relaxing',
        'sound': 'https://cdn.pixabay.com/audio/2021/08/09/audio_0625c1539c.mp3',
        'activities': ['Museum visit', 'Shopping', 'Outdoor walk', 'Coffee shop'],
        'mood': 'relaxed',
        'color_palette': ['#808080', '#A9A9A9', '#D3D3D3', '#778899'],
        'emoji': '☁️',
        'clothing': ['Light jacket', 'Comfortable shoes', 'Layers'],
        'health_tips': ['Perfect weather for outdoor activities', 'Stay active']
    },
    'Rain': {
        'playlist': 'rainy day jazz',
        'sound': 'https://cdn.pixabay.com/audio/2022/03/10/audio_c9054832ff.mp3',
        'activities': ['Movie marathon', 'Reading', 'Indoor cafe', 'Cooking'],
        'mood': 'cozy',
        'color_palette': ['#4682B4', '#5F9EA0', '#708090', '#2F4F4F'],
        'emoji': '🌧️',
        'clothing': ['Umbrella', 'Raincoat', 'Waterproof shoes'],
        'health_tips': ['Stay warm and dry', 'Hot beverages recommended']
    },
    'Drizzle': {
        'playlist': 'peaceful piano',
        'sound': 'https://cdn.pixabay.com/audio/2022/03/10/audio_d0d5b89a6c.mp3',
        'activities': ['Umbrella walk', 'Photography', 'Bookstore visit', 'Tea time'],
        'mood': 'contemplative',
        'color_palette': ['#B0C4DE', '#ADD8E6', '#87CEEB', '#6495ED'],
        'emoji': '🌦️',
        'clothing': ['Light rain jacket', 'Umbrella', 'Comfortable shoes'],
        'health_tips': ['Perfect for contemplation', 'Stay moderately active']
    },
    'Snow': {
        'playlist': 'winter acoustic',
        'sound': 'https://cdn.pixabay.com/audio/2022/01/18/audio_12b2c26c8c.mp3',
        'activities': ['Build snowman', 'Hot chocolate', 'Winter photography', 'Sledding'],
        'mood': 'peaceful',
        'color_palette': ['#FFFFFF', '#F0F8FF', '#E0FFFF', '#B0E0E6'],
        'emoji': '❄️',
        'clothing': ['Heavy coat', 'Gloves', 'Scarf', 'Winter boots'],
        'health_tips': ['Layer up', 'Protect extremities', 'Stay warm']
    },
    'Thunderstorm': {
        'playlist': 'epic cinematic',
        'sound': 'https://cdn.pixabay.com/audio/2021/08/04/audio_12b0c7443c.mp3',
        'activities': ['Stay indoors', 'Board games', 'Movie watching', 'Baking'],
        'mood': 'intense',
        'color_palette': ['#2F4F4F', '#36454F', '#343434', '#800080'],
        'emoji': '⛈️',
        'clothing': ['Stay indoors', 'Emergency kit ready'],
        'health_tips': ['Stay indoors', 'Avoid electrical devices']
    },
    'Mist': {
        'playlist': 'ambient soundscapes',
        'sound': 'https://cdn.pixabay.com/audio/2021/10/07/audio_bb630cc098.mp3',
        'activities': ['Meditation', 'Yoga', 'Gentle walk', 'Spa day'],
        'mood': 'mysterious',
        'color_palette': ['#F5F5F5', '#DCDCDC', '#C0C0C0', '#A9A9A9'],
        'emoji': '🌫️',
        'clothing': ['Light layers', 'Visibility clothing'],
        'health_tips': ['Drive carefully', 'Use visibility aids']
    },
}

WEATHER_FUN_FACTS = [
    "The highest temperature ever recorded on Earth was 134°F (56.7°C) in Death Valley, California in 1913",
    "Lightning strikes the Earth about 100 times every second",
    "Antarctica is the world's largest desert",
    "Modern weather forecasting has a 5-day accuracy rate of approximately 90%",
    "Rainbows are actually full circles, but we typically see only half from ground level",
    "A single cumulus cloud can weigh more than 1 million pounds",
    "The fastest wind speed ever recorded was 253 mph during Tropical Cyclone Olivia in 1996",
]

GLOBAL_CITIES = [
    'Tokyo,JP', 'London,UK', 'Paris,FR', 'New York,US', 'Sydney,AU',
    'Dubai,AE', 'Singapore,SG', 'Mumbai,IN', 'Toronto,CA', 'Berlin,DE',
    'Rome,IT', 'Barcelona,ES', 'Rio de Janeiro,BR', 'Cairo,EG', 'Bangkok,TH',
]

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

spotify_token_cache = {'token': None, 'expires_at': 0}

def get_spotify_token():
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        return None
    
    if spotify_token_cache['token'] and time.time() < spotify_token_cache['expires_at']:
        return spotify_token_cache['token']
    
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
        
        spotify_token_cache['token'] = token_data.get('access_token')
        spotify_token_cache['expires_at'] = time.time() + token_data.get('expires_in', 3600) - 60
        
        return spotify_token_cache['token']
    except Exception as e:
        logger.error(f"Spotify authentication failed: {e}")
        return None

def get_greeting():
    hour = datetime.now(timezone.utc).hour
    
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
    
    return max(0, min(100, score))

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

@app.route('/', methods=['GET'])
def home():
    accuracy_mode = "100% Accurate - Google Maps" if GOOGLE_MAPS_API_KEY else "High Accuracy"
    providers = ["Google Geocoding API", "Nominatim-OSM", "BigDataCloud", "LocationIQ"] if GOOGLE_MAPS_API_KEY else ["Nominatim-OSM", "BigDataCloud", "LocationIQ"]
    
    return jsonify({
        'service': 'SkyVibe Weather API',
        'version': '2.1.1',
        'status': 'operational',
        'location_accuracy': accuracy_mode,
        'providers': providers,
        'ip_location_note': 'IP location may show ISP/data center location. Use GPS for exact location.',
        'endpoints': {
            'health': 'GET /health',
            'location_coords': 'POST /api/location/coords',
            'location_ip': 'GET /api/location/auto',
            'weather_current': 'GET /api/weather/current',
            'weather_forecast': 'GET /api/weather/forecast',
            'weather_alerts': 'GET /api/weather/alerts',
            'weather_explore': 'GET /api/weather/explore',
            'fun_fact': 'GET /api/insights/fun-fact',
            'activities': 'GET /api/insights/activities',
            'spotify': 'GET /api/entertainment/spotify',
            'sounds': 'GET /api/entertainment/sounds',
            'stats': 'GET /api/stats'
        }
    }), 200

@app.route('/health', methods=['GET'])
def health_check():
    stats = location_service.get_stats()
    
    return jsonify({
        'status': 'healthy',
        'service': 'SkyVibe Weather API',
        'version': '2.1.1',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'google_maps_enabled': bool(GOOGLE_MAPS_API_KEY),
        'statistics': stats,
        'ip_location_accuracy': 'ISP/Data Center level - Use GPS for exact location'
    }), 200

@app.route('/api/location/coords', methods=['POST'])
@limiter.limit("200 per hour")
def get_location_by_coords():
    data = request.get_json()
    
    if not data or 'lat' not in data or 'lon' not in data:
        return jsonify({
            'success': False,
            'error': 'Missing coordinates. Please provide lat and lon.',
            'code': 'MISSING_COORDINATES'
        }), 400
    
    try:
        lat = float(data['lat'])
        lon = float(data['lon'])
        
        logger.info(f"Processing GPS coordinates: lat={lat}, lon={lon}")
        
        location = location_service.get_location_from_coordinates(lat, lon, use_cache=False)
        greeting = get_greeting()
        moon = get_moon_phase()
        
        full_location = location_service._format_full_location(location)
        
        return jsonify({
            'success': True,
            'location': location,
            'display_location': full_location,
            'greeting': greeting,
            'moon_phase': moon,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 200
        
    except ValueError as e:
        logger.warning(f"Invalid coordinates: {e}")
        return jsonify({
            'success': False,
            'error': 'Invalid coordinates format',
            'code': 'INVALID_COORDINATES'
        }), 400
        
    except LocationServiceError as e:
        logger.error(f"Location service error: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'code': 'LOCATION_SERVICE_ERROR'
        }), 503
        
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'code': 'INTERNAL_ERROR'
        }), 500

@app.route('/api/location/auto', methods=['GET'])
@limiter.limit("200 per hour")
def auto_detect_location():
    ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    try:
        logger.info(f"Processing IP-based location: {ip_address or 'auto'}")
        
        location = location_service.get_location_from_ip(ip_address)
        greeting = get_greeting()
        moon = get_moon_phase()
        
        full_location = location_service._format_full_location(location)
        
        return jsonify({
            'success': True,
            'location': location,
            'display_location': full_location,
            'greeting': greeting,
            'moon_phase': moon,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'note': 'IP location may show ISP/data center location. For exact location, enable GPS.'
        }), 200
        
    except LocationServiceError as e:
        logger.error(f"Location error: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'code': 'LOCATION_SERVICE_ERROR'
        }), 503
        
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'code': 'INTERNAL_ERROR'
        }), 500

@app.route('/api/weather/current', methods=['GET'])
@limiter.limit("200 per hour")
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
        
        try:
            location = location_service.get_location_from_ip(ip_address)
            lat, lon = location['lat'], location['lon']
        except LocationServiceError:
            return jsonify({
                'success': False,
                'error': 'Location required',
                'code': 'LOCATION_REQUIRED'
            }), 400
    
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
                'name': data.get('name', 'Unknown'),
                'country': data['sys'].get('country', 'Unknown'),
                'coordinates': {
                    'lat': data['coord'].get('lat'),
                    'lon': data['coord'].get('lon')
                }
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
        logger.error(f"Weather API error: {e}")
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
        
        try:
            location = location_service.get_location_from_ip(ip_address)
            lat, lon = location['lat'], location['lon']
        except:
            return jsonify({'error': 'Location required', 'success': False}), 400
    
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
        logger.error(f"Forecast API error: {e}")
        return jsonify({'error': 'Forecast service unavailable', 'success': False}), 503

@app.route('/api/weather/alerts', methods=['GET'])
@limiter.limit("100 per hour")
def get_weather_alerts():
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    
    if not lat or not lon:
        return jsonify({
            'success': True,
            'alerts': [],
            'count': 0,
            'has_alerts': False,
            'message': 'Coordinates required for alerts'
        }), 200
    
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

@app.route('/api/weather/explore', methods=['GET'])
@limiter.limit("50 per hour")
def explore_random_weather():
    count = request.args.get('count', 6, type=int)
    count = min(count, 10)
    units = request.args.get('units', 'metric')
    
    cities = random.sample(GLOBAL_CITIES, min(count, len(GLOBAL_CITIES)))
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
        'total_facts': len(WEATHER_FUN_FACTS),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }), 200

@app.route('/api/insights/activities', methods=['GET'])
def get_activity_suggestions():
    weather = request.args.get('weather', 'Clear')
    temp = request.args.get('temp', type=float)
    
    weather_config = WEATHER_CONDITION_MAP.get(weather, WEATHER_CONDITION_MAP['Clear'])
    activities = weather_config['activities'].copy()
    
    if temp:
        if temp > 30:
            activities.extend(['Swimming', 'Water park', 'Ice cream'])
        elif temp < 5:
            activities.extend(['Indoor activities', 'Hot beverages'])
    
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
    playlist_query = weather_config['playlist']
    
    token = get_spotify_token()
    
    if not token:
        return jsonify({
            'success': False,
            'message': 'Spotify service unavailable',
            'weather': weather,
            'mood': weather_config['mood']
        }), 200
    
    search_url = f"https://api.spotify.com/v1/search?q={playlist_query}&type=playlist&limit={limit}"
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
                        'description': item.get('description', f'Curated for {weather_config["mood"]} mood'),
                        'url': item.get('external_urls', {}).get('spotify', '#'),
                        'image': item.get('images', [{}])[0].get('url') if item.get('images') else None,
                        'tracks': item.get('tracks', {}).get('total', 0),
                        'owner': item.get('owner', {}).get('display_name', 'Spotify')
                    })
        
        return jsonify({
            'success': True,
            'weather': weather,
            'mood': weather_config['mood'],
            'playlists': playlists,
            'total_found': len(playlists)
        }), 200
        
    except Exception as e:
        logger.error(f"Spotify API error: {e}")
        return jsonify({
            'success': False,
            'error': 'Spotify service error',
            'weather': weather,
            'mood': weather_config['mood']
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

@app.route('/api/stats', methods=['GET'])
def get_service_stats():
    return jsonify({
        'success': True,
        'statistics': location_service.get_stats(),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }), 200

@app.route('/api/admin/clear-cache', methods=['POST'])
@limiter.limit("10 per hour")
def clear_cache():
    admin_key = request.headers.get('X-Admin-Key')
    if admin_key != os.getenv('ADMIN_API_KEY', 'default-admin-key'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    location_service.clear_cache()
    return jsonify({'success': True, 'message': 'Location cache cleared'}), 200

@app.errorhandler(404)
def not_found(error):
    return jsonify({'success': False, 'error': 'Endpoint not found', 'code': 404}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.exception(f"Internal server error: {error}")
    return jsonify({'success': False, 'error': 'Internal server error', 'code': 500}), 500

@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({'success': False, 'error': 'Rate limit exceeded', 'code': 429}), 429

if __name__ == '__main__':
    location_service.clear_cache()
    
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') == 'development'
    
    logger.info("=" * 60)
    logger.info("SkyVibe Weather API v2.1.1")
    logger.info("=" * 60)
    logger.info(f"Server running on port {port}")
    logger.info(f"Debug mode: {debug}")
    if GOOGLE_MAPS_API_KEY:
        logger.info("Location Service: GOOGLE MAPS MODE (100% Accuracy)")
        logger.info("Providers: Google Geocoding API, Nominatim, BigDataCloud, LocationIQ")
    else:
        logger.info("Location Service: HIGH ACCURACY MODE")
        logger.info("Providers: Nominatim, BigDataCloud, LocationIQ")
    logger.info("Cache: CLEARED on startup")
    logger.info("NOTE: IP location shows ISP/data center location - use GPS for exact location")
    logger.info("=" * 60)
    
    app.run(host='0.0.0.0', port=port, debug=debug)

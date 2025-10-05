# backend/services/location.py
import os
import requests
import redis
import json
import time
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass
from statistics import median, mode
import asyncio
import aiohttp
from urllib.parse import urlparse
from math import radians, cos, sin, asin, sqrt

logger = logging.getLogger(__name__)

@dataclass
class LocationResult:
    lat: float
    lon: float
    accuracy: float
    confidence: float
    provider: str
    city: str = ""
    state: str = ""
    country: str = ""
    country_code: str = ""
    suburb: str = ""
    neighbourhood: str = ""
    road: str = ""
    house_number: str = ""
    zipcode: str = ""
    formatted_address: str = ""
    source_type: str = "ip"
    elevation: float = 0.0
    timezone: str = ""
    accuracy_radius: float = 0.0
    postal_town: str = ""
    district: str = ""

class LocationServiceError(Exception):
    pass

class UltraAccurateLocationService:
    def __init__(self):
        self.redis_client = None
        self.session_timeout = 3600
        
        self.providers_config = {
            'google_maps': {'weight': 0.50, 'timeout': 5, 'priority': 1},
            'maxmind_city': {'weight': 0.20, 'timeout': 5, 'priority': 2},
            'ipgeolocation': {'weight': 0.15, 'timeout': 5, 'priority': 3},
            'ipinfo': {'weight': 0.05, 'timeout': 5, 'priority': 4},
            'ipstack': {'weight': 0.05, 'timeout': 5, 'priority': 5},
            'ipapi': {'weight': 0.03, 'timeout': 5, 'priority': 6},
            'ip2location': {'weight': 0.02, 'timeout': 5, 'priority': 7}
        }
        
        self.google_maps_key = os.getenv('GOOGLE_MAPS_API_KEY')
        self.ipgeolocation_key = os.getenv('IPGEOLOCATION_API_KEY')
        self.maxmind_license_key = os.getenv('MAXMIND_LICENSE_KEY')
        self.ipstack_key = os.getenv('IPSTACK_API_KEY')
        self.ip2location_key = os.getenv('IP2LOCATION_API_KEY')
        
        self.accuracy_threshold = 0.95
        self.confidence_threshold = 0.90
        self.spatial_threshold = 0.005
        
        try:
            redis_url = os.getenv('REDIS_URL')
            if redis_url:
                self.redis_client = redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=5)
            else:
                self.redis_client = redis.Redis(
                    host=os.getenv('REDIS_HOST', 'localhost'),
                    port=int(os.getenv('REDIS_PORT', 6379)),
                    db=0,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
            self.redis_client.ping()
            logger.info("Redis connected successfully")
        except Exception as e:
            logger.warning(f"Redis not available: {e}, using memory cache")
            self.redis_client = None
    
    async def get_ultra_accurate_location(self, ip_address: Optional[str] = None, 
                                        session_id: Optional[str] = None,
                                        browser_location: Optional[Dict] = None) -> LocationResult:
        
        # PRIORITY 1: Browser GPS + Google Maps (Most Accurate)
        if browser_location and self._validate_browser_location(browser_location):
            logger.info("Using browser GPS location for highest accuracy")
            lat = float(browser_location['latitude'])
            lon = float(browser_location['longitude'])
            accuracy_meters = browser_location.get('accuracy', 100)
            
            # Always try Google Maps first for GPS coordinates
            if self.google_maps_key:
                try:
                    logger.info(f"Enhancing GPS location with Google Maps: {lat:.6f}, {lon:.6f}")
                    enhanced = await self._reverse_geocode_google_detailed(lat, lon)
                    enhanced.accuracy = min(0.99, 1 - (accuracy_meters / 50000))
                    enhanced.confidence = 0.99
                    enhanced.source_type = "gps_google_maps"
                    enhanced.accuracy_radius = accuracy_meters
                    enhanced.provider = "gps+google_maps"
                    
                    logger.info(f"GPS + Google Maps success: {enhanced.city}, {enhanced.state}, {enhanced.country}")
                    logger.info(f"Detailed address: {enhanced.formatted_address}")
                    
                    if session_id and self.redis_client:
                        self._store_session_location(session_id, enhanced)
                    
                    return enhanced
                except Exception as e:
                    logger.error(f"Google Maps enhancement failed: {e}")
            
            # Fallback to Nominatim for GPS coordinates
            try:
                logger.info("Falling back to Nominatim for GPS enhancement")
                enhanced = await self._reverse_geocode_nominatim_detailed(lat, lon)
                enhanced.accuracy = min(0.95, 1 - (accuracy_meters / 50000))
                enhanced.confidence = 0.95
                enhanced.source_type = "gps_nominatim"
                enhanced.accuracy_radius = accuracy_meters
                enhanced.provider = "gps+nominatim"
                
                if session_id and self.redis_client:
                    self._store_session_location(session_id, enhanced)
                
                return enhanced
            except Exception as e:
                logger.warning(f"Nominatim enhancement failed: {e}")
                
                # Return basic GPS location without geocoding
                return LocationResult(
                    lat=lat,
                    lon=lon,
                    accuracy=min(0.90, 1 - (accuracy_meters / 50000)),
                    confidence=0.90,
                    provider="gps_raw",
                    source_type="gps",
                    accuracy_radius=accuracy_meters,
                    formatted_address=f"GPS Location: {lat:.6f}, {lon:.6f}"
                )
        
        # PRIORITY 2: Check session cache
        if session_id and self.redis_client:
            cached = self._get_session_location(session_id)
            if cached and cached.accuracy >= self.accuracy_threshold:
                logger.info(f"Using high-accuracy cached location: {cached.city}")
                return cached
        
        # PRIORITY 3: IP-based location (Less Accurate - Show Warning)
        logger.warning("No GPS location available - using less accurate IP-based location")
        
        if not ip_address or self._is_private_ip(ip_address):
            ip_address = await self._get_public_ip_enhanced()
            logger.info(f"Detected public IP: {ip_address}")
        
        # Try to get IP location with multiple providers
        provider_results = await self._query_enhanced_providers(ip_address)
        
        if not provider_results:
            logger.error("All location providers failed")
            
            # Return a result that indicates GPS is needed
            return LocationResult(
                lat=0.0,
                lon=0.0,
                accuracy=0.1,
                confidence=0.1,
                provider="needs_gps",
                source_type="error",
                city="Unknown",
                state="Unknown",
                country="Unknown",
                formatted_address="GPS location required for accurate weather",
                accuracy_radius=300000  # 300km uncertainty
            )
        
        # Calculate consensus from IP providers
        consensus_location = await self._calculate_ultra_consensus(provider_results, ip_address)
        
        # Try to enhance with Google Maps if available
        if self.google_maps_key and consensus_location.lat != 0 and consensus_location.lon != 0:
            try:
                enhanced = await self._enhance_with_google_maps_detailed(consensus_location)
                enhanced.source_type = "ip_enhanced"
                enhanced.accuracy = min(0.70, consensus_location.accuracy)  # Cap IP accuracy at 70%
                enhanced.accuracy_radius = 50000  # 50km uncertainty for IP
                
                if session_id and self.redis_client:
                    self._store_session_location(session_id, enhanced)
                
                return enhanced
            except Exception as e:
                logger.warning(f"Google Maps IP enhancement failed: {e}")
        
        # Mark IP-based results as less accurate
        consensus_location.source_type = "ip"
        consensus_location.accuracy = min(0.60, consensus_location.accuracy)  # Cap at 60%
        consensus_location.accuracy_radius = 100000  # 100km uncertainty
        
        if session_id and self.redis_client and consensus_location.accuracy >= 0.50:
            self._store_session_location(session_id, consensus_location)
        
        return consensus_location
    
    async def get_location_from_ip_enhanced(self, ip_address: Optional[str] = None, 
                                          session_id: Optional[str] = None) -> LocationResult:
        # Check session cache first
        if session_id and self.redis_client:
            cached = self._get_session_location(session_id)
            if cached:
                logger.info(f"Using cached location for session: {cached.city}")
                return cached
        
        if not ip_address or self._is_private_ip(ip_address):
            ip_address = await self._get_public_ip()
        
        logger.info(f"Getting location for IP: {ip_address}")
        
        providers_results = await self._query_multiple_providers(ip_address)
        
        if not providers_results:
            raise LocationServiceError("All location providers failed")
        
        consensus_location = self._calculate_consensus(providers_results)
        
        # Always try Google Maps for better accuracy
        if self.google_maps_key:
            enhanced_location = await self._enhance_with_google_maps_detailed(consensus_location)
        else:
            enhanced_location = await self._enhance_with_nominatim_detailed(consensus_location)
        
        # Mark as IP-based with limited accuracy
        enhanced_location.source_type = "ip"
        enhanced_location.accuracy = min(0.70, enhanced_location.accuracy)
        enhanced_location.accuracy_radius = 50000  # 50km uncertainty
        
        if session_id and self.redis_client:
            self._store_session_location(session_id, enhanced_location)
        
        return enhanced_location
    
    async def get_location_from_coordinates(self, lat: float, lon: float) -> LocationResult:
        if not self._validate_coordinates(lat, lon):
            raise LocationServiceError(f"Invalid coordinates: {lat}, {lon}")
        
        logger.info(f"Reverse geocoding coordinates: {lat:.6f}, {lon:.6f}")
        
        # Always prefer Google Maps for coordinates
        if self.google_maps_key:
            return await self._reverse_geocode_google_detailed(lat, lon)
        else:
            return await self._reverse_geocode_nominatim_detailed(lat, lon)
    
    async def _reverse_geocode_google_detailed(self, lat: float, lon: float) -> LocationResult:
        """Enhanced Google Maps reverse geocoding with detailed address components"""
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'latlng': f"{lat},{lon}",
            'key': self.google_maps_key,
            'result_type': 'street_address|route|neighborhood|locality|administrative_area_level_1|country',
            'language': 'en',
            'location_type': 'ROOFTOP'  # Request highest accuracy
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise LocationServiceError(f"Google Maps API error: {response.status}")
                
                data = await response.json()
                
                if data.get('status') != 'OK' or not data.get('results'):
                    raise LocationServiceError(f"Google Geocoding failed: {data.get('status')}")
                
                # Use the most detailed result
                result = data['results'][0]
                components = result.get('address_components', [])
                geometry = result.get('geometry', {})
                
                location = LocationResult(
                    lat=lat,
                    lon=lon,
                    accuracy=0.99,
                    confidence=0.99,
                    provider='google_maps',
                    source_type='gps',
                    formatted_address=result.get('formatted_address', ''),
                    accuracy_radius=10  # Very accurate with Google Maps
                )
                
                # Extract all address components
                for component in components:
                    types = component.get('types', [])
                    long_name = component.get('long_name', '')
                    short_name = component.get('short_name', '')
                    
                    if 'street_number' in types:
                        location.house_number = long_name
                    elif 'route' in types:
                        location.road = long_name
                    elif 'neighborhood' in types:
                        location.neighbourhood = long_name
                    elif 'sublocality_level_1' in types or 'sublocality' in types:
                        location.suburb = long_name
                    elif 'locality' in types:
                        location.city = long_name
                    elif 'postal_town' in types:
                        location.postal_town = long_name
                    elif 'administrative_area_level_2' in types:
                        location.district = long_name
                    elif 'administrative_area_level_1' in types:
                        location.state = long_name
                    elif 'country' in types:
                        location.country = long_name
                        location.country_code = short_name
                    elif 'postal_code' in types:
                        location.zipcode = long_name
                
                # Ensure city is set (fallback to postal_town or district)
                if not location.city:
                    location.city = location.postal_town or location.district or location.suburb or "Unknown"
                
                # Get location type for accuracy
                location_type = geometry.get('location_type', 'APPROXIMATE')
                accuracy_map = {
                    'ROOFTOP': 0.99,
                    'RANGE_INTERPOLATED': 0.95,
                    'GEOMETRIC_CENTER': 0.90,
                    'APPROXIMATE': 0.85
                }
                location.accuracy = accuracy_map.get(location_type, 0.85)
                
                logger.info(f"Google Maps geocoding success: {location.city}, {location.state}, {location.country}")
                return location
    
    async def _reverse_geocode_nominatim_detailed(self, lat: float, lon: float) -> LocationResult:
        """Enhanced Nominatim reverse geocoding with detailed address"""
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json',
            'addressdetails': 1,
            'zoom': 18,
            'extratags': 1,
            'namedetails': 1
        }
        headers = {'User-Agent': 'SkyVibeWeatherApp/4.0'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers, timeout=10) as response:
                if response.status != 200:
                    raise LocationServiceError(f"Nominatim API error: {response.status}")
                
                data = await response.json()
                address = data.get('address', {})
                
                location = LocationResult(
                    lat=lat,
                    lon=lon,
                    accuracy=0.90,
                    confidence=0.90,
                    provider='nominatim',
                    source_type='gps',
                    formatted_address=data.get('display_name', ''),
                    accuracy_radius=50
                )
                
                # Extract detailed address components
                location.house_number = address.get('house_number', '')
                location.road = address.get('road', '')
                location.neighbourhood = address.get('neighbourhood', '') or address.get('residential', '')
                location.suburb = address.get('suburb', '') or address.get('city_district', '')
                location.city = (address.get('city') or address.get('town') or 
                               address.get('village') or address.get('municipality', ''))
                location.district = address.get('county', '')
                location.state = address.get('state', '') or address.get('region', '')
                location.country = address.get('country', '')
                location.country_code = address.get('country_code', '').upper()
                location.zipcode = address.get('postcode', '')
                
                logger.info(f"Nominatim geocoding success: {location.city}, {location.state}, {location.country}")
                return location
    
    async def _enhance_with_google_maps_detailed(self, location: LocationResult) -> LocationResult:
        """Enhance an existing location with Google Maps details"""
        if not self.google_maps_key:
            return location
        
        try:
            enhanced = await self._reverse_geocode_google_detailed(location.lat, location.lon)
            
            # Preserve original accuracy metrics but use Google's address details
            enhanced.accuracy = min(0.99, location.accuracy + 0.10)
            enhanced.confidence = min(0.99, location.confidence + 0.05)
            enhanced.provider = f"{location.provider}+google_maps"
            enhanced.source_type = location.source_type
            enhanced.accuracy_radius = location.accuracy_radius
            
            return enhanced
        except Exception as e:
            logger.warning(f"Google Maps enhancement failed: {e}")
            return location
    
    async def _enhance_with_nominatim_detailed(self, location: LocationResult) -> LocationResult:
        """Enhance an existing location with Nominatim details"""
        try:
            enhanced = await self._reverse_geocode_nominatim_detailed(location.lat, location.lon)
            
            # Preserve original accuracy metrics but use Nominatim's address details
            enhanced.accuracy = min(0.95, location.accuracy + 0.05)
            enhanced.confidence = location.confidence
            enhanced.provider = f"{location.provider}+nominatim"
            enhanced.source_type = location.source_type
            enhanced.accuracy_radius = location.accuracy_radius
            
            return enhanced
        except Exception as e:
            logger.warning(f"Nominatim enhancement failed: {e}")
            return location
    
    async def _query_enhanced_providers(self, ip_address: str) -> List[LocationResult]:
        tasks = []
        
        # Only query providers that are configured
        if self.maxmind_license_key:
            tasks.append(self._query_maxmind_city_enhanced(ip_address))
        
        if self.ipgeolocation_key:
            tasks.append(self._query_ipgeolocation_enhanced(ip_address))
        
        if self.ipstack_key:
            tasks.append(self._query_ipstack(ip_address))
        
        if self.ip2location_key:
            tasks.append(self._query_ip2location(ip_address))
        
        # Always try free providers
        tasks.extend([
            self._query_ipinfo_enhanced(ip_address),
            self._query_ipapi_enhanced(ip_address),
            self._query_ip_api_com_enhanced(ip_address)
        ])
        
        results = []
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
                
                for task_result in completed_tasks:
                    if isinstance(task_result, LocationResult):
                        results.append(task_result)
                    elif isinstance(task_result, Exception):
                        logger.warning(f"Provider failed: {task_result}")
        except Exception as e:
            logger.error(f"Error querying enhanced providers: {e}")
        
        return results
    
    async def _query_multiple_providers(self, ip_address: str) -> List[LocationResult]:
        tasks = []
        
        if self.ipgeolocation_key:
            tasks.append(self._query_ipgeolocation(ip_address))
        
        tasks.extend([
            self._query_ipinfo(ip_address),
            self._query_ipapi(ip_address),
            self._query_ip_api_com(ip_address)
        ])
        
        if self.maxmind_license_key:
            tasks.append(self._query_maxmind(ip_address))
        
        results = []
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
                
                for task_result in completed_tasks:
                    if isinstance(task_result, LocationResult):
                        results.append(task_result)
                    elif isinstance(task_result, Exception):
                        logger.warning(f"Provider failed: {task_result}")
        except Exception as e:
            logger.error(f"Error querying providers: {e}")
        
        return results
    
    async def _query_ipgeolocation_enhanced(self, ip_address: str) -> LocationResult:
        if not self.ipgeolocation_key:
            raise LocationServiceError("IPGeolocation API key not configured")
            
        url = "https://api.ipgeolocation.io/ipgeo"
        params = {
            'apiKey': self.ipgeolocation_key,
            'ip': ip_address,
            'fields': 'geo,time_zone,isp'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=8) as response:
                if response.status != 200:
                    raise LocationServiceError(f"IPGeolocation API error: {response.status}")
                
                data = await response.json()
                
                return LocationResult(
                    lat=float(data['latitude']),
                    lon=float(data['longitude']),
                    accuracy=0.70,  # IP-based, limited accuracy
                    confidence=0.75,
                    provider='ipgeolocation',
                    city=data.get('city', ''),
                    state=data.get('state_prov', ''),
                    country=data.get('country_name', ''),
                    country_code=data.get('country_code2', ''),
                    zipcode=data.get('zipcode', ''),
                    timezone=data.get('time_zone', {}).get('name', ''),
                    source_type='ip',
                    accuracy_radius=50000
                )
    
    async def _query_ipgeolocation(self, ip_address: str) -> LocationResult:
        if not self.ipgeolocation_key:
            raise LocationServiceError("IPGeolocation API key not configured")
            
        url = f"https://api.ipgeolocation.io/ipgeo?apiKey={self.ipgeolocation_key}&ip={ip_address}&fields=geo,isp"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                if response.status != 200:
                    raise LocationServiceError(f"IPGeolocation API error: {response.status}")
                    
                data = await response.json()
                
                return LocationResult(
                    lat=float(data['latitude']),
                    lon=float(data['longitude']),
                    accuracy=0.65,
                    confidence=0.70,
                    provider='ipgeolocation',
                    city=data.get('city', ''),
                    state=data.get('state_prov', ''),
                    country=data.get('country_name', ''),
                    country_code=data.get('country_code2', ''),
                    zipcode=data.get('zipcode', ''),
                    source_type='ip',
                    accuracy_radius=50000
                )
    
    async def _query_ipinfo_enhanced(self, ip_address: str) -> LocationResult:
        url = f"https://ipinfo.io/{ip_address}/json"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=8) as response:
                if response.status != 200:
                    raise LocationServiceError(f"IPInfo API error: {response.status}")
                    
                data = await response.json()
                
                if 'loc' not in data:
                    raise LocationServiceError("No location data from IPInfo")
                
                lat, lon = map(float, data['loc'].split(','))
                
                return LocationResult(
                    lat=lat,
                    lon=lon,
                    accuracy=0.60,
                    confidence=0.65,
                    provider='ipinfo',
                    city=data.get('city', ''),
                    state=data.get('region', ''),
                    country=data.get('country', ''),
                    zipcode=data.get('postal', ''),
                    timezone=data.get('timezone', ''),
                    source_type='ip',
                    accuracy_radius=75000
                )
    
    async def _query_ipinfo(self, ip_address: str) -> LocationResult:
        url = f"https://ipinfo.io/{ip_address}/json"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                if response.status != 200:
                    raise LocationServiceError(f"IPInfo API error: {response.status}")
                    
                data = await response.json()
                
                if 'loc' not in data:
                    raise LocationServiceError("No location data from IPInfo")
                
                lat, lon = map(float, data['loc'].split(','))
                
                return LocationResult(
                    lat=lat,
                    lon=lon,
                    accuracy=0.55,
                    confidence=0.60,
                    provider='ipinfo',
                    city=data.get('city', ''),
                    state=data.get('region', ''),
                    country=data.get('country', ''),
                    zipcode=data.get('postal', ''),
                    source_type='ip',
                    accuracy_radius=75000
                )
    
    async def _query_ipapi_enhanced(self, ip_address: str) -> LocationResult:
        url = f"https://ipapi.co/{ip_address}/json/"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=8) as response:
                    if response.status == 429:
                        raise LocationServiceError("IPAPI rate limited")
                    
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' not in content_type:
                        raise LocationServiceError(f"Unexpected content type: {content_type}")
                    
                    data = await response.json()
                    
                    if 'error' in data:
                        raise LocationServiceError(f"IPAPI error: {data.get('reason')}")
                    
                    return LocationResult(
                        lat=float(data['latitude']),
                        lon=float(data['longitude']),
                        accuracy=0.55,
                        confidence=0.60,
                        provider='ipapi',
                        city=data.get('city', ''),
                        state=data.get('region', ''),
                        country=data.get('country_name', ''),
                        country_code=data.get('country_code', ''),
                        zipcode=data.get('postal', ''),
                        timezone=data.get('timezone', ''),
                        source_type='ip',
                        accuracy_radius=100000
                    )
            except aiohttp.ClientError as e:
                raise LocationServiceError(f"IPAPI connection error: {e}")
    
    async def _query_ipapi(self, ip_address: str) -> LocationResult:
        url = f"https://ipapi.co/{ip_address}/json/"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=5) as response:
                    if response.status == 429:
                        raise LocationServiceError("IPAPI rate limited")
                    
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' not in content_type:
                        raise LocationServiceError(f"Unexpected content type: {content_type}")
                    
                    data = await response.json()
                    
                    if 'error' in data:
                        raise LocationServiceError(f"IPAPI error: {data.get('reason')}")
                    
                    return LocationResult(
                        lat=float(data['latitude']),
                        lon=float(data['longitude']),
                        accuracy=0.50,
                        confidence=0.55,
                        provider='ipapi',
                        city=data.get('city', ''),
                        state=data.get('region', ''),
                        country=data.get('country_name', ''),
                        country_code=data.get('country_code', ''),
                        zipcode=data.get('postal', ''),
                        source_type='ip',
                        accuracy_radius=100000
                    )
            except aiohttp.ClientError as e:
                raise LocationServiceError(f"IPAPI connection error: {e}")
    
    async def _query_ip_api_com_enhanced(self, ip_address: str) -> LocationResult:
        url = f"http://ip-api.com/json/{ip_address}?fields=status,lat,lon,city,regionName,country,countryCode,zip,isp,org,as,timezone"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=8) as response:
                    if response.status == 429:
                        raise LocationServiceError("IP-API rate limited")
                    
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' not in content_type:
                        text = await response.text()
                        if 'rate' in text.lower():
                            raise LocationServiceError("IP-API rate limited")
                        raise LocationServiceError(f"Unexpected response from IP-API")
                    
                    data = await response.json()
                    
                    if data.get('status') == 'fail':
                        raise LocationServiceError(f"IP-API error: {data.get('message')}")
                    
                    return LocationResult(
                        lat=float(data['lat']),
                        lon=float(data['lon']),
                        accuracy=0.50,
                        confidence=0.55,
                        provider='ip-api',
                        city=data.get('city', ''),
                        state=data.get('regionName', ''),
                        country=data.get('country', ''),
                        country_code=data.get('countryCode', ''),
                        zipcode=data.get('zip', ''),
                        timezone=data.get('timezone', ''),
                        source_type='ip',
                        accuracy_radius=100000
                    )
            except aiohttp.ClientError as e:
                raise LocationServiceError(f"IP-API connection error: {e}")
    
    async def _query_ip_api_com(self, ip_address: str) -> LocationResult:
        url = f"http://ip-api.com/json/{ip_address}?fields=status,lat,lon,city,regionName,country,countryCode,zip,isp,org"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=5) as response:
                    if response.status == 429:
                        raise LocationServiceError("IP-API rate limited")
                    
                    data = await response.json()
                    
                    if data.get('status') == 'fail':
                        raise LocationServiceError(f"IP-API error: {data.get('message')}")
                    
                    return LocationResult(
                        lat=float(data['lat']),
                        lon=float(data['lon']),
                        accuracy=0.45,
                        confidence=0.50,
                        provider='ip-api',
                        city=data.get('city', ''),
                        state=data.get('regionName', ''),
                        country=data.get('country', ''),
                        country_code=data.get('countryCode', ''),
                        zipcode=data.get('zip', ''),
                        source_type='ip',
                        accuracy_radius=100000
                    )
            except Exception as e:
                raise LocationServiceError(f"IP-API error: {e}")
    
    async def _query_maxmind_city_enhanced(self, ip_address: str) -> LocationResult:
        if not self.maxmind_license_key:
            raise LocationServiceError("MaxMind API key not configured")
            
        url = f"https://geoip.maxmind.com/geoip/v2.1/city/{ip_address}"
        headers = {'Authorization': f'Basic {self.maxmind_license_key}'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    raise LocationServiceError(f"MaxMind API error: {response.status}")
                    
                data = await response.json()
                
                location = data.get('location', {})
                city_data = data.get('city', {})
                subdivisions = data.get('subdivisions', [{}])
                country_data = data.get('country', {})
                postal = data.get('postal', {})
                
                accuracy_radius = location.get('accuracy_radius', 50)
                
                return LocationResult(
                    lat=float(location.get('latitude', 0)),
                    lon=float(location.get('longitude', 0)),
                    accuracy=min(0.75, 1 - (accuracy_radius / 200)),
                    confidence=0.80,
                    provider='maxmind',
                    city=city_data.get('names', {}).get('en', ''),
                    state=subdivisions[0].get('names', {}).get('en', '') if subdivisions else '',
                    country=country_data.get('names', {}).get('en', ''),
                    country_code=country_data.get('iso_code', ''),
                    zipcode=postal.get('code', ''),
                    accuracy_radius=accuracy_radius * 1000,  # Convert to meters
                    timezone=location.get('time_zone', ''),
                    source_type='ip'
                )
    
    async def _query_maxmind(self, ip_address: str) -> LocationResult:
        if not self.maxmind_license_key:
            raise LocationServiceError("MaxMind API key not configured")
        
        url = f"https://geoip.maxmind.com/geoip/v2.1/city/{ip_address}"
        headers = {'Authorization': f'Basic {self.maxmind_license_key}'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=5) as response:
                if response.status != 200:
                    raise LocationServiceError(f"MaxMind API error: {response.status}")
                    
                data = await response.json()
                
                location = data.get('location', {})
                city_data = data.get('city', {})
                subdivisions = data.get('subdivisions', [{}])
                country_data = data.get('country', {})
                postal = data.get('postal', {})
                
                return LocationResult(
                    lat=float(location.get('latitude', 0)),
                    lon=float(location.get('longitude', 0)),
                    accuracy=0.70,
                    confidence=0.75,
                    provider='maxmind',
                    city=city_data.get('names', {}).get('en', ''),
                    state=subdivisions[0].get('names', {}).get('en', '') if subdivisions else '',
                    country=country_data.get('names', {}).get('en', ''),
                    country_code=country_data.get('iso_code', ''),
                    zipcode=postal.get('code', ''),
                    source_type='ip',
                    accuracy_radius=50000
                )
    
    async def _query_ipstack(self, ip_address: str) -> LocationResult:
        if not self.ipstack_key:
            raise LocationServiceError("IPStack API key not configured")
            
        url = f"http://api.ipstack.com/{ip_address}"
        params = {
            'access_key': self.ipstack_key,
            'fields': 'main,location'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=8) as response:
                if response.status != 200:
                    raise LocationServiceError(f"IPStack API error: {response.status}")
                    
                data = await response.json()
                
                if 'error' in data:
                    raise LocationServiceError(f"IPStack error: {data['error']}")
                
                return LocationResult(
                    lat=float(data.get('latitude', 0)),
                    lon=float(data.get('longitude', 0)),
                    accuracy=0.60,
                    confidence=0.65,
                    provider='ipstack',
                    city=data.get('city', ''),
                    state=data.get('region_name', ''),
                    country=data.get('country_name', ''),
                    country_code=data.get('country_code', ''),
                    zipcode=data.get('zip', ''),
                    source_type='ip',
                    accuracy_radius=75000
                )
    
    async def _query_ip2location(self, ip_address: str) -> LocationResult:
        if not self.ip2location_key:
            raise LocationServiceError("IP2Location API key not configured")
            
        url = "https://api.ip2location.io/"
        params = {
            'key': self.ip2location_key,
            'ip': ip_address,
            'format': 'json'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=8) as response:
                if response.status != 200:
                    raise LocationServiceError(f"IP2Location API error: {response.status}")
                    
                data = await response.json()
                
                return LocationResult(
                    lat=float(data.get('latitude', 0)),
                    lon=float(data.get('longitude', 0)),
                    accuracy=0.55,
                    confidence=0.60,
                    provider='ip2location',
                    city=data.get('city_name', ''),
                    state=data.get('region_name', ''),
                    country=data.get('country_name', ''),
                    country_code=data.get('country_code', ''),
                    zipcode=data.get('zip_code', ''),
                    source_type='ip',
                    accuracy_radius=100000
                )
    
    async def _calculate_ultra_consensus(self, results: List[LocationResult], ip_address: str) -> LocationResult:
        if not results:
            raise LocationServiceError("No valid location results")
        
        # Sort by confidence and accuracy
        sorted_results = sorted(results, key=lambda r: (r.confidence * r.accuracy), reverse=True)
        
        # Remove outliers
        filtered_results = self._remove_outliers(sorted_results)
        
        if not filtered_results:
            filtered_results = sorted_results[:3]
        
        # Calculate weighted average
        total_weight = 0
        weighted_lat = 0
        weighted_lon = 0
        
        for result in filtered_results:
            weight = result.confidence * result.accuracy
            weighted_lat += result.lat * weight
            weighted_lon += result.lon * weight
            total_weight += weight
        
        if total_weight == 0:
            consensus_lat = median([r.lat for r in filtered_results])
            consensus_lon = median([r.lon for r in filtered_results])
        else:
            consensus_lat = weighted_lat / total_weight
            consensus_lon = weighted_lon / total_weight
        
        best_result = filtered_results[0]
        
        # Calculate variance for accuracy assessment
        lat_variance = sum(abs(r.lat - consensus_lat) for r in filtered_results) / len(filtered_results)
        lon_variance = sum(abs(r.lon - consensus_lon) for r in filtered_results) / len(filtered_results)
        
        # Higher variance means lower accuracy
        variance_penalty = min(0.3, (lat_variance + lon_variance) * 10)
        consensus_accuracy = max(0.40, min(0.70, best_result.accuracy - variance_penalty))
        
        return LocationResult(
            lat=consensus_lat,
            lon=consensus_lon,
            accuracy=consensus_accuracy,
            confidence=0.65,
            provider=f"consensus_{len(filtered_results)}_providers",
            city=best_result.city,
            state=best_result.state,
            country=best_result.country,
            country_code=best_result.country_code,
            zipcode=best_result.zipcode,
            timezone=best_result.timezone,
            source_type='ip',
            accuracy_radius=100000  # 100km for IP-based consensus
        )
    
    def _calculate_consensus(self, results: List[LocationResult]) -> LocationResult:
        if not results:
            raise LocationServiceError("No valid location results")
        
        # Use median for more robust consensus
        consensus_lat = median([r.lat for r in results])
        consensus_lon = median([r.lon for r in results])
        
        # Find best result for metadata
        best_result = max(results, key=lambda r: r.confidence * r.accuracy)
        
        # Calculate accuracy based on agreement
        accuracy_variance = max([
            abs(r.lat - consensus_lat) + abs(r.lon - consensus_lon) 
            for r in results
        ])
        
        final_accuracy = max(0.30, min(0.60, 1 - (accuracy_variance * 5)))
        
        return LocationResult(
            lat=consensus_lat,
            lon=consensus_lon,
            accuracy=final_accuracy,
            confidence=0.60,
            provider=f"consensus_{len(results)}",
            city=best_result.city,
            state=best_result.state,
            country=best_result.country,
            country_code=best_result.country_code,
            zipcode=best_result.zipcode,
            source_type='ip',
            accuracy_radius=100000
        )
    
    def _remove_outliers(self, results: List[LocationResult]) -> List[LocationResult]:
        if len(results) < 3:
            return results
        
        median_lat = median([r.lat for r in results])
        median_lon = median([r.lon for r in results])
        
        filtered = []
        for result in results:
            distance = self._calculate_distance(result.lat, result.lon, median_lat, median_lon)
            if distance < 200:  # Within 200km
                filtered.append(result)
        
        return filtered if filtered else results[:3]
    
    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points in kilometers"""
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 6371  # Earth radius in kilometers
        
        return c * r
    
    async def _get_public_ip(self) -> str:
        urls = [
            'https://api.ipify.org',
            'https://checkip.amazonaws.com',
            'https://icanhazip.com'
        ]
        
        for url in urls:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=3) as response:
                        ip = (await response.text()).strip()
                        if self._is_valid_ip(ip):
                            return ip
            except:
                continue
        
        raise LocationServiceError("Could not determine public IP")
    
    async def _get_public_ip_enhanced(self) -> str:
        ip_services = [
            'https://api.ipify.org',
            'https://checkip.amazonaws.com',
            'https://icanhazip.com',
            'https://ipecho.net/plain',
            'https://myexternalip.com/raw'
        ]
        
        for service in ip_services:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(service, timeout=5) as response:
                        ip = (await response.text()).strip()
                        if self._is_valid_ip(ip) and not self._is_private_ip(ip):
                            return ip
            except:
                continue
        
        raise LocationServiceError("Could not determine public IP address")
    
    def _validate_browser_location(self, browser_location: Dict) -> bool:
        try:
            lat = float(browser_location.get('latitude', 0))
            lon = float(browser_location.get('longitude', 0))
            accuracy = browser_location.get('accuracy', 0)
            
            return (
                -90 <= lat <= 90 and 
                -180 <= lon <= 180 and 
                lat != 0 and lon != 0 and  # Ensure not default values
                0 < accuracy < 50000  # Accuracy in meters, max 50km
            )
        except:
            return False
    
    def _validate_coordinates(self, lat: float, lon: float) -> bool:
        try:
            return -90 <= float(lat) <= 90 and -180 <= float(lon) <= 180
        except:
            return False
    
    def _is_private_ip(self, ip: str) -> bool:
        private_ranges = ['127.', '10.', '172.16.', '192.168.', '::1', 'localhost']
        return any(ip.startswith(prefix) for prefix in private_ranges)
    
    def _is_valid_ip(self, ip: str) -> bool:
        try:
            parts = ip.split('.')
            return len(parts) == 4 and all(0 <= int(part) <= 255 for part in parts)
        except:
            return False
    
    def _get_session_location(self, session_id: str) -> Optional[LocationResult]:
        if not self.redis_client:
            return None
        
        try:
            data = self.redis_client.get(f"location:{session_id}")
            if data:
                location_dict = json.loads(data)
                return LocationResult(**location_dict)
        except Exception as e:
            logger.warning(f"Redis get error: {e}")
        return None
    
    def _store_session_location(self, session_id: str, location: LocationResult) -> None:
        if not self.redis_client:
            return
        
        try:
            self.redis_client.setex(
                f"location:{session_id}",
                self.session_timeout,
                json.dumps(location.__dict__)
            )
        except Exception as e:
            logger.warning(f"Redis set error: {e}")
    
    def format_full_location(self, location: LocationResult) -> str:
        """Format location for display with all available details"""
        parts = []
        
        # Add street address if available
        if location.house_number and location.road:
            parts.append(f"{location.house_number} {location.road}")
        elif location.road:
            parts.append(location.road)
        
        # Add neighborhood/suburb
        if location.neighbourhood:
            parts.append(location.neighbourhood)
        elif location.suburb:
            parts.append(location.suburb)
        
        # Add city (required)
        if location.city:
            parts.append(location.city)
        elif location.postal_town:
            parts.append(location.postal_town)
        
        # Add state/region
        if location.state:
            parts.append(location.state)
        elif location.district:
            parts.append(location.district)
        
        # Add country
        if location.country:
            parts.append(location.country)
        
        # If we have a formatted address from Google, prefer that
        if location.formatted_address and len(location.formatted_address) > 10:
            return location.formatted_address
        
        # Otherwise build from components
        return ', '.join(parts) if parts else 'Unknown Location'

# Create singleton instance
location_service = UltraAccurateLocationService()
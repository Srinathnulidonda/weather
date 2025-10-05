#backend/services/location.py
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

class LocationServiceError(Exception):
    pass

class EnhancedLocationService:
    def __init__(self):
        self.redis_client = None
        self.session_timeout = 3600
        self.providers_config = {
            'ipinfo': {'weight': 0.2, 'timeout': 5},
            'ipgeolocation': {'weight': 0.25, 'timeout': 5},
            'ipapi': {'weight': 0.15, 'timeout': 5},
            'maxmind': {'weight': 0.2, 'timeout': 5},
            'cloudflare': {'weight': 0.2, 'timeout': 5}
        }
        self.google_maps_key = os.getenv('GOOGLE_MAPS_API_KEY')
        self.ipgeolocation_key = os.getenv('IPGEOLOCATION_API_KEY')
        self.maxmind_key = os.getenv('MAXMIND_API_KEY')
        
        try:
            redis_url = os.getenv('REDIS_URL')
            if redis_url:
                self.redis_client = redis.from_url(redis_url, decode_responses=True)
            else:
                self.redis_client = redis.Redis(
                    host=os.getenv('REDIS_HOST', 'localhost'),
                    port=int(os.getenv('REDIS_PORT', 6379)),
                    db=0,
                    decode_responses=True
                )
            self.redis_client.ping()
            logger.info("Redis connected successfully")
        except Exception as e:
            logger.warning(f"Redis not available: {e}, using memory cache")
            self.redis_client = None
    
    async def get_location_from_ip_enhanced(self, ip_address: Optional[str] = None, 
                                          session_id: Optional[str] = None) -> LocationResult:
        if session_id and self.redis_client:
            cached = self._get_session_location(session_id)
            if cached:
                return cached
        
        if not ip_address or self._is_private_ip(ip_address):
            ip_address = await self._get_public_ip()
        
        providers_results = await self._query_multiple_providers(ip_address)
        
        if not providers_results:
            raise LocationServiceError("All location providers failed")
        
        consensus_location = self._calculate_consensus(providers_results)
        
        if self.google_maps_key:
            enhanced_location = await self._enhance_with_google_maps(consensus_location)
        else:
            enhanced_location = await self._enhance_with_nominatim(consensus_location)
        
        if session_id and self.redis_client:
            self._store_session_location(session_id, enhanced_location)
        
        return enhanced_location
    
    async def get_location_from_coordinates(self, lat: float, lon: float) -> LocationResult:
        if not self._validate_coordinates(lat, lon):
            raise LocationServiceError(f"Invalid coordinates: {lat}, {lon}")
        
        if self.google_maps_key:
            return await self._reverse_geocode_google(lat, lon)
        else:
            return await self._reverse_geocode_nominatim_coords(lat, lon)
    
    async def _query_multiple_providers(self, ip_address: str) -> List[LocationResult]:
        tasks = []
        
        if self.ipgeolocation_key:
            tasks.append(self._query_ipgeolocation(ip_address))
        
        tasks.extend([
            self._query_ipinfo(ip_address),
            self._query_ipapi(ip_address),
            self._query_ip_api_com(ip_address),
            self._query_cloudflare_trace(ip_address)
        ])
        
        if self.maxmind_key:
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
    
    async def _query_ipgeolocation(self, ip_address: str) -> LocationResult:
        url = f"https://api.ipgeolocation.io/ipgeo?apiKey={self.ipgeolocation_key}&ip={ip_address}&fields=geo,isp"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                data = await response.json()
                
                return LocationResult(
                    lat=float(data['latitude']),
                    lon=float(data['longitude']),
                    accuracy=self._calculate_accuracy(data.get('isp', ''), 'ipgeolocation'),
                    confidence=0.85,
                    provider='ipgeolocation',
                    city=data.get('city', ''),
                    state=data.get('state_prov', ''),
                    country=data.get('country_name', ''),
                    country_code=data.get('country_code2', ''),
                    zipcode=data.get('zipcode', '')
                )
    
    async def _query_ipinfo(self, ip_address: str) -> LocationResult:
        url = f"https://ipinfo.io/{ip_address}/json"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                data = await response.json()
                
                if 'loc' not in data:
                    raise LocationServiceError("No location data from IPInfo")
                
                lat, lon = map(float, data['loc'].split(','))
                
                return LocationResult(
                    lat=lat,
                    lon=lon,
                    accuracy=self._calculate_accuracy(data.get('org', ''), 'ipinfo'),
                    confidence=0.75,
                    provider='ipinfo',
                    city=data.get('city', ''),
                    state=data.get('region', ''),
                    country=data.get('country', ''),
                    zipcode=data.get('postal', '')
                )
    
    async def _query_ipapi(self, ip_address: str) -> LocationResult:
        url = f"https://ipapi.co/{ip_address}/json/"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=5) as response:
                    if response.status == 429:
                        # Rate limited - skip this provider
                        raise LocationServiceError("IPAPI rate limited")
                    
                    # Check content type before parsing JSON
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' not in content_type:
                        raise LocationServiceError(f"Unexpected content type: {content_type}")
                    
                    data = await response.json()
                    
                    if 'error' in data:
                        raise LocationServiceError(f"IPAPI error: {data.get('reason')}")
                    
                    return LocationResult(
                        lat=float(data['latitude']),
                        lon=float(data['longitude']),
                        accuracy=self._calculate_accuracy(data.get('org', ''), 'ipapi'),
                        confidence=0.7,
                        provider='ipapi',
                        city=data.get('city', ''),
                        state=data.get('region', ''),
                        country=data.get('country_name', ''),
                        country_code=data.get('country_code', ''),
                        zipcode=data.get('postal', '')
                    )
            except aiohttp.ClientError as e:
                raise LocationServiceError(f"IPAPI connection error: {e}")

    
    async def _query_ip_api_com(self, ip_address: str) -> LocationResult:
        url = f"http://ip-api.com/json/{ip_address}?fields=status,lat,lon,city,regionName,country,countryCode,zip,isp,org"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                data = await response.json()
                
                if data.get('status') == 'fail':
                    raise LocationServiceError(f"IP-API error: {data.get('message')}")
                
                return LocationResult(
                    lat=float(data['lat']),
                    lon=float(data['lon']),
                    accuracy=self._calculate_accuracy(data.get('isp', ''), 'ip-api'),
                    confidence=0.65,
                    provider='ip-api',
                    city=data.get('city', ''),
                    state=data.get('regionName', ''),
                    country=data.get('country', ''),
                    country_code=data.get('countryCode', ''),
                    zipcode=data.get('zip', '')
                )
    
    async def _query_cloudflare_trace(self, ip_address: str) -> LocationResult:
        url = "https://www.cloudflare.com/cdn-cgi/trace"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                text = await response.text()
                data = dict(line.split('=') for line in text.strip().split('\n') if '=' in line)
                
                if 'loc' not in data:
                    raise LocationServiceError("No location from Cloudflare")
                
                country_code = data.get('loc', '').upper()
                
                geo_url = f"https://ipapi.co/{ip_address}/json/"
                async with session.get(geo_url, timeout=3) as geo_response:
                    geo_data = await geo_response.json()
                    
                    return LocationResult(
                        lat=float(geo_data['latitude']),
                        lon=float(geo_data['longitude']),
                        accuracy=0.8,
                        confidence=0.9,
                        provider='cloudflare',
                        city=geo_data.get('city', ''),
                        state=geo_data.get('region', ''),
                        country=geo_data.get('country_name', ''),
                        country_code=country_code
                    )
    
    async def _query_maxmind(self, ip_address: str) -> LocationResult:
        if not self.maxmind_key:
            raise LocationServiceError("MaxMind API key not configured")
        
        url = f"https://geoip.maxmind.com/geoip/v2.1/city/{ip_address}"
        headers = {'Authorization': f'Basic {self.maxmind_key}'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=5) as response:
                data = await response.json()
                
                location = data.get('location', {})
                city_data = data.get('city', {})
                subdivisions = data.get('subdivisions', [{}])
                country_data = data.get('country', {})
                postal = data.get('postal', {})
                
                return LocationResult(
                    lat=float(location.get('latitude', 0)),
                    lon=float(location.get('longitude', 0)),
                    accuracy=location.get('accuracy_radius', 100) / 100,
                    confidence=0.95,
                    provider='maxmind',
                    city=city_data.get('names', {}).get('en', ''),
                    state=subdivisions[0].get('names', {}).get('en', ''),
                    country=country_data.get('names', {}).get('en', ''),
                    country_code=country_data.get('iso_code', ''),
                    zipcode=postal.get('code', '')
                )
    
    def _calculate_consensus(self, results: List[LocationResult]) -> LocationResult:
        if not results:
            raise LocationServiceError("No valid location results")
        
        weighted_lats = []
        weighted_lons = []
        total_weight = 0
        
        for result in results:
            weight = self.providers_config.get(result.provider, {}).get('weight', 0.1)
            confidence_weight = weight * result.confidence
            
            weighted_lats.append(result.lat * confidence_weight)
            weighted_lons.append(result.lon * confidence_weight)
            total_weight += confidence_weight
        
        if total_weight == 0:
            consensus_lat = median([r.lat for r in results])
            consensus_lon = median([r.lon for r in results])
        else:
            consensus_lat = sum(weighted_lats) / total_weight
            consensus_lon = sum(weighted_lons) / total_weight
        
        best_result = max(results, key=lambda r: r.confidence)
        
        overall_confidence = min(0.99, sum(r.confidence for r in results) / len(results))
        
        accuracy_variance = max([
            abs(r.lat - consensus_lat) + abs(r.lon - consensus_lon) 
            for r in results
        ])
        
        final_accuracy = max(0.1, 1 - (accuracy_variance * 10))
        
        return LocationResult(
            lat=consensus_lat,
            lon=consensus_lon,
            accuracy=final_accuracy,
            confidence=overall_confidence,
            provider=f"consensus-{len(results)}",
            city=best_result.city,
            state=best_result.state,
            country=best_result.country,
            country_code=best_result.country_code,
            zipcode=best_result.zipcode
        )
    
    async def _enhance_with_google_maps(self, location: LocationResult) -> LocationResult:
        url = f"https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'latlng': f"{location.lat},{location.lon}",
            'key': self.google_maps_key,
            'result_type': 'street_address|route|neighborhood|locality'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                data = await response.json()
                
                if data.get('status') != 'OK' or not data.get('results'):
                    return location
                
                result = data['results'][0]
                components = result.get('address_components', [])
                
                enhanced = LocationResult(
                    lat=location.lat,
                    lon=location.lon,
                    accuracy=0.99,
                    confidence=min(0.99, location.confidence + 0.1),
                    provider=f"{location.provider}+google",
                    formatted_address=result.get('formatted_address', '')
                )
                
                for component in components:
                    types = component.get('types', [])
                    long_name = component.get('long_name', '')
                    
                    if 'street_number' in types:
                        enhanced.house_number = long_name
                    elif 'route' in types:
                        enhanced.road = long_name
                    elif 'neighborhood' in types:
                        enhanced.neighbourhood = long_name
                    elif 'sublocality' in types:
                        enhanced.suburb = long_name
                    elif 'locality' in types:
                        enhanced.city = long_name
                    elif 'administrative_area_level_1' in types:
                        enhanced.state = long_name
                    elif 'country' in types:
                        enhanced.country = long_name
                        enhanced.country_code = component.get('short_name', '')
                    elif 'postal_code' in types:
                        enhanced.zipcode = long_name
                
                return enhanced
    
    async def _enhance_with_nominatim(self, location: LocationResult) -> LocationResult:
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            'lat': location.lat,
            'lon': location.lon,
            'format': 'json',
            'addressdetails': 1,
            'zoom': 18
        }
        headers = {'User-Agent': 'SkyVibeWeatherApp/3.0'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers, timeout=10) as response:
                data = await response.json()
                
                address = data.get('address', {})
                
                enhanced = LocationResult(
                    lat=location.lat,
                    lon=location.lon,
                    accuracy=min(0.95, location.accuracy + 0.05),
                    confidence=location.confidence,
                    provider=f"{location.provider}+nominatim",
                    road=address.get('road', ''),
                    house_number=address.get('house_number', ''),
                    suburb=address.get('suburb', ''),
                    neighbourhood=address.get('neighbourhood', ''),
                    city=address.get('city') or address.get('town', ''),
                    state=address.get('state', ''),
                    country=address.get('country', ''),
                    country_code=address.get('country_code', '').upper(),
                    zipcode=address.get('postcode', ''),
                    formatted_address=data.get('display_name', '')
                )
                
                return enhanced
    
    async def _reverse_geocode_google(self, lat: float, lon: float) -> LocationResult:
        url = f"https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'latlng': f"{lat},{lon}",
            'key': self.google_maps_key,
            'result_type': 'street_address|route|neighborhood|locality'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                data = await response.json()
                
                if data.get('status') != 'OK' or not data.get('results'):
                    raise LocationServiceError("Google Geocoding failed")
                
                result = data['results'][0]
                components = result.get('address_components', [])
                
                location = LocationResult(
                    lat=lat,
                    lon=lon,
                    accuracy=0.99,
                    confidence=0.99,
                    provider='google-reverse',
                    source_type='gps',
                    formatted_address=result.get('formatted_address', '')
                )
                
                for component in components:
                    types = component.get('types', [])
                    long_name = component.get('long_name', '')
                    
                    if 'street_number' in types:
                        location.house_number = long_name
                    elif 'route' in types:
                        location.road = long_name
                    elif 'neighborhood' in types:
                        location.neighbourhood = long_name
                    elif 'sublocality' in types:
                        location.suburb = long_name
                    elif 'locality' in types:
                        location.city = long_name
                    elif 'administrative_area_level_1' in types:
                        location.state = long_name
                    elif 'country' in types:
                        location.country = long_name
                        location.country_code = component.get('short_name', '')
                    elif 'postal_code' in types:
                        location.zipcode = long_name
                
                return location
    
    async def _reverse_geocode_nominatim_coords(self, lat: float, lon: float) -> LocationResult:
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json',
            'addressdetails': 1,
            'zoom': 18
        }
        headers = {'User-Agent': 'SkyVibeWeatherApp/3.0'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers, timeout=10) as response:
                data = await response.json()
                
                address = data.get('address', {})
                
                return LocationResult(
                    lat=lat,
                    lon=lon,
                    accuracy=0.85,
                    confidence=0.85,
                    provider='nominatim-reverse',
                    source_type='gps',
                    road=address.get('road', ''),
                    house_number=address.get('house_number', ''),
                    suburb=address.get('suburb', ''),
                    neighbourhood=address.get('neighbourhood', ''),
                    city=address.get('city') or address.get('town', ''),
                    state=address.get('state', ''),
                    country=address.get('country', ''),
                    country_code=address.get('country_code', '').upper(),
                    zipcode=address.get('postcode', ''),
                    formatted_address=data.get('display_name', '')
                )
    
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
    
    def _calculate_accuracy(self, isp_info: str, provider: str) -> float:
        base_accuracy = 0.5
        
        if any(term in isp_info.lower() for term in ['mobile', 'cellular', '4g', '5g', 'lte']):
            base_accuracy += 0.3
        elif any(term in isp_info.lower() for term in ['fiber', 'broadband', 'cable']):
            base_accuracy += 0.4
        elif 'wifi' in isp_info.lower():
            base_accuracy += 0.35
        
        provider_bonus = {
            'maxmind': 0.15,
            'ipgeolocation': 0.1,
            'cloudflare': 0.1,
            'ipinfo': 0.05
        }.get(provider, 0)
        
        return min(0.95, base_accuracy + provider_bonus)
    
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
                return LocationResult(**json.loads(data))
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
        parts = []
        
        if location.house_number and location.road:
            parts.append(f"{location.house_number} {location.road}")
        elif location.road:
            parts.append(location.road)
        
        if location.suburb:
            parts.append(location.suburb)
        elif location.neighbourhood:
            parts.append(location.neighbourhood)
        
        if location.city:
            parts.append(location.city)
        
        if location.state:
            parts.append(location.state)
        
        if location.country:
            parts.append(location.country)
        
        return ', '.join(parts) if parts else location.formatted_address or 'Unknown Location'

location_service = EnhancedLocationService()
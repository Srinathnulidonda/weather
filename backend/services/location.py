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

class LocationServiceError(Exception):
    pass

class UltraAccurateLocationService:
    def __init__(self):
        self.redis_client = None
        self.session_timeout = 3600
        
        self.providers_config = {
            'maxmind_city': {'weight': 0.35, 'timeout': 5, 'priority': 1},
            'ipgeolocation': {'weight': 0.25, 'timeout': 5, 'priority': 2},
            'ipinfo': {'weight': 0.15, 'timeout': 5, 'priority': 3},
            'ipstack': {'weight': 0.1, 'timeout': 5, 'priority': 4},
            'ipapi': {'weight': 0.08, 'timeout': 5, 'priority': 5},
            'ip2location': {'weight': 0.07, 'timeout': 5, 'priority': 6}
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
    
    async def get_ultra_accurate_location(self, ip_address: Optional[str] = None, 
                                        session_id: Optional[str] = None,
                                        browser_location: Optional[Dict] = None) -> LocationResult:
        
        if browser_location and self._validate_browser_location(browser_location):
            logger.info("Using browser GPS location (highest accuracy)")
            lat = float(browser_location['latitude'])
            lon = float(browser_location['longitude'])
            accuracy_meters = browser_location.get('accuracy', 100)
            
            if self.google_maps_key:
                try:
                    enhanced = await self._reverse_geocode_google(lat, lon)
                    enhanced.accuracy = min(0.99, 1 - (accuracy_meters / 10000))
                    enhanced.confidence = 0.99
                    enhanced.source_type = "gps"
                    enhanced.accuracy_radius = accuracy_meters
                    logger.info(f"GPS location enhanced with Google Maps: {enhanced.city}, {enhanced.state}")
                    
                    if session_id and self.redis_client:
                        self._store_session_location(session_id, enhanced)
                    
                    return enhanced
                except Exception as e:
                    logger.warning(f"Google Maps enhancement failed: {e}")
            
            try:
                enhanced = await self._reverse_geocode_nominatim_coords(lat, lon)
                enhanced.accuracy = min(0.95, 1 - (accuracy_meters / 10000))
                enhanced.confidence = 0.95
                enhanced.source_type = "gps"
                enhanced.accuracy_radius = accuracy_meters
                
                if session_id and self.redis_client:
                    self._store_session_location(session_id, enhanced)
                
                return enhanced
            except Exception as e:
                logger.warning(f"Nominatim enhancement failed: {e}")
        
        if session_id and self.redis_client:
            cached = self._get_session_location(session_id)
            if cached and cached.accuracy >= self.accuracy_threshold:
                logger.info("Using high-accuracy cached location")
                return cached
        
        if not ip_address or self._is_private_ip(ip_address):
            ip_address = await self._get_public_ip_enhanced()
        
        provider_results = await self._query_enhanced_providers(ip_address)
        
        if not provider_results:
            raise LocationServiceError("All location providers failed")
        
        consensus_location = await self._calculate_ultra_consensus(provider_results, ip_address)
        enhanced_location = await self._ultra_enhance_location(consensus_location)
        final_location = await self._validate_and_refine(enhanced_location, ip_address)
        
        if session_id and self.redis_client and final_location.accuracy >= 0.85:
            self._store_session_location(session_id, final_location)
        
        return final_location
    
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
    
    async def _process_browser_location(self, browser_location: Dict) -> LocationResult:
        lat = float(browser_location['latitude'])
        lon = float(browser_location['longitude'])
        accuracy_meters = browser_location.get('accuracy', 100)
        
        accuracy_score = max(0.1, min(0.99, 1 - (accuracy_meters / 1000)))
        
        enhanced = await self._ultra_enhance_location(LocationResult(
            lat=lat,
            lon=lon,
            accuracy=accuracy_score,
            confidence=0.98,
            provider="browser_gps",
            source_type="gps",
            accuracy_radius=accuracy_meters
        ))
        
        return enhanced
    
    async def _query_enhanced_providers(self, ip_address: str) -> List[LocationResult]:
        tasks = []
        
        if self.maxmind_license_key:
            tasks.append(self._query_maxmind_city_enhanced(ip_address))
        
        if self.ipgeolocation_key:
            tasks.append(self._query_ipgeolocation_enhanced(ip_address))
        
        if self.ipstack_key:
            tasks.append(self._query_ipstack(ip_address))
        
        if self.ip2location_key:
            tasks.append(self._query_ip2location(ip_address))
        
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
            self._query_ip_api_com(ip_address),
            self._query_cloudflare_trace(ip_address)
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
    
    async def _query_maxmind_city_enhanced(self, ip_address: str) -> LocationResult:
        url = f"https://geoip.maxmind.com/geoip/v2.1/city/{ip_address}"
        headers = {'Authorization': f'Basic {self.maxmind_license_key}'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as response:
                data = await response.json()
                
                location = data.get('location', {})
                city_data = data.get('city', {})
                subdivisions = data.get('subdivisions', [{}])
                country_data = data.get('country', {})
                postal = data.get('postal', {})
                traits = data.get('traits', {})
                
                accuracy_radius = location.get('accuracy_radius', 50)
                accuracy_score = max(0.1, min(0.95, 1 - (accuracy_radius / 100)))
                
                return LocationResult(
                    lat=float(location.get('latitude', 0)),
                    lon=float(location.get('longitude', 0)),
                    accuracy=accuracy_score,
                    confidence=0.95,
                    provider='maxmind_city',
                    city=city_data.get('names', {}).get('en', ''),
                    state=subdivisions[0].get('names', {}).get('en', ''),
                    country=country_data.get('names', {}).get('en', ''),
                    country_code=country_data.get('iso_code', ''),
                    zipcode=postal.get('code', ''),
                    accuracy_radius=accuracy_radius,
                    timezone=location.get('time_zone', '')
                )
    
    async def _query_ipgeolocation_enhanced(self, ip_address: str) -> LocationResult:
        url = f"https://api.ipgeolocation.io/ipgeo"
        params = {
            'apiKey': self.ipgeolocation_key,
            'ip': ip_address,
            'fields': 'geo,time_zone,isp,threat'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=8) as response:
                data = await response.json()
                
                isp_type = data.get('isp', '').lower()
                accuracy = self._calculate_enhanced_accuracy(isp_type, 'ipgeolocation', data)
                
                return LocationResult(
                    lat=float(data['latitude']),
                    lon=float(data['longitude']),
                    accuracy=accuracy,
                    confidence=0.88,
                    provider='ipgeolocation_enhanced',
                    city=data.get('city', ''),
                    state=data.get('state_prov', ''),
                    country=data.get('country_name', ''),
                    country_code=data.get('country_code2', ''),
                    zipcode=data.get('zipcode', ''),
                    timezone=data.get('time_zone', {}).get('name', '')
                )
    
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
    
    async def _query_ipstack(self, ip_address: str) -> LocationResult:
        url = f"http://api.ipstack.com/{ip_address}"
        params = {
            'access_key': self.ipstack_key,
            'fields': 'main,location'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=8) as response:
                data = await response.json()
                
                if 'error' in data:
                    raise LocationServiceError(f"IPStack error: {data['error']}")
                
                location_data = data.get('location', {})
                accuracy = self._calculate_enhanced_accuracy(
                    data.get('connection', {}).get('isp', ''), 
                    'ipstack', 
                    data
                )
                
                return LocationResult(
                    lat=float(data.get('latitude', 0)),
                    lon=float(data.get('longitude', 0)),
                    accuracy=accuracy,
                    confidence=0.82,
                    provider='ipstack',
                    city=data.get('city', ''),
                    state=data.get('region_name', ''),
                    country=data.get('country_name', ''),
                    country_code=data.get('country_code', ''),
                    zipcode=data.get('zip', '')
                )
    
    async def _query_ip2location(self, ip_address: str) -> LocationResult:
        url = f"https://api.ip2location.io/"
        params = {
            'key': self.ip2location_key,
            'ip': ip_address,
            'format': 'json'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=8) as response:
                data = await response.json()
                
                accuracy = self._calculate_enhanced_accuracy(
                    data.get('as', ''), 
                    'ip2location', 
                    data
                )
                
                return LocationResult(
                    lat=float(data.get('latitude', 0)),
                    lon=float(data.get('longitude', 0)),
                    accuracy=accuracy,
                    confidence=0.78,
                    provider='ip2location',
                    city=data.get('city_name', ''),
                    state=data.get('region_name', ''),
                    country=data.get('country_name', ''),
                    country_code=data.get('country_code', ''),
                    zipcode=data.get('zip_code', '')
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
    
    async def _query_ipinfo_enhanced(self, ip_address: str) -> LocationResult:
        url = f"https://ipinfo.io/{ip_address}/json"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=8) as response:
                data = await response.json()
                
                if 'loc' not in data:
                    raise LocationServiceError("No location data from IPInfo")
                
                lat, lon = map(float, data['loc'].split(','))
                org_info = data.get('org', '')
                
                accuracy = self._calculate_enhanced_accuracy(org_info, 'ipinfo', data)
                
                return LocationResult(
                    lat=lat,
                    lon=lon,
                    accuracy=accuracy,
                    confidence=0.75,
                    provider='ipinfo_enhanced',
                    city=data.get('city', ''),
                    state=data.get('region', ''),
                    country=data.get('country', ''),
                    zipcode=data.get('postal', ''),
                    timezone=data.get('timezone', '')
                )
    
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
                    
                    org_info = data.get('org', '')
                    accuracy = self._calculate_enhanced_accuracy(org_info, 'ipapi', data)
                    
                    return LocationResult(
                        lat=float(data['latitude']),
                        lon=float(data['longitude']),
                        accuracy=accuracy,
                        confidence=0.7,
                        provider='ipapi_enhanced',
                        city=data.get('city', ''),
                        state=data.get('region', ''),
                        country=data.get('country_name', ''),
                        country_code=data.get('country_code', ''),
                        zipcode=data.get('postal', ''),
                        timezone=data.get('timezone', '')
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
    
    async def _query_ip_api_com_enhanced(self, ip_address: str) -> LocationResult:
        url = f"http://ip-api.com/json/{ip_address}?fields=status,lat,lon,city,regionName,country,countryCode,zip,isp,org,as,timezone"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=8) as response:
                data = await response.json()
                
                if data.get('status') == 'fail':
                    raise LocationServiceError(f"IP-API error: {data.get('message')}")
                
                isp_info = data.get('isp', '')
                accuracy = self._calculate_enhanced_accuracy(isp_info, 'ip-api', data)
                
                return LocationResult(
                    lat=float(data['lat']),
                    lon=float(data['lon']),
                    accuracy=accuracy,
                    confidence=0.65,
                    provider='ip-api_enhanced',
                    city=data.get('city', ''),
                    state=data.get('regionName', ''),
                    country=data.get('country', ''),
                    country_code=data.get('countryCode', ''),
                    zipcode=data.get('zip', ''),
                    timezone=data.get('timezone', '')
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
        if not self.maxmind_license_key:
            raise LocationServiceError("MaxMind API key not configured")
        
        url = f"https://geoip.maxmind.com/geoip/v2.1/city/{ip_address}"
        headers = {'Authorization': f'Basic {self.maxmind_license_key}'}
        
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
    
    async def _calculate_ultra_consensus(self, results: List[LocationResult], ip_address: str) -> LocationResult:
        if not results:
            raise LocationServiceError("No valid location results")
        
        sorted_results = sorted(results, key=lambda r: (r.confidence * r.accuracy), reverse=True)
        
        filtered_results = self._remove_outliers(sorted_results)
        
        if not filtered_results:
            filtered_results = sorted_results[:3]
        
        total_weight = 0
        weighted_lat = 0
        weighted_lon = 0
        
        for result in filtered_results:
            provider_weight = self.providers_config.get(result.provider.split('_')[0], {}).get('weight', 0.1)
            confidence_weight = result.confidence
            accuracy_weight = result.accuracy
            
            final_weight = provider_weight * confidence_weight * accuracy_weight
            
            weighted_lat += result.lat * final_weight
            weighted_lon += result.lon * final_weight
            total_weight += final_weight
        
        if total_weight == 0:
            consensus_lat = median([r.lat for r in filtered_results])
            consensus_lon = median([r.lon for r in filtered_results])
        else:
            consensus_lat = weighted_lat / total_weight
            consensus_lon = weighted_lon / total_weight
        
        best_result = filtered_results[0]
        
        lat_variance = sum(abs(r.lat - consensus_lat) for r in filtered_results) / len(filtered_results)
        lon_variance = sum(abs(r.lon - consensus_lon) for r in filtered_results) / len(filtered_results)
        
        variance_penalty = min(0.4, (lat_variance + lon_variance) * 20)
        consensus_accuracy = max(0.1, min(0.95, best_result.accuracy - variance_penalty))
        
        agreement_score = 1 - min(0.5, (lat_variance + lon_variance) * 10)
        consensus_confidence = min(0.99, (sum(r.confidence for r in filtered_results) / len(filtered_results)) * agreement_score)
        
        return LocationResult(
            lat=consensus_lat,
            lon=consensus_lon,
            accuracy=consensus_accuracy,
            confidence=consensus_confidence,
            provider=f"ultra_consensus_{len(filtered_results)}",
            city=best_result.city,
            state=best_result.state,
            country=best_result.country,
            country_code=best_result.country_code,
            zipcode=best_result.zipcode,
            timezone=best_result.timezone
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
    
    def _calculate_enhanced_accuracy(self, isp_info: str, provider: str, data: Dict) -> float:
        base_accuracy = 0.5
        
        isp_lower = isp_info.lower()
        if any(term in isp_lower for term in ['fiber', 'ftth', 'fttp']):
            base_accuracy += 0.35
        elif any(term in isp_lower for term in ['cable', 'broadband', 'dsl']):
            base_accuracy += 0.25
        elif any(term in isp_lower for term in ['mobile', 'cellular', '4g', '5g', 'lte']):
            base_accuracy += 0.15
        elif 'wifi' in isp_lower:
            base_accuracy += 0.20
        
        provider_bonus = {
            'maxmind': 0.20,
            'ipgeolocation': 0.15,
            'ipstack': 0.12,
            'ip2location': 0.10,
            'ipinfo': 0.08,
            'ipapi': 0.05
        }.get(provider.split('_')[0], 0)
        
        if data.get('zipcode') or data.get('postal'):
            base_accuracy += 0.05
        if data.get('timezone'):
            base_accuracy += 0.03
        if data.get('accuracy_radius', 0) < 20:
            base_accuracy += 0.05
        
        return min(0.95, base_accuracy + provider_bonus)
    
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
    
    def _remove_outliers(self, results: List[LocationResult]) -> List[LocationResult]:
        if len(results) < 3:
            return results
        
        median_lat = median([r.lat for r in results])
        median_lon = median([r.lon for r in results])
        
        filtered = []
        for result in results:
            distance = self._calculate_distance(result.lat, result.lon, median_lat, median_lon)
            if distance < 50:
                filtered.append(result)
        
        return filtered if filtered else results[:3]
    
    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 6371
        
        return c * r
    
    async def _ultra_enhance_location(self, location: LocationResult) -> LocationResult:
        enhanced = location
        
        if self.google_maps_key:
            try:
                enhanced = await self._enhance_with_google_maps_ultra(location)
            except Exception as e:
                logger.warning(f"Google Maps enhancement failed: {e}")
        
        if enhanced == location:
            try:
                enhanced = await self._enhance_with_nominatim_ultra(location)
            except Exception as e:
                logger.warning(f"Nominatim enhancement failed: {e}")
        
        return enhanced
    
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
    
    async def _enhance_with_google_maps_ultra(self, location: LocationResult) -> LocationResult:
        url = f"https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'latlng': f"{location.lat},{location.lon}",
            'key': self.google_maps_key,
            'result_type': 'street_address|route|neighborhood|locality|administrative_area_level_1|country',
            'language': 'en'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=12) as response:
                data = await response.json()
                
                if data.get('status') != 'OK' or not data.get('results'):
                    return location
                
                result = data['results'][0]
                geometry = result.get('geometry', {})
                components = result.get('address_components', [])
                
                location_type = geometry.get('location_type', 'APPROXIMATE')
                accuracy_bonus = {
                    'ROOFTOP': 0.05,
                    'RANGE_INTERPOLATED': 0.03,
                    'GEOMETRIC_CENTER': 0.02,
                    'APPROXIMATE': 0.01
                }.get(location_type, 0)
                
                enhanced = LocationResult(
                    lat=location.lat,
                    lon=location.lon,
                    accuracy=min(0.99, location.accuracy + accuracy_bonus),
                    confidence=min(0.99, location.confidence + 0.05),
                    provider=f"{location.provider}+google_ultra",
                    formatted_address=result.get('formatted_address', ''),
                    source_type=location.source_type
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
    
    async def _enhance_with_nominatim_ultra(self, location: LocationResult) -> LocationResult:
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            'lat': location.lat,
            'lon': location.lon,
            'format': 'json',
            'addressdetails': 1,
            'zoom': 18,
            'extratags': 1
        }
        headers = {'User-Agent': 'SkyVibeWeatherApp/4.0'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers, timeout=12) as response:
                data = await response.json()
                
                address = data.get('address', {})
                
                enhanced = LocationResult(
                    lat=location.lat,
                    lon=location.lon,
                    accuracy=min(0.95, location.accuracy + 0.03),
                    confidence=location.confidence,
                    provider=f"{location.provider}+nominatim_ultra",
                    road=address.get('road', ''),
                    house_number=address.get('house_number', ''),
                    suburb=address.get('suburb', ''),
                    neighbourhood=address.get('neighbourhood', ''),
                    city=address.get('city') or address.get('town', ''),
                    state=address.get('state', ''),
                    country=address.get('country', ''),
                    country_code=address.get('country_code', '').upper(),
                    zipcode=address.get('postcode', ''),
                    formatted_address=data.get('display_name', ''),
                    source_type=location.source_type
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
    
    async def _validate_and_refine(self, location: LocationResult, ip_address: str) -> LocationResult:
        return location
    
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
            lat = float(browser_location['latitude'])
            lon = float(browser_location['longitude'])
            accuracy = browser_location.get('accuracy', 0)
            
            return (
                -90 <= lat <= 90 and 
                -180 <= lon <= 180 and 
                0 < accuracy < 10000
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

location_service = UltraAccurateLocationService()
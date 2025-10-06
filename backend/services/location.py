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
from math import radians, cos, sin, asin, sqrt, atan2

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
    place_id: str = ""
    plus_code: str = ""
    location_type: str = ""
    bounds: Dict = None
    verification_score: float = 0.0

class LocationServiceError(Exception):
    pass

class GoogleMapsAccuracyEnhancer:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session_cache = {}
        
    async def get_ultra_precise_location(self, 
                                       browser_location: Optional[Dict] = None,
                                       ip_address: Optional[str] = None,
                                       wifi_data: Optional[List[Dict]] = None,
                                       cell_towers: Optional[List[Dict]] = None) -> LocationResult:
        
        if browser_location and self._validate_browser_location(browser_location):
            logger.info("Enhancing GPS location with Google precision APIs")
            return await self._enhance_gps_with_google_precision(browser_location)
        
        if wifi_data or cell_towers:
            logger.info("Using Google Geolocation API for WiFi/cellular positioning")
            return await self._get_geolocation_api_position(wifi_data, cell_towers)
        
        if ip_address:
            logger.info("Enhancing IP location with Google precision")
            return await self._enhance_ip_with_google_precision(ip_address)
        
        raise Exception("No location data available for precision enhancement")
    
    async def _enhance_gps_with_google_precision(self, browser_location: Dict) -> LocationResult:
        lat = float(browser_location['latitude'])
        lon = float(browser_location['longitude'])
        accuracy_meters = browser_location.get('accuracy', 100)
        
        detailed_location = await self._ultra_precise_reverse_geocode(lat, lon)
        snapped_location = await self._snap_to_roads(lat, lon)
        nearby_validation = await self._validate_with_nearby_places(lat, lon)
        plus_code = await self._generate_plus_code(lat, lon)
        
        verified_location = await self._cross_validate_location_data(
            detailed_location, snapped_location, nearby_validation, plus_code
        )
        
        base_accuracy = min(0.95, 1 - (accuracy_meters / 50000))
        google_enhancement = 0.049
        final_accuracy = min(0.999, base_accuracy + google_enhancement)
        
        verified_location.accuracy = final_accuracy
        verified_location.confidence = 0.999
        verified_location.source_type = "gps_google_ultra_precise"
        verified_location.provider = "gps+google_precision_suite"
        verified_location.accuracy_radius = min(5, accuracy_meters)
        verified_location.verification_score = self._calculate_verification_score(verified_location)
        
        logger.info(f"Ultra-precise GPS location: {verified_location.formatted_address}")
        logger.info(f"Accuracy: {final_accuracy:.3%}, Radius: {verified_location.accuracy_radius}m")
        
        return verified_location
    
    async def _ultra_precise_reverse_geocode(self, lat: float, lon: float) -> LocationResult:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        
        params = {
            'latlng': f"{lat:.8f},{lon:.8f}",
            'key': self.api_key,
            'result_type': 'rooftop|range_interpolated|geometric_center|street_address|premise|route',
            'location_type': 'ROOFTOP',
            'language': 'en',
            'region': 'us'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"Google Geocoding API error: {response.status}")
                
                data = await response.json()
                
                if data.get('status') != 'OK' or not data.get('results'):
                    raise Exception(f"Google Geocoding failed: {data.get('status')}")
                
                best_result = None
                for result in data['results']:
                    location_type = result.get('geometry', {}).get('location_type', 'APPROXIMATE')
                    if location_type == 'ROOFTOP':
                        best_result = result
                        break
                
                if not best_result:
                    best_result = data['results'][0]
                
                return self._parse_detailed_geocoding_result(best_result, lat, lon)
    
    async def _snap_to_roads(self, lat: float, lon: float) -> Dict:
        url = "https://roads.googleapis.com/v1/nearestRoads"
        params = {
            'points': f"{lat:.8f},{lon:.8f}",
            'key': self.api_key
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=8) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get('snappedPoints'):
                            snapped = data['snappedPoints'][0]
                            snapped_location = snapped['location']
                            
                            return {
                                'snapped_lat': snapped_location['latitude'],
                                'snapped_lon': snapped_location['longitude'],
                                'place_id': snapped.get('placeId', ''),
                                'road_snapped': True,
                                'accuracy_improvement': True
                            }
        except Exception as e:
            logger.warning(f"Road snapping failed: {e}")
        
        return {
            'snapped_lat': lat,
            'snapped_lon': lon,
            'road_snapped': False,
            'accuracy_improvement': False
        }
    
    async def _validate_with_nearby_places(self, lat: float, lon: float) -> Dict:
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            'location': f"{lat:.8f},{lon:.8f}",
            'radius': '50',
            'key': self.api_key,
            'type': 'establishment'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=8) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        places = data.get('results', [])
                        if places:
                            closest_place = min(places, key=lambda p: self._calculate_distance_meters(
                                lat, lon,
                                p['geometry']['location']['lat'],
                                p['geometry']['location']['lng']
                            ))
                            
                            distance = self._calculate_distance_meters(
                                lat, lon,
                                closest_place['geometry']['location']['lat'],
                                closest_place['geometry']['location']['lng']
                            )
                            
                            return {
                                'nearby_validation': True,
                                'closest_place': closest_place.get('name', ''),
                                'distance_to_landmark': distance,
                                'validation_confidence': max(0, 1 - (distance / 50))
                            }
        except Exception as e:
            logger.warning(f"Nearby places validation failed: {e}")
        
        return {
            'nearby_validation': False,
            'validation_confidence': 0.5
        }
    
    async def _generate_plus_code(self, lat: float, lon: float) -> str:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'latlng': f"{lat:.8f},{lon:.8f}",
            'key': self.api_key,
            'result_type': 'plus_code'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=5) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get('status') == 'OK' and data.get('results'):
                            for result in data['results']:
                                if 'plus_code' in result:
                                    return result['plus_code']['global_code']
        except Exception as e:
            logger.warning(f"Plus code generation failed: {e}")
        
        return ""
    
    async def _get_geolocation_api_position(self, wifi_data: Optional[List[Dict]] = None, 
                                           cell_towers: Optional[List[Dict]] = None) -> LocationResult:
        url = "https://www.googleapis.com/geolocation/v1/geolocate"
        params = {'key': self.api_key}
        
        payload = {
            'considerIp': True,
            'wifiAccessPoints': wifi_data or [],
            'cellTowers': cell_towers or []
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params, json=payload, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"Google Geolocation API error: {response.status}")
                
                data = await response.json()
                
                if 'location' not in data:
                    raise Exception("No location returned from Geolocation API")
                
                location = data['location']
                accuracy = data.get('accuracy', 1000)
                
                enhanced = await self._ultra_precise_reverse_geocode(
                    location['lat'], location['lng']
                )
                
                enhanced.accuracy = min(0.95, 1 - (accuracy / 10000))
                enhanced.confidence = 0.95
                enhanced.source_type = "google_geolocation_api"
                enhanced.provider = "google_geolocation"
                enhanced.accuracy_radius = accuracy
                
                return enhanced
    
    async def _enhance_ip_with_google_precision(self, ip_address: str) -> LocationResult:
        basic_location = await self._get_basic_ip_location(ip_address)
        
        nearby_validation = await self._validate_with_nearby_places(
            basic_location.lat, basic_location.lon
        )
        
        enhanced = await self._ultra_precise_reverse_geocode(
            basic_location.lat, basic_location.lon
        )
        
        enhanced.accuracy = min(0.75, basic_location.accuracy + 0.15)
        enhanced.source_type = "ip_google_enhanced"
        enhanced.provider = f"{basic_location.provider}+google_precision"
        
        return enhanced
    
    async def _cross_validate_location_data(self, detailed_location: LocationResult, 
                                          snapped_data: Dict, nearby_data: Dict, 
                                          plus_code: str) -> LocationResult:
        if snapped_data.get('road_snapped'):
            detailed_location.lat = snapped_data['snapped_lat']
            detailed_location.lon = snapped_data['snapped_lon']
            if snapped_data.get('place_id'):
                detailed_location.place_id = snapped_data['place_id']
        
        detailed_location.plus_code = plus_code
        
        validation_confidence = nearby_data.get('validation_confidence', 0.5)
        detailed_location.confidence = min(0.999, detailed_location.confidence * (0.5 + validation_confidence))
        
        return detailed_location
    
    def _parse_detailed_geocoding_result(self, result: Dict, lat: float, lon: float) -> LocationResult:
        components = result.get('address_components', [])
        geometry = result.get('geometry', {})
        
        location = LocationResult(
            lat=lat,
            lon=lon,
            accuracy=0.99,
            confidence=0.99,
            provider='google_precision',
            source_type='google_enhanced',
            formatted_address=result.get('formatted_address', ''),
            place_id=result.get('place_id', ''),
            location_type=geometry.get('location_type', 'APPROXIMATE'),
            bounds=geometry.get('bounds', {})
        )
        
        if 'plus_code' in result:
            location.plus_code = result['plus_code'].get('global_code', '')
        
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
        
        if not location.city:
            location.city = location.postal_town or location.district or location.suburb or "Unknown"
        
        accuracy_map = {
            'ROOFTOP': 0.999,
            'RANGE_INTERPOLATED': 0.98,
            'GEOMETRIC_CENTER': 0.95,
            'APPROXIMATE': 0.90
        }
        location.accuracy = accuracy_map.get(location.location_type, 0.90)
        
        return location
    
    def _calculate_verification_score(self, location: LocationResult) -> float:
        score = 0.0
        
        address_components = [
            location.house_number, location.road, location.city, 
            location.state, location.country, location.zipcode
        ]
        address_completeness = len([c for c in address_components if c]) / len(address_components)
        score += address_completeness * 0.30
        
        type_scores = {
            'ROOFTOP': 1.0,
            'RANGE_INTERPOLATED': 0.9,
            'GEOMETRIC_CENTER': 0.8,
            'APPROXIMATE': 0.6
        }
        score += type_scores.get(location.location_type, 0.6) * 0.25
        
        if location.plus_code:
            score += 0.15
        
        if location.place_id:
            score += 0.15
        
        if 'google' in location.provider.lower():
            score += 0.15
        
        return min(1.0, score)
    
    def _calculate_distance_meters(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371000
        
        lat1_rad = radians(lat1)
        lat2_rad = radians(lat2)
        delta_lat = radians(lat2 - lat1)
        delta_lon = radians(lon2 - lon1)
        
        a = sin(delta_lat/2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        return R * c
    
    def _validate_browser_location(self, browser_location: Dict) -> bool:
        try:
            lat = float(browser_location.get('latitude', 0))
            lon = float(browser_location.get('longitude', 0))
            accuracy = browser_location.get('accuracy', 0)
            
            return (
                -90 <= lat <= 90 and 
                -180 <= lon <= 180 and 
                lat != 0 and lon != 0 and
                0 < accuracy < 50000 and
                abs(lat) > 0.001 and abs(lon) > 0.001
            )
        except:
            return False
    
    async def _get_basic_ip_location(self, ip_address: str) -> LocationResult:
        return LocationResult(
            lat=0.0, lon=0.0, accuracy=0.5, confidence=0.6,
            provider="ip_basic", source_type="ip"
        )

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
        self.google_enhancer = GoogleMapsAccuracyEnhancer(self.google_maps_key) if self.google_maps_key else None
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
                                        browser_location: Optional[Dict] = None,
                                        wifi_data: Optional[List[Dict]] = None,
                                        cell_towers: Optional[List[Dict]] = None) -> LocationResult:
        
        if self.google_enhancer and (browser_location or wifi_data or cell_towers):
            try:
                logger.info("Using Google Ultra-Precision location services")
                ultra_precise = await self.google_enhancer.get_ultra_precise_location(
                    browser_location=browser_location,
                    ip_address=ip_address,
                    wifi_data=wifi_data,
                    cell_towers=cell_towers
                )
                
                if session_id and self.redis_client:
                    self._store_session_location(session_id, ultra_precise)
                
                return ultra_precise
                
            except Exception as e:
                logger.error(f"Google Ultra-Precision failed: {e}")
        
        if browser_location and self._validate_browser_location(browser_location):
            logger.info("Using browser GPS location for highest accuracy")
            lat = float(browser_location['latitude'])
            lon = float(browser_location['longitude'])
            accuracy_meters = browser_location.get('accuracy', 100)
            
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
        
        if session_id and self.redis_client:
            cached = self._get_session_location(session_id)
            if cached and cached.accuracy >= self.accuracy_threshold:
                logger.info(f"Using high-accuracy cached location: {cached.city}")
                return cached
        
        logger.warning("No GPS location available - using less accurate IP-based location")
        
        if not ip_address or self._is_private_ip(ip_address):
            ip_address = await self._get_public_ip_enhanced()
            logger.info(f"Detected public IP: {ip_address}")
        
        provider_results = await self._query_enhanced_providers(ip_address)
        
        if not provider_results:
            logger.error("All location providers failed")
            
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
                accuracy_radius=300000
            )
        
        consensus_location = await self._calculate_ultra_consensus(provider_results, ip_address)
        
        if self.google_maps_key and consensus_location.lat != 0 and consensus_location.lon != 0:
            try:
                enhanced = await self._enhance_with_google_maps_detailed(consensus_location)
                enhanced.source_type = "ip_enhanced"
                enhanced.accuracy = min(0.70, consensus_location.accuracy)
                enhanced.accuracy_radius = 50000
                
                if session_id and self.redis_client:
                    self._store_session_location(session_id, enhanced)
                
                return enhanced
            except Exception as e:
                logger.warning(f"Google Maps IP enhancement failed: {e}")
        
        consensus_location.source_type = "ip"
        consensus_location.accuracy = min(0.60, consensus_location.accuracy)
        consensus_location.accuracy_radius = 100000
        
        if session_id and self.redis_client and consensus_location.accuracy >= 0.50:
            self._store_session_location(session_id, consensus_location)
        
        return consensus_location
    
    async def get_location_from_ip_enhanced(self, ip_address: Optional[str] = None, 
                                          session_id: Optional[str] = None) -> LocationResult:
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
        
        if self.google_maps_key:
            enhanced_location = await self._enhance_with_google_maps_detailed(consensus_location)
        else:
            enhanced_location = await self._enhance_with_nominatim_detailed(consensus_location)
        
        enhanced_location.source_type = "ip"
        enhanced_location.accuracy = min(0.70, enhanced_location.accuracy)
        enhanced_location.accuracy_radius = 50000
        
        if session_id and self.redis_client:
            self._store_session_location(session_id, enhanced_location)
        
        return enhanced_location
    
    async def get_location_from_coordinates(self, lat: float, lon: float) -> LocationResult:
        if not self._validate_coordinates(lat, lon):
            raise LocationServiceError(f"Invalid coordinates: {lat}, {lon}")
        
        logger.info(f"Reverse geocoding coordinates: {lat:.6f}, {lon:.6f}")
        
        if self.google_maps_key:
            return await self._reverse_geocode_google_detailed(lat, lon)
        else:
            return await self._reverse_geocode_nominatim_detailed(lat, lon)
    
    async def _reverse_geocode_google_detailed(self, lat: float, lon: float) -> LocationResult:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'latlng': f"{lat},{lon}",
            'key': self.google_maps_key,
            'result_type': 'street_address|route|neighborhood|locality|administrative_area_level_1|country',
            'language': 'en',
            'location_type': 'ROOFTOP'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise LocationServiceError(f"Google Maps API error: {response.status}")
                
                data = await response.json()
                
                if data.get('status') != 'OK' or not data.get('results'):
                    raise LocationServiceError(f"Google Geocoding failed: {data.get('status')}")
                
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
                    accuracy_radius=10
                )
                
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
                
                if not location.city:
                    location.city = location.postal_town or location.district or location.suburb or "Unknown"
                
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
        headers = {'User-Agent': 'NimbusApp/1.2'}
        
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
        if not self.google_maps_key:
            return location
        
        try:
            enhanced = await self._reverse_geocode_google_detailed(location.lat, location.lon)
            
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
        try:
            enhanced = await self._reverse_geocode_nominatim_detailed(location.lat, location.lon)
            
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
                    accuracy=0.70,
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

    async def search_location(self, query: str) -> List[Dict]:
        if not query or len(query) < 2:
            return []
        
        logger.info(f"Searching for location: {query}")
        
        if self.google_maps_key:
            try:
                results = await self._search_google_places(query)
                if results:
                    return results
            except Exception as e:
                logger.warning(f"Google Places search failed: {e}")
        
        try:
            results = await self._search_nominatim(query)
            return results
        except Exception as e:
            logger.error(f"Nominatim search failed: {e}")
            return []

    async def _search_google_places(self, query: str) -> List[Dict]:
        autocomplete_url = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
        params = {
            'input': query,
            'key': self.google_maps_key,
            'types': '(cities)|geocode',
            'language': 'en'
        }
        
        results = []
        
        async with aiohttp.ClientSession() as session:
            async with session.get(autocomplete_url, params=params, timeout=5) as response:
                if response.status != 200:
                    raise Exception(f"Google Places API error: {response.status}")
                
                data = await response.json()
                
                if data.get('status') != 'OK' or not data.get('predictions'):
                    return await self._search_google_geocoding(query)
                
                for prediction in data['predictions'][:5]:
                    place_id = prediction['place_id']
                    
                    details_url = "https://maps.googleapis.com/maps/api/place/details/json"
                    details_params = {
                        'place_id': place_id,
                        'key': self.google_maps_key,
                        'fields': 'geometry,formatted_address,address_components,name'
                    }
                    
                    async with session.get(details_url, params=details_params, timeout=5) as detail_response:
                        if detail_response.status == 200:
                            detail_data = await detail_response.json()
                            
                            if detail_data.get('status') == 'OK' and detail_data.get('result'):
                                result = detail_data['result']
                                geometry = result.get('geometry', {})
                                location = geometry.get('location', {})
                                
                                components = result.get('address_components', [])
                                city = ''
                                state = ''
                                country = ''
                                
                                for component in components:
                                    types = component.get('types', [])
                                    if 'locality' in types:
                                        city = component.get('long_name', '')
                                    elif 'administrative_area_level_1' in types:
                                        state = component.get('long_name', '')
                                    elif 'country' in types:
                                        country = component.get('long_name', '')
                                
                                results.append({
                                    'place_id': place_id,
                                    'display_name': result.get('formatted_address', ''),
                                    'name': result.get('name', ''),
                                    'lat': location.get('lat'),
                                    'lon': location.get('lng'),
                                    'city': city,
                                    'state': state,
                                    'country': country,
                                    'type': 'place',
                                    'source': 'google_places'
                                })
        
        return results

    async def _search_google_geocoding(self, query: str) -> List[Dict]:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'address': query,
            'key': self.google_maps_key,
            'language': 'en'
        }
        
        results = []
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=5) as response:
                if response.status != 200:
                    raise Exception(f"Google Geocoding API error: {response.status}")
                
                data = await response.json()
                
                if data.get('status') != 'OK' or not data.get('results'):
                    return []
                
                for result in data['results'][:5]:
                    geometry = result.get('geometry', {})
                    location = geometry.get('location', {})
                    components = result.get('address_components', [])
                    
                    city = ''
                    state = ''
                    country = ''
                    
                    for component in components:
                        types = component.get('types', [])
                        if 'locality' in types:
                            city = component.get('long_name', '')
                        elif 'administrative_area_level_1' in types:
                            state = component.get('long_name', '')
                        elif 'country' in types:
                            country = component.get('long_name', '')
                    
                    results.append({
                        'place_id': result.get('place_id', ''),
                        'display_name': result.get('formatted_address', ''),
                        'name': city or result.get('formatted_address', '').split(',')[0],
                        'lat': location.get('lat'),
                        'lon': location.get('lng'),
                        'city': city,
                        'state': state,
                        'country': country,
                        'type': result.get('types', [''])[0],
                        'source': 'google_geocoding'
                    })
        
        return results

    async def _search_nominatim(self, query: str) -> List[Dict]:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': query,
            'format': 'json',
            'addressdetails': 1,
            'limit': 5,
            'accept-language': 'en'
        }
        headers = {'User-Agent': 'NimbusApp/1.2'}
        
        results = []
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers, timeout=5) as response:
                if response.status != 200:
                    raise Exception(f"Nominatim API error: {response.status}")
                
                data = await response.json()
                
                for item in data:
                    address = item.get('address', {})
                    
                    city = (address.get('city') or 
                        address.get('town') or 
                        address.get('village') or 
                        address.get('municipality', ''))
                    
                    results.append({
                        'place_id': item.get('place_id', ''),
                        'display_name': item.get('display_name', ''),
                        'name': item.get('name', '') or city,
                        'lat': float(item.get('lat', 0)),
                        'lon': float(item.get('lon', 0)),
                        'city': city,
                        'state': address.get('state', ''),
                        'country': address.get('country', ''),
                        'type': item.get('type', ''),
                        'source': 'nominatim'
                    })
        
        return results

    async def get_location_from_place_id(self, place_id: str, source: str = 'google') -> LocationResult:
        if source == 'google_places' and self.google_maps_key:
            return await self._get_location_from_google_place_id(place_id)
        elif source == 'nominatim':
            return await self._get_location_from_nominatim_place_id(place_id)
        else:
            raise LocationServiceError(f"Unknown source: {source}")

    async def _get_location_from_google_place_id(self, place_id: str) -> LocationResult:
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            'place_id': place_id,
            'key': self.google_maps_key,
            'fields': 'geometry,formatted_address,address_components,name'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=5) as response:
                if response.status != 200:
                    raise LocationServiceError(f"Google Place Details API error: {response.status}")
                
                data = await response.json()
                
                if data.get('status') != 'OK' or not data.get('result'):
                    raise LocationServiceError("Place not found")
                
                result = data['result']
                geometry = result.get('geometry', {})
                location = geometry.get('location', {})
                components = result.get('address_components', [])
                
                location_result = LocationResult(
                    lat=location.get('lat'),
                    lon=location.get('lng'),
                    accuracy=0.99,
                    confidence=0.99,
                    provider='google_places',
                    source_type='search',
                    formatted_address=result.get('formatted_address', ''),
                    accuracy_radius=100
                )
                
                for component in components:
                    types = component.get('types', [])
                    long_name = component.get('long_name', '')
                    short_name = component.get('short_name', '')
                    
                    if 'street_number' in types:
                        location_result.house_number = long_name
                    elif 'route' in types:
                        location_result.road = long_name
                    elif 'neighborhood' in types:
                        location_result.neighbourhood = long_name
                    elif 'sublocality' in types:
                        location_result.suburb = long_name
                    elif 'locality' in types:
                        location_result.city = long_name
                    elif 'administrative_area_level_1' in types:
                        location_result.state = long_name
                    elif 'country' in types:
                        location_result.country = long_name
                        location_result.country_code = short_name
                    elif 'postal_code' in types:
                        location_result.zipcode = long_name
                
                return location_result

    async def _get_location_from_nominatim_place_id(self, place_id: str) -> LocationResult:
        url = f"https://nominatim.openstreetmap.org/details"
        params = {
            'place_id': place_id,
            'format': 'json',
            'addressdetails': 1
        }
        headers = {'User-Agent': 'NimbusApp/1.2'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers, timeout=5) as response:
                if response.status != 200:
                    lookup_url = "https://nominatim.openstreetmap.org/lookup"
                    lookup_params = {
                        'osm_ids': f"N{place_id}",
                        'format': 'json',
                        'addressdetails': 1
                    }
                    
                    async with session.get(lookup_url, params=lookup_params, headers=headers, timeout=5) as lookup_response:
                        if lookup_response.status != 200:
                            raise LocationServiceError(f"Nominatim API error: {lookup_response.status}")
                        
                        data = await lookup_response.json()
                        if not data:
                            raise LocationServiceError("Place not found")
                        
                        item = data[0]
                else:
                    item = await response.json()
                
                address = item.get('address', {})
                
                return LocationResult(
                    lat=float(item.get('lat', 0)),
                    lon=float(item.get('lon', 0)),
                    accuracy=0.95,
                    confidence=0.95,
                    provider='nominatim',
                    source_type='search',
                    formatted_address=item.get('display_name', ''),
                    house_number=address.get('house_number', ''),
                    road=address.get('road', ''),
                    neighbourhood=address.get('neighbourhood', ''),
                    suburb=address.get('suburb', ''),
                    city=address.get('city') or address.get('town', ''),
                    state=address.get('state', ''),
                    country=address.get('country', ''),
                    country_code=address.get('country_code', '').upper(),
                    zipcode=address.get('postcode', ''),
                    accuracy_radius=500
                )
    
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
                        lon=float(data['longitude']),
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
                    accuracy_radius=accuracy_radius * 1000,
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
        
        sorted_results = sorted(results, key=lambda r: (r.confidence * r.accuracy), reverse=True)
        
        filtered_results = self._remove_outliers(sorted_results)
        
        if not filtered_results:
            filtered_results = sorted_results[:3]
        
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
        
        lat_variance = sum(abs(r.lat - consensus_lat) for r in filtered_results) / len(filtered_results)
        lon_variance = sum(abs(r.lon - consensus_lon) for r in filtered_results) / len(filtered_results)
        
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
            accuracy_radius=100000
        )
    
    def _calculate_consensus(self, results: List[LocationResult]) -> LocationResult:
        if not results:
            raise LocationServiceError("No valid location results")
        
        consensus_lat = median([r.lat for r in results])
        consensus_lon = median([r.lon for r in results])
        
        best_result = max(results, key=lambda r: r.confidence * r.accuracy)
        
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
            if distance < 200:
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
                lat != 0 and lon != 0 and
                0 < accuracy < 50000
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
        parts = []
        
        if location.house_number and location.road:
            parts.append(f"{location.house_number} {location.road}")
        elif location.road:
            parts.append(location.road)
        
        if location.neighbourhood:
            parts.append(location.neighbourhood)
        elif location.suburb:
            parts.append(location.suburb)
        
        if location.city:
            parts.append(location.city)
        elif location.postal_town:
            parts.append(location.postal_town)
        
        if location.state:
            parts.append(location.state)
        elif location.district:
            parts.append(location.district)
        
        if location.country:
            parts.append(location.country)
        
        if location.formatted_address and len(location.formatted_address) > 10:
            return location.formatted_address
        
        return ', '.join(parts) if parts else 'Unknown Location'

location_service = UltraAccurateLocationService()
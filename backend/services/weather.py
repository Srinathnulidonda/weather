#backend/services/weather.py
import os
import requests
import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json

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

@dataclass
class HealthInsights:
    heat_index: float
    wind_chill: float
    pollen_level: str
    air_quality_advice: str
    uv_advice: str
    general_health_tips: List[str]

class WeatherService:
    def __init__(self):
        self.tomorrow_api_key = os.getenv('TOMORROW_API_KEY')
        self.visual_crossing_api_key = os.getenv('VISUAL_CROSSING_API_KEY')
        self.openweather_api_key = os.getenv('OPENWEATHER_API_KEY')
        
        self.time_periods = {
            'morning': (6, 12),
            'afternoon': (12, 18),
            'evening': (18, 22),
            'night': (22, 6)
        }
        
        self.weather_recommendations = {
            'Clear': {
                'morning': {
                    'activities': ['Jogging', 'Hiking', 'Outdoor Yoga', 'Cycling', 'Photography'],
                    'clothing': ['Light athletic wear', 'Sunglasses', 'Sunscreen', 'Cap'],
                    'health_tips': ['Start hydrating early', 'Apply SPF 30+', 'Perfect for vitamin D'],
                    'mood': 'energetic',
                    'color_palette': ['#FFD700', '#FFA500', '#87CEEB', '#00BFFF']
                },
                'afternoon': {
                    'activities': ['Beach visit', 'Picnic', 'Outdoor sports', 'Swimming'],
                    'clothing': ['Lightweight clothing', 'Sun hat', 'Sunscreen', 'Sunglasses'],
                    'health_tips': ['Seek shade during peak hours', 'Drink water frequently', 'Wear protective clothing'],
                    'mood': 'vibrant',
                    'color_palette': ['#FF6B35', '#F7931E', '#FFD23F', '#06FFA5']
                },
                'evening': {
                    'activities': ['Outdoor dining', 'Sunset watching', 'Walking', 'Barbecue'],
                    'clothing': ['Light layers', 'Comfortable shoes', 'Light jacket'],
                    'health_tips': ['Perfect time for outdoor activities', 'Stay hydrated'],
                    'mood': 'relaxed',
                    'color_palette': ['#FF8C42', '#FF6B35', '#C73E1D', '#592E34']
                },
                'night': {
                    'activities': ['Stargazing', 'Night photography', 'Outdoor socializing'],
                    'clothing': ['Light jacket', 'Long pants', 'Comfortable shoes'],
                    'health_tips': ['Enjoy the cool air', 'Perfect for relaxation'],
                    'mood': 'peaceful',
                    'color_palette': ['#2C3E50', '#34495E', '#5D6D7E', '#85929E']
                }
            },
            'Clouds': {
                'morning': {
                    'activities': ['Walking', 'Outdoor workout', 'Running', 'City exploration'],
                    'clothing': ['Light layers', 'Comfortable shoes', 'Light jacket'],
                    'health_tips': ['Perfect temperature for exercise', 'No harsh sun exposure'],
                    'mood': 'comfortable',
                    'color_palette': ['#BDC3C7', '#95A5A6', '#7F8C8D', '#AEB6BF']
                },
                'afternoon': {
                    'activities': ['Shopping', 'Museum visits', 'Café hopping', 'Photography'],
                    'clothing': ['Casual layers', 'Comfortable walking shoes'],
                    'health_tips': ['Ideal conditions for all activities', 'No weather concerns'],
                    'mood': 'neutral',
                    'color_palette': ['#D5DBDB', '#AEB6BF', '#85929E', '#566573']
                },
                'evening': {
                    'activities': ['Outdoor dining', 'Walking', 'Social gatherings'],
                    'clothing': ['Light sweater', 'Comfortable attire'],
                    'health_tips': ['Comfortable weather continues', 'Perfect for socializing'],
                    'mood': 'social',
                    'color_palette': ['#909497', '#717D7E', '#515A5A', '#2C3E50']
                },
                'night': {
                    'activities': ['Movie night outdoors', 'Casual walks', 'Reading outside'],
                    'clothing': ['Warm layers', 'Jacket', 'Closed shoes'],
                    'health_tips': ['Mild and pleasant', 'Layer for comfort'],
                    'mood': 'cozy',
                    'color_palette': ['#5D6D7E', '#566573', '#515A5A', '#424949']
                }
            },
            'Rain': {
                'morning': {
                    'activities': ['Indoor workout', 'Yoga', 'Reading', 'Cooking'],
                    'clothing': ['Waterproof jacket', 'Umbrella', 'Waterproof shoes'],
                    'health_tips': ['Stay dry and warm', 'Perfect for indoor activities'],
                    'mood': 'contemplative',
                    'color_palette': ['#5DADE2', '#3498DB', '#2980B9', '#1B4F72']
                },
                'afternoon': {
                    'activities': ['Indoor café', 'Shopping mall', 'Museum', 'Movies'],
                    'clothing': ['Rain gear', 'Layers', 'Waterproof accessories'],
                    'health_tips': ['Avoid getting soaked', 'Warm beverages recommended'],
                    'mood': 'introspective',
                    'color_palette': ['#7FB3D3', '#5DADE2', '#3498DB', '#2980B9']
                },
                'evening': {
                    'activities': ['Home cooking', 'Board games', 'Reading', 'Streaming'],
                    'clothing': ['Cozy indoor wear', 'Warm layers'],
                    'health_tips': ['Perfect for relaxation', 'Stay warm and dry'],
                    'mood': 'homey',
                    'color_palette': ['#AED6F1', '#85C1E9', '#5DADE2', '#3498DB']
                },
                'night': {
                    'activities': ['Reading', 'Relaxing bath', 'Meditation', 'Sleep'],
                    'clothing': ['Comfortable pajamas', 'Warm sleepwear'],
                    'health_tips': ['Perfect sleeping weather', 'Rain sounds aid relaxation'],
                    'mood': 'restful',
                    'color_palette': ['#D6EAF8', '#AED6F1', '#85C1E9', '#5DADE2']
                }
            },
            'Snow': {
                'morning': {
                    'activities': ['Snow activities', 'Winter photography', 'Hot chocolate'],
                    'clothing': ['Heavy winter coat', 'Gloves', 'Warm hat', 'Snow boots'],
                    'health_tips': ['Layer properly', 'Protect extremities', 'Stay warm'],
                    'mood': 'magical',
                    'color_palette': ['#FDFEFE', '#F8F9F9', '#EBF5FB', '#D6EAF8']
                },
                'afternoon': {
                    'activities': ['Snowman building', 'Sledding', 'Winter sports', 'Photography'],
                    'clothing': ['Insulated layers', 'Waterproof outer layer', 'Winter accessories'],
                    'health_tips': ['Stay active to keep warm', 'Hydrate despite cold'],
                    'mood': 'playful',
                    'color_palette': ['#FFFFFF', '#F7F9F9', '#EAF2F8', '#D4E6F1']
                },
                'evening': {
                    'activities': ['Indoor warmth', 'Hot beverages', 'Fireplace time'],
                    'clothing': ['Heavy coat if going out', 'Warm indoor layers'],
                    'health_tips': ['Warm up gradually when coming inside', 'Hot liquids help'],
                    'mood': 'cozy',
                    'color_palette': ['#EBF5FB', '#D6EAF8', '#AED6F1', '#85C1E9']
                },
                'night': {
                    'activities': ['Indoor activities', 'Warm baths', 'Reading by fire'],
                    'clothing': ['Warm pajamas', 'Extra blankets', 'Warm slippers'],
                    'health_tips': ['Keep bedroom warm', 'Layer bedding for comfort'],
                    'mood': 'snug',
                    'color_palette': ['#F4F6F6', '#E8F6F3', '#D1F2EB', '#A3E4D7']
                }
            },
            'Thunderstorm': {
                'morning': {
                    'activities': ['Stay indoors', 'Indoor exercise', 'Reading', 'Meditation'],
                    'clothing': ['Stay inside', 'Comfortable indoor wear'],
                    'health_tips': ['Avoid outdoor activities', 'Stay away from windows'],
                    'mood': 'dramatic',
                    'color_palette': ['#566573', '#515A5A', '#424949', '#2C3E50']
                },
                'afternoon': {
                    'activities': ['Indoor hobbies', 'Cooking', 'Organizing', 'Learning'],
                    'clothing': ['Remain indoors', 'Comfortable clothing'],
                    'health_tips': ['Wait for storm to pass', 'Avoid electrical devices'],
                    'mood': 'intense',
                    'color_palette': ['#7B7D7D', '#73C6B6', '#48C9B0', '#17A2B8']
                },
                'evening': {
                    'activities': ['Indoor dining', 'Games', 'Movies', 'Relaxation'],
                    'clothing': ['Cozy indoor attire'],
                    'health_tips': ['Safe indoor activities only', 'Monitor weather updates'],
                    'mood': 'sheltered',
                    'color_palette': ['#85929E', '#7B7D7D', '#6C7B7F', '#5D6D7E']
                },
                'night': {
                    'activities': ['Sleep', 'Quiet indoor activities', 'Rest'],
                    'clothing': ['Comfortable sleepwear'],
                    'health_tips': ['Storm sounds can aid sleep', 'Stay indoors'],
                    'mood': 'protective',
                    'color_palette': ['#ABB2B9', '#A6ACAF', '#A2A9AF', '#9EA6AD']
                }
            }
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
        
        if self.tomorrow_api_key:
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
    
    async def _get_tomorrow_current(self, lat: float, lon: float) -> WeatherData:
        url = f"https://api.tomorrow.io/v4/weather/realtime"
        params = {
            'location': f"{lat},{lon}",
            'apikey': self.tomorrow_api_key,
            'fields': 'temperature,temperatureApparent,humidity,pressureSeaLevel,windSpeed,windDirection,visibility,uvIndex,cloudCover,weatherCode'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                data = await response.json()
                
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
                    timestamp=datetime.fromisoformat(data['data']['time'].replace('Z', '+00:00'))
                )
    
    async def _get_tomorrow_air_quality(self, lat: float, lon: float) -> AirQualityData:
        url = f"https://api.tomorrow.io/v4/weather/realtime"
        params = {
            'location': f"{lat},{lon}",
            'apikey': self.tomorrow_api_key,
            'fields': 'epaIndex,particulateMatter25,particulateMatter10,ozoneLevel,nitrogenDioxideLevel,carbonMonoxideLevel,sulphurDioxideLevel'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                data = await response.json()
                
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
        url = f"https://api.tomorrow.io/v4/weather/realtime"
        params = {
            'location': f"{lat},{lon}",
            'apikey': self.tomorrow_api_key,
            'fields': 'temperature,temperatureApparent,humidity,uvIndex,windSpeed,treeIndex,grassIndex,weedIndex'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                data = await response.json()
                
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
                    general_health_tips=health_tips
                )
    
    async def _get_visual_crossing_current(self, lat: float, lon: float) -> WeatherData:
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat},{lon}"
        params = {
            'key': self.visual_crossing_api_key,
            'include': 'current',
            'elements': 'temp,feelslike,humidity,pressure,windspeed,winddir,visibility,uvindex,cloudcover,conditions,icon'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                data = await response.json()
                
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
                    timestamp=datetime.now()
                )
    
    async def _get_openweather_current(self, lat: float, lon: float) -> WeatherData:
        url = f"https://api.openweathermap.org/data/2.5/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.openweather_api_key,
            'units': 'metric'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
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
    
    async def _get_openweather_air_quality(self, lat: float, lon: float) -> AirQualityData:
        url = f"http://api.openweathermap.org/data/2.5/air_pollution"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.openweather_api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                data = await response.json()
                
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
    
    async def _get_tomorrow_forecast(self, lat: float, lon: float, days: int) -> List[Dict]:
        url = f"https://api.tomorrow.io/v4/weather/forecast"
        params = {
            'location': f"{lat},{lon}",
            'apikey': self.tomorrow_api_key,
            'timesteps': '1d',
            'fields': 'temperatureMin,temperatureMax,temperatureAvg,humidity,pressureSeaLevel,windSpeed,uvIndex,weatherCode'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=15) as response:
                data = await response.json()
                
                forecast = []
                for item in data['timelines']['daily'][:days]:
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
                            'avg': round(values.get('temperatureAvg', 0), 1)
                        },
                        'condition': condition,
                        'description': description,
                        'icon': icon,
                        'humidity': values.get('humidity', 0),
                        'pressure': values.get('pressureSeaLevel', 0),
                        'wind_speed': values.get('windSpeed', 0),
                        'uv_index': values.get('uvIndex', 0)
                    })
                
                return forecast
    
    async def _get_visual_crossing_forecast(self, lat: float, lon: float, days: int) -> List[Dict]:
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat},{lon}"
        params = {
            'key': self.visual_crossing_api_key,
            'elements': 'tempmin,tempmax,temp,humidity,pressure,windspeed,uvindex,conditions,icon'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=15) as response:
                data = await response.json()
                
                forecast = []
                for item in data['days'][:days]:
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
                        'uv_index': item.get('uvindex', 0)
                    })
                
                return forecast
    
    async def _get_openweather_forecast(self, lat: float, lon: float, days: int) -> List[Dict]:
        url = f"https://api.openweathermap.org/data/2.5/forecast"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.openweather_api_key,
            'units': 'metric'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                data = await response.json()
                
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
    
    def _get_time_period(self, hour: int) -> str:
        if 6 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 18:
            return 'afternoon'
        elif 18 <= hour < 22:
            return 'evening'
        else:
            return 'night'
    
    def _get_time_based_recommendations(self, condition: str, time_period: str, temperature: float) -> Dict:
        base_recommendations = self.weather_recommendations.get(condition, {}).get(time_period, {})
        
        if not base_recommendations:
            base_recommendations = self.weather_recommendations['Clear'][time_period]
        
        recommendations = base_recommendations.copy()
        
        if temperature > 30:
            recommendations['activities'].extend(['Swimming', 'Water activities', 'Seek air conditioning'])
            recommendations['clothing'].extend(['Lightweight fabrics', 'Cooling accessories'])
            recommendations['health_tips'].extend(['Stay very hydrated', 'Avoid prolonged sun exposure'])
        elif temperature < 0:
            recommendations['activities'] = ['Indoor activities', 'Hot beverages', 'Warm shelter']
            recommendations['clothing'].extend(['Heavy winter gear', 'Multiple layers', 'Thermal wear'])
            recommendations['health_tips'].extend(['Protect from frostbite', 'Limit outdoor exposure'])
        
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
    
    def _get_air_quality_advice(self, aqi: int) -> str:
        if aqi <= 2:
            return "Air quality is good - perfect for outdoor activities"
        elif aqi <= 3:
            return "Air quality is moderate - sensitive groups should limit prolonged exposure"
        elif aqi <= 4:
            return "Air quality is poor - limit outdoor activities"
        else:
            return "Air quality is very poor - avoid outdoor activities"
    
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

weather_service = WeatherService()
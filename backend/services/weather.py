# backend/services/weather.py
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
    precipitation: float = 0.0
    precipitation_probability: int = 0
    dew_point: float = 0.0

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

class UltraWeatherService:
    def __init__(self):
        self.accuweather_api_key = os.getenv('ACCUWEATHER_API_KEY')
        self.tomorrow_api_key = os.getenv('TOMORROW_API_KEY')
        self.visual_crossing_api_key = os.getenv('VISUAL_CROSSING_API_KEY')
        self.openweather_api_key = os.getenv('OPENWEATHER_API_KEY')
        
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
        
        self.weather_recommendations = {
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
                    'activities': ['Stargazing', 'Peaceful walks', 'Reading'],
                    'clothing': ['Light jacket', 'Comfortable wear'],
                    'health_tips': ['Use proper lighting', 'Dress for temperature drop']
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
                    'activities': ['Indoor exercise', 'Café visits', 'Reading'],
                    'clothing': ['Waterproof jacket', 'Rain boots', 'Umbrella'],
                    'health_tips': ['Stay dry', 'Watch for slippery surfaces']
                },
                'afternoon': {
                    'activities': ['Indoor shopping', 'Museums', 'Movies'],
                    'clothing': ['Rain gear', 'Waterproof clothing'],
                    'health_tips': ['Drive carefully', 'Stay warm']
                },
                'evening': {
                    'activities': ['Home activities', 'Reading', 'Movies'],
                    'clothing': ['Cozy indoor wear', 'Warm layers'],
                    'health_tips': ['Stay indoors for comfort']
                },
                'night': {
                    'activities': ['Sleeping', 'Reading', 'Relaxation'],
                    'clothing': ['Cozy pajamas', 'Warm layers'],
                    'health_tips': ['Perfect sleeping weather']
                }
            },
            'Drizzle': {
                'morning': {
                    'activities': ['Light indoor activities', 'Coffee shops'],
                    'clothing': ['Light rain jacket', 'Comfortable shoes'],
                    'health_tips': ['Light protection needed']
                },
                'afternoon': {
                    'activities': ['Indoor venues', 'Shopping', 'Museums'],
                    'clothing': ['Light layers', 'Water-resistant clothing'],
                    'health_tips': ['Stay comfortable']
                },
                'evening': {
                    'activities': ['Indoor dining', 'Home activities'],
                    'clothing': ['Comfortable indoor wear'],
                    'health_tips': ['Cozy evening indoors']
                },
                'night': {
                    'activities': ['Rest', 'Sleep', 'Relaxation'],
                    'clothing': ['Comfortable sleepwear'],
                    'health_tips': ['Good for sleep']
                }
            },
            'Snow': {
                'morning': {
                    'activities': ['Snow sports', 'Indoor activities'],
                    'clothing': ['Heavy winter gear', 'Waterproof boots'],
                    'health_tips': ['Layer properly', 'Stay warm']
                },
                'afternoon': {
                    'activities': ['Indoor activities', 'Hot beverages'],
                    'clothing': ['Winter coat', 'Warm layers'],
                    'health_tips': ['Limit exposure', 'Stay warm']
                },
                'evening': {
                    'activities': ['Indoor activities', 'Cozy home time'],
                    'clothing': ['Warm indoor clothing'],
                    'health_tips': ['Stay indoors']
                },
                'night': {
                    'activities': ['Sleep', 'Rest'],
                    'clothing': ['Warm pajamas', 'Extra blankets'],
                    'health_tips': ['Keep warm']
                }
            },
            'Thunderstorm': {
                'morning': {
                    'activities': ['Stay indoors', 'Postpone outdoor plans'],
                    'clothing': ['Indoor comfortable wear'],
                    'health_tips': ['Avoid outdoor activities', 'Stay safe']
                },
                'afternoon': {
                    'activities': ['Indoor only', 'Safe shelter'],
                    'clothing': ['Comfortable indoor clothing'],
                    'health_tips': ['Stay away from windows', 'Unplug electronics']
                },
                'evening': {
                    'activities': ['Indoor activities only'],
                    'clothing': ['Comfortable wear'],
                    'health_tips': ['Monitor weather updates']
                },
                'night': {
                    'activities': ['Sleep', 'Stay indoors'],
                    'clothing': ['Comfortable sleepwear'],
                    'health_tips': ['Stay safe indoors']
                }
            },
            'Mist': {
                'morning': {
                    'activities': ['Light activities', 'Short walks'],
                    'clothing': ['Visibility gear', 'Light layers'],
                    'health_tips': ['Drive carefully', 'Use lights']
                },
                'afternoon': {
                    'activities': ['Indoor preferred', 'Short trips'],
                    'clothing': ['Comfortable layers'],
                    'health_tips': ['Be visible']
                },
                'evening': {
                    'activities': ['Indoor activities'],
                    'clothing': ['Warm layers'],
                    'health_tips': ['Limited visibility']
                },
                'night': {
                    'activities': ['Stay indoors'],
                    'clothing': ['Comfortable wear'],
                    'health_tips': ['Avoid travel']
                }
            }
        }
        
        self.ultra_recommendations = {
            'Clear': {
                'early_morning': {
                    'health_safety': {
                        'uv_warning': 'Minimal UV - perfect for sunrise activities',
                        'air_quality': 'Excellent - fresh morning air',
                        'heat_stress': 'No risk - optimal temperature',
                        'safety_tips': ['Perfect for outdoor exercise', 'Great visibility', 'Cool and refreshing'],
                        'health_benefits': ['Vitamin D synthesis beginning', 'Fresh oxygen levels', 'Natural energy boost']
                    },
                    'clothing': {
                        'recommended': ['Light athletic wear', 'Moisture-wicking fabrics', 'Light jacket for warmth'],
                        'avoid': ['Heavy clothing', 'Non-breathable materials'],
                        'accessories': ['Water bottle', 'Fitness tracker', 'Light cap'],
                        'footwear': ['Running shoes', 'Athletic socks', 'Comfortable sneakers']
                    },
                    'activities': {
                        'highly_recommended': ['Sunrise jogging', 'Meditation', 'Yoga', 'Photography', 'Walking'],
                        'suitable': ['Cycling', 'Outdoor stretching', 'Morning coffee outside'],
                        'avoid': ['Heavy workouts without warmup', 'Intense sun exposure'],
                        'energy_level': 'Building - perfect for gentle start'
                    },
                    'spotify_moods': ['morning meditation', 'sunrise acoustic', 'peaceful piano', 'nature sounds'],
                    'color_palette': ['#FFE5B4', '#FFEFD5', '#F0E68C', '#87CEEB', '#B0E0E6'],
                    'comfort_factors': {
                        'temperature_comfort': 9,
                        'humidity_comfort': 9,
                        'wind_comfort': 8,
                        'overall_comfort': 9
                    }
                },
                'morning': {
                    'health_safety': {
                        'uv_warning': 'Low to moderate UV - light protection recommended',
                        'air_quality': 'Excellent conditions for outdoor activities',
                        'heat_stress': 'Minimal risk - perfect exercise window',
                        'safety_tips': ['Apply light sunscreen', 'Stay hydrated', 'Perfect for cardio'],
                        'health_benefits': ['Peak vitamin D synthesis', 'Optimal air quality', 'Energy and mood boost']
                    },
                    'clothing': {
                        'recommended': ['Athletic wear', 'Breathable fabrics', 'Light sun protection', 'Comfortable layers'],
                        'avoid': ['Heavy layers', 'Dark heat-absorbing colors', 'Non-breathable materials'],
                        'accessories': ['Sunglasses', 'Water bottle', 'Sunscreen SPF 30+', 'Sweat towel'],
                        'footwear': ['Running shoes', 'Breathable sneakers', 'Moisture-wicking socks']
                    },
                    'activities': {
                        'highly_recommended': ['Jogging', 'Cycling', 'Outdoor yoga', 'Hiking', 'Team sports'],
                        'suitable': ['Walking', 'Gardening', 'Outdoor dining', 'Photography'],
                        'avoid': ['Prolonged direct sun without protection'],
                        'energy_level': 'High - ideal for active pursuits'
                    },
                    'spotify_moods': ['workout motivation', 'upbeat pop', 'energetic indie', 'morning pump-up'],
                    'color_palette': ['#FFD700', '#FFA500', '#87CEEB', '#00BFFF', '#32CD32'],
                    'comfort_factors': {
                        'temperature_comfort': 10,
                        'humidity_comfort': 9,
                        'wind_comfort': 9,
                        'overall_comfort': 10
                    }
                },
                'late_morning': {
                    'health_safety': {
                        'uv_warning': 'Moderate UV - protection advised',
                        'air_quality': 'Good conditions, monitor in urban areas',
                        'heat_stress': 'Low to moderate risk',
                        'safety_tips': ['Seek shade periodically', 'Hydrate regularly', 'Protect skin'],
                        'health_benefits': ['Good for vitamin D', 'Active metabolism', 'High alertness']
                    },
                    'clothing': {
                        'recommended': ['Light clothing', 'UV protection', 'Breathable materials', 'Hat'],
                        'avoid': ['Dark colors', 'Heavy fabrics', 'Tight clothing'],
                        'accessories': ['Sunglasses', 'Sunscreen', 'Water bottle', 'Light scarf'],
                        'footwear': ['Comfortable shoes', 'Breathable options', 'Light socks']
                    },
                    'activities': {
                        'highly_recommended': ['Brunch outdoors', 'Light shopping', 'Sightseeing', 'Photography'],
                        'suitable': ['Walking', 'Casual sports', 'Gardening', 'Outdoor markets'],
                        'avoid': ['Intense physical activity', 'Extended sun exposure'],
                        'energy_level': 'Moderate to high - transitioning period'
                    },
                    'spotify_moods': ['brunch vibes', 'feel-good hits', 'sunny day playlist', 'acoustic pop'],
                    'color_palette': ['#FDB813', '#FFCC00', '#FFE135', '#F0E68C', '#87CEEB'],
                    'comfort_factors': {
                        'temperature_comfort': 8,
                        'humidity_comfort': 8,
                        'wind_comfort': 8,
                        'overall_comfort': 8
                    }
                },
                'afternoon': {
                    'health_safety': {
                        'uv_warning': 'High UV levels - maximum protection required',
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
                    'comfort_factors': {
                        'temperature_comfort': 6,
                        'humidity_comfort': 6,
                        'wind_comfort': 7,
                        'overall_comfort': 6
                    }
                },
                'late_afternoon': {
                    'health_safety': {
                        'uv_warning': 'Moderate to high UV - still need protection',
                        'air_quality': 'Improving as temperature drops',
                        'heat_stress': 'Decreasing risk',
                        'safety_tips': ['Continue sun protection', 'Stay hydrated', 'Good for activities'],
                        'health_benefits': ['Golden hour benefits', 'Comfortable temperatures', 'Social time']
                    },
                    'clothing': {
                        'recommended': ['Casual layers', 'Light fabrics', 'Comfortable attire'],
                        'avoid': ['Heavy clothing', 'Restrictive items'],
                        'accessories': ['Sunglasses', 'Light bag', 'Camera'],
                        'footwear': ['Walking shoes', 'Comfortable sandals', 'Casual sneakers']
                    },
                    'activities': {
                        'highly_recommended': ['Photography', 'Outdoor dining', 'Walking', 'Social sports'],
                        'suitable': ['Shopping', 'Sightseeing', 'Casual exercise', 'Outdoor events'],
                        'avoid': ['Intense workouts in heat'],
                        'energy_level': 'Moderate - perfect for varied activities'
                    },
                    'spotify_moods': ['golden hour', 'indie sunset', 'chill pop', 'acoustic afternoon'],
                    'color_palette': ['#FF9A56', '#FFB366', '#FFC947', '#FFD700', '#FFA500'],
                    'comfort_factors': {
                        'temperature_comfort': 8,
                        'humidity_comfort': 8,
                        'wind_comfort': 8,
                        'overall_comfort': 8
                    }
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
                    'comfort_factors': {
                        'temperature_comfort': 9,
                        'humidity_comfort': 9,
                        'wind_comfort': 9,
                        'overall_comfort': 9
                    }
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
                    'comfort_factors': {
                        'temperature_comfort': 8,
                        'humidity_comfort': 8,
                        'wind_comfort': 7,
                        'overall_comfort': 8
                    }
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
                    'comfort_factors': {
                        'temperature_comfort': 9,
                        'humidity_comfort': 8,
                        'wind_comfort': 8,
                        'overall_comfort': 9
                    }
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
                    'comfort_factors': {
                        'temperature_comfort': 8,
                        'humidity_comfort': 8,
                        'wind_comfort': 8,
                        'overall_comfort': 8
                    }
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
                    'comfort_factors': {
                        'temperature_comfort': 9,
                        'humidity_comfort': 8,
                        'wind_comfort': 8,
                        'overall_comfort': 9
                    }
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
                    'comfort_factors': {
                        'temperature_comfort': 8,
                        'humidity_comfort': 8,
                        'wind_comfort': 7,
                        'overall_comfort': 8
                    }
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
                    'comfort_factors': {
                        'temperature_comfort': 7,
                        'humidity_comfort': 6,
                        'wind_comfort': 5,
                        'overall_comfort': 6
                    }
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
                    'comfort_factors': {
                        'temperature_comfort': 7,
                        'humidity_comfort': 6,
                        'wind_comfort': 5,
                        'overall_comfort': 6
                    }
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
                    'comfort_factors': {
                        'temperature_comfort': 8,
                        'humidity_comfort': 7,
                        'wind_comfort': 6,
                        'overall_comfort': 7
                    }
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
                    'comfort_factors': {
                        'temperature_comfort': 9,
                        'humidity_comfort': 8,
                        'wind_comfort': 7,
                        'overall_comfort': 8
                    }
                }
            }
        }
    
    async def get_ultra_weather_analysis(self, lat: float, lon: float) -> Dict:
        weather_data = None
        air_quality = None
        health_insights = None
        
        if self.accuweather_api_key:
            try:
                logger.info("Querying AccuWeather (primary provider)")
                weather_data = await self._get_accuweather_current(lat, lon)
                air_quality = await self._get_accuweather_air_quality(lat, lon)
                health_insights = await self._calculate_comprehensive_health_insights(weather_data, air_quality)
            except Exception as e:
                logger.warning(f"AccuWeather failed: {e}")
        
        if not weather_data:
            for provider in ['tomorrow', 'visual_crossing', 'openweather']:
                try:
                    if provider == 'tomorrow' and self.tomorrow_api_key:
                        weather_data = await self._get_tomorrow_current(lat, lon)
                        if not air_quality:
                            air_quality = await self._get_tomorrow_air_quality(lat, lon)
                    elif provider == 'visual_crossing' and self.visual_crossing_api_key:
                        weather_data = await self._get_visual_crossing_current(lat, lon)
                    elif provider == 'openweather' and self.openweather_api_key:
                        weather_data = await self._get_openweather_current(lat, lon)
                        if not air_quality:
                            air_quality = await self._get_openweather_air_quality(lat, lon)
                    
                    if weather_data:
                        logger.info(f"Using {provider} as weather provider")
                        break
                except Exception as e:
                    logger.warning(f"{provider} failed: {e}")
        
        if not weather_data:
            raise Exception("All weather providers failed")
        
        if not health_insights:
            health_insights = await self._calculate_comprehensive_health_insights(weather_data, air_quality)
        
        current_hour = datetime.now().hour
        time_period = self._get_precise_time_period(current_hour)
        
        recommendations = self._get_ultra_recommendations(
            weather_data.condition, time_period, weather_data.temperature, weather_data
        )
        
        activity_scores = self._calculate_activity_suitability(weather_data, time_period)
        
        personalized_suggestions = self._generate_personalized_suggestions(
            weather_data, time_period, health_insights
        )
        
        return {
            'success': True,
            'weather': {
                'temperature': {
                    'current': round(weather_data.temperature, 1),
                    'feels_like': round(weather_data.feels_like, 1),
                    'dew_point': round(weather_data.dew_point, 1),
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
            },
            'air_quality': air_quality.__dict__ if air_quality else None,
            'health_insights': health_insights.__dict__ if health_insights else None,
            'time_period': time_period,
            'comprehensive_insights': {
                'health_safety': recommendations['health_safety'],
                'clothing_recommendations': recommendations['clothing'],
                'activity_suggestions': recommendations['activities'],
                'spotify_moods': recommendations['spotify_moods'],
                'color_palette': recommendations['color_palette'],
                'comfort_factors': recommendations['comfort_factors']
            },
            'activity_suitability': activity_scores,
            'personalized_suggestions': personalized_suggestions,
            'location': {'lat': lat, 'lon': lon},
            'timestamp': datetime.utcnow().isoformat(),
            'accuracy_score': 0.98 if self.accuweather_api_key and weather_data.condition else 0.85
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
    
    async def _get_accuweather_air_quality(self, lat: float, lon: float) -> AirQualityData:

        location_key = await self._get_accuweather_location_key(lat, lon)

        url = f"http://dataservice.accuweather.com/airquality/v2/current/{location_key}"
        params = {'apikey': self.accuweather_api_key}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"AccuWeather air quality API error: {response.status}")
                
                data = await response.json()
                
                if not data or not isinstance(data, list) or len(data) == 0:
                    raise Exception("Invalid AccuWeather air quality response")
                
                aqi = data[0]['AirQualityIndex']
                level_map = {
                    (0, 50): 'Good',
                    (51, 100): 'Moderate',
                    (101, 150): 'Unhealthy for Sensitive Groups',
                    (151, 200): 'Unhealthy',
                    (201, 300): 'Very Unhealthy',
                    (301, 500): 'Hazardous'
                }
                
                level = 'Unknown'
                for (min_val, max_val), level_name in level_map.items():
                    if min_val <= aqi <= max_val:
                        level = level_name
                        break
                
                return AirQualityData(
                    aqi=aqi,
                    level=level,
                    pm2_5=data[0].get('PM25', 0),
                    pm10=data[0].get('PM10', 0),
                    o3=data[0].get('Ozone', 0),
                    no2=data[0].get('NO2', 0),
                    co=data[0].get('CO', 0),
                    so2=data[0].get('SO2', 0),
                    health_recommendation=self._get_enhanced_air_quality_advice(aqi)
                )
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
    
    async def _get_tomorrow_air_quality(self, lat: float, lon: float) -> AirQualityData:
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
    
    async def _get_openweather_air_quality(self, lat: float, lon: float) -> AirQualityData:
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
    
    def _calculate_activity_suitability(self, weather: WeatherData, time_period: str) -> Dict:
        activities = {
            'outdoor_exercise': 0,
            'walking': 0,
            'cycling': 0,
            'water_activities': 0,
            'indoor_activities': 0,
            'social_outdoor': 0,
            'photography': 0,
            'relaxation': 0
        }
        
        temp = weather.temperature
        wind = weather.wind_speed
        humidity = weather.humidity
        condition = weather.condition.lower()
        
        if 15 <= temp <= 25 and wind < 15 and 'clear' in condition:
            activities['outdoor_exercise'] = 95
        elif 10 <= temp <= 30 and wind < 20:
            activities['outdoor_exercise'] = 75
        else:
            activities['outdoor_exercise'] = 40
        
        if temp > 0 and wind < 25 and 'thunderstorm' not in condition:
            activities['walking'] = 85
        else:
            activities['walking'] = 30
        
        if 10 <= temp <= 28 and wind < 20 and 'rain' not in condition:
            activities['cycling'] = 80
        else:
            activities['cycling'] = 35
        
        if temp > 22 and 'clear' in condition or 'cloud' in condition:
            activities['water_activities'] = 90
        else:
            activities['water_activities'] = 20
        
        activities['indoor_activities'] = 85
        
        if 15 <= temp <= 28 and 'rain' not in condition:
            activities['social_outdoor'] = 85
        else:
            activities['social_outdoor'] = 45
        
        if time_period in ['early_morning', 'late_afternoon', 'evening']:
            activities['photography'] = 90
        else:
            activities['photography'] = 70
        
        if 18 <= temp <= 26:
            activities['relaxation'] = 95
        else:
            activities['relaxation'] = 75
        
        return activities
    
    def _generate_personalized_suggestions(self, weather: WeatherData, time_period: str, health: HealthInsights) -> Dict:
        suggestions = {
            'immediate_actions': [],
            'clothing_specifics': [],
            'activity_recommendations': [],
            'health_priorities': [],
            'comfort_tips': []
        }
        
        temp = weather.temperature
        condition = weather.condition.lower()
        wind = weather.wind_speed
        humidity = weather.humidity
        
        if temp > 30:
            suggestions['immediate_actions'].extend([
                'Seek air conditioning or shade',
                'Increase water intake immediately',
                'Avoid strenuous outdoor activities'
            ])
            suggestions['clothing_specifics'].extend([
                'Wear light-colored, loose-fitting clothing',
                'Use a wide-brimmed hat',
                'Apply sunscreen SPF 50+'
            ])
        elif temp < 5:
            suggestions['immediate_actions'].extend([
                'Layer clothing properly',
                'Protect extremities from cold',
                'Warm up gradually when going outside'
            ])
            suggestions['clothing_specifics'].extend([
                'Wear thermal underwear',
                'Use waterproof outer layer',
                'Don\'t forget gloves and warm hat'
            ])
        
        if time_period == 'morning':
            suggestions['activity_recommendations'].extend([
                'Perfect time for outdoor exercise',
                'Great for vitamin D synthesis',
                'Ideal for photography with soft lighting'
            ])
        elif time_period == 'afternoon':
            suggestions['activity_recommendations'].extend([
                'Seek indoor or shaded activities',
                'Perfect for swimming or water sports',
                'Good time for shopping or museums'
            ])
        
        if 'rain' in condition:
            suggestions['immediate_actions'].append('Have rain gear ready')
            suggestions['comfort_tips'].append('Enjoy the calming sound of rain')
        
        if wind > 20:
            suggestions['immediate_actions'].append('Secure loose items outdoors')
            suggestions['clothing_specifics'].append('Wear windproof outer layer')
        
        suggestions['health_priorities'].append(health.hydration_advice)
        suggestions['health_priorities'].append(health.exercise_advice)
        
        return suggestions
    
    def _get_time_period(self, hour: int) -> str:
        if 6 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 18:
            return 'afternoon'
        elif 18 <= hour < 22:
            return 'evening'
        else:
            return 'night'
    
    def _get_precise_time_period(self, hour: int) -> str:
        for period, (start, end) in self.time_periods.items():
            if start <= hour < end or (period == 'night' and (hour >= 22 or hour < 4)):
                return period
        return 'morning'
    
    def _get_ultra_recommendations(self, condition: str, time_period: str, temperature: float, weather_data: WeatherData) -> Dict:
        base_recommendations = self.ultra_recommendations.get(condition, {}).get(time_period, {})
        
        if not base_recommendations:
            base_recommendations = self.ultra_recommendations.get('Clear', {}).get('morning', {})
        
        enhanced = json.loads(json.dumps(base_recommendations))
        
        if temperature > 35:
            enhanced['health_safety']['safety_tips'].extend(['Extreme heat warning', 'Stay indoors during peak hours'])
            enhanced['activities']['avoid'].extend(['Any outdoor activities', 'Direct sun exposure'])
        elif temperature < -5:
            enhanced['health_safety']['safety_tips'].extend(['Extreme cold warning', 'Limit outdoor exposure'])
            enhanced['clothing']['recommended'].extend(['Extreme cold gear', 'Face protection'])
        
        return enhanced
    
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
    
    def _get_enhanced_air_quality_advice(self, aqi: int) -> str:
        if aqi <= 50:
            return "Excellent air quality - enjoy unrestricted outdoor activities"
        elif aqi <= 100:
            return "Good air quality - suitable for all outdoor activities"
        elif aqi <= 150:
            return "Moderate air quality - sensitive individuals should reduce prolonged outdoor exertion"
        elif aqi <= 200:
            return "Unhealthy air quality - everyone should limit prolonged outdoor exertion"
        elif aqi <= 300:
            return "Very unhealthy air quality - avoid outdoor activities"
        else:
            return "Hazardous air quality - remain indoors with windows closed"
    
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
        elif wind_speed < 61:
            return "Near gale"
        elif wind_speed < 74:
            return "Gale"
        else:
            return "Strong gale"

weather_service = UltraWeatherService()
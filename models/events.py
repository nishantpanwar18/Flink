"""
Event data models for device analytics platform.
Defines schemas for all event types.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
import json


@dataclass
class AppUsageEvent:
    """Application usage event from device SDK"""
    device_id: str
    user_id: str
    app_name: str
    app_category: str
    action: str  # opened, closed, backgrounded
    session_id: str
    duration_seconds: Optional[int]
    timestamp: str
    device_os: str
    device_model: str
    app_version: str
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class VideoEvent:
    """Video playback event from device SDK"""
    device_id: str
    user_id: str
    video_id: str
    video_title: str
    video_category: str
    action: str  # play, pause, stop, complete, seek
    position_seconds: int
    duration_seconds: int
    quality: str  # 480p, 720p, 1080p, 4K
    timestamp: str
    device_os: str
    buffering_time_ms: Optional[int]
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class PurchaseEvent:
    """Purchase transaction event from device SDK"""
    device_id: str
    user_id: str
    transaction_id: str
    product_id: str
    product_name: str
    product_category: str
    price: float
    currency: str
    quantity: int
    payment_method: str
    timestamp: str
    device_os: str
    country: str
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class DeviceMetrics:
    """Device health and performance metrics"""
    device_id: str
    user_id: str
    cpu_usage_percent: float
    memory_usage_percent: float
    battery_level_percent: int
    network_type: str  # wifi, 4g, 5g
    network_speed_mbps: float
    timestamp: str
    device_os: str
    os_version: str
    app_crashes: int
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))

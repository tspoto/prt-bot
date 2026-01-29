import requests
from google.transit import gtfs_realtime_pb2
from atproto import Client
import time
import json
import os
from datetime import datetime
import pytz
import hashlib
import re

class PRTAlertBot:
    def __init__(self):
        # Get credentials from environment variables
        self.bluesky_handle = os.environ.get('BLUESKY_HANDLE')
        self.bluesky_password = os.environ.get('BLUESKY_PASSWORD')
        
        if not self.bluesky_handle or not self.bluesky_password:
            raise ValueError("Missing BLUESKY_HANDLE or BLUESKY_PASSWORD environment variables")
        
        # Port Authority GTFS-RT Alert Feeds (bus and train)
        self.alert_feed_urls = {
            'bus': 'https://truetime.portauthority.org/gtfsrt-bus/alerts',
            'train': 'https://truetime.portauthority.org/gtfsrt-train/alerts'
        }
        
        # Known PRT routes for emoji detection
        self.known_routes = ['1', '2', '4', '6', '7', '8', '11', '12', '13', '14', '15', '16', '17', '18', '19L', '20', '21', '22', '24', '26', '27', '28X', '29', '31', '36', '38', '39', '40', '41', '43', '44', '48', '51', '51L', '52L', '53', '53L', '54', '55', '56', '57', '58', '59', '60', '61A', '61B', '61C', '61D', '64', '65', '67', '69', '71', '71A', '71B', '71C', '71D', '74', '75', '77', '79', '81', '82', '83', '86', '87', '88', '89', '91', '93', 'G2', 'G3', 'G31', 'O1', 'O12', 'O5', 'P1', 'P10', 'P12', 'P13', 'P16', 'P17', 'P3', 'P67', 'P68', 'P69', 'P7', 'P71', 'P76', 'P78', 'Y1', 'Y45', 'Y46', 'Y47', 'Y49']
        
        self.posted_ids_file = 'posted_alerts.json'
        self.posted_ids = self.load_posted_ids()
        
        # Initialize Bluesky client
        print("Logging into Bluesky...")
        self.client = Client()
        self.client.login(self.bluesky_handle, self.bluesky_password)
        print("✓ Logged in successfully")
    
    def load_posted_ids(self):
        """Load previously posted alert IDs"""
        try:
            with open(self.posted_ids_file, 'r') as f:
                data = json.load(f)
                print(f"Loaded {len(data)} previously posted IDs")
                return set(data)
        except FileNotFoundError:
            print("No previous posts found - starting fresh")
            return set()
    
    def save_posted_ids(self):
        """Save posted IDs to disk"""
        with open(self.posted_ids_file, 'w') as f:
            json.dump(list(self.posted_ids), f, indent=2)
    
    def get_alert_hash(self, entity):
        """Generate unique hash from alert content (not just ID)"""
        alert = entity.alert
        
        # Get header and description
        header = ""
        if alert.header_text.translation:
            header = alert.header_text.translation[0].text
        
        description = ""
        if alert.description_text.translation:
            description = alert.description_text.translation[0].text
        
        # Create hash from content
        content = f"{header}|{description}"
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def fetch_alerts(self):
        """Fetch alerts from Port Authority GTFS-RT feeds (bus and train)"""
        all_alerts = []
        
        for feed_type, feed_url in self.alert_feed_urls.items():
            try:
                response = requests.get(feed_url, timeout=10)
                response.raise_for_status()
                
                # Parse GTFS-Realtime protobuf
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(response.content)
                
                feed_alerts = []
                for entity in feed.entity:
                    if entity.HasField('alert'):
                        feed_alerts.append((feed_type, entity))
                
                print(f"✓ Fetched {len(feed_alerts)} alerts from {feed_type} feed")
                all_alerts.extend(feed_alerts)
                
            except Exception as e:
                print(f"✗ Error fetching {feed_type} alerts: {e}")
        
        print(f"✓ Total: {len(all_alerts)} alerts from all feeds")
        return all_alerts
    
    def format_alert(self, entity, feed_types):
        """Format alert for Bluesky post"""
        alert = entity.alert
        
        # Get header and description
        header = ""
        if alert.header_text.translation:
            header = alert.header_text.translation[0].text
        
        description = ""
        if alert.description_text.translation:
            description = alert.description_text.translation[0].text
        
        # Use description if available, otherwise header
        if description:
            text = description
        else:
            text = header
        
        # Clean up newlines
        text = text.replace('\n\n', ' - ').replace('\n', ' ').strip()
        
        # Detect Out of Service BEFORE replacing
        is_out_of_service = bool(re.search(r'\b(OS|O/S|OSS)\b', text, re.IGNORECASE))
        
        # Find route numbers BEFORE any replacements
        route_pattern = r'\b([A-Z]?\d+[A-Z]?)\b'
        found_routes = re.findall(route_pattern, text)
        unique_routes = [r for r in set(found_routes) if r in self.known_routes]
        
        # Replace OS/O/S/OSS with "Out of Service"
        text = re.sub(r'\b(OSS|O/S|OS)\b', 'Out of Service', text, flags=re.IGNORECASE)
        
        # Format times: 957 → 9:57
        def add_time_colon(match):
            t = match.group(0)
            if len(t) == 3:
                return f"{t[0]}:{t[1:]}"
            elif len(t) == 4:
                return f"{t[:2]}:{t[2:]}"
            return t
        
        text = re.sub(r'\b([0-2]?\d{3})\b(?=\s*[-–]|\s*[ap]|\s*[IO]B)', add_time_colon, text)
        
        # Try full replacements
        full_text = text.replace(' IB ', ' Inbound ').replace(' OB ', ' Outbound ')
        full_text = full_text.replace('IB:', 'Inbound:').replace('OB:', 'Outbound:')
        
        # Color emojis
        full_text = re.sub(r'\bRED\b', '🟥 Red', full_text, flags=re.IGNORECASE)
        full_text = re.sub(r'\bBLUE\b', '🟦 Blue', full_text, flags=re.IGNORECASE)
        full_text = re.sub(r'\b(SILVER|SLVR)\b', '⬜ Silver', full_text, flags=re.IGNORECASE)
        
        # Add route emojis (1-2 routes only)
        primary_feed = feed_types[0] if feed_types else 'bus'
        if len(unique_routes) <= 2 and len(unique_routes) > 0:
            emoji = '🚌' if primary_feed == 'bus' else '🚊'
            for route in unique_routes:
                # Replace first occurrence only
                full_text = re.sub(rf'\b{re.escape(route)}\b', f'{route} {emoji}', full_text, count=1)
        
        # Use full text if it fits
        if len(full_text) <= 280:
            text = full_text
        
        # Add feed emoji prefix
        if len(feed_types) > 1:
            prefix = '🚌🚊'
        elif 'train' in feed_types:
            prefix = '🚊'
        elif len(unique_routes) > 0:
            prefix = ''  # Route has emoji already
        else:
            prefix = '🚌'
        
        # Add out of service emoji
        if is_out_of_service:
            text = f"⚠️ {text}"
        
        if prefix:
            text = f"{prefix} {text}".strip()
        
        # Bluesky limit
        if len(text) > 300:
            text = text[:297] + "..."
        
        return text
    
    def is_within_operating_hours(self):
        """Check if we're between 5 AM and midnight Eastern"""
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)
        
        if now.hour < 5:
            print(f"Outside operating hours ({now.strftime('%I:%M %p')} ET) - skipping")
            return False
        
        return True
    
    def run(self):
        """Main bot execution"""
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)
        
        print(f"\n{'='*50}")
        print(f"PRT Alert Bot - {now.strftime('%Y-%m-%d %I:%M:%S %p')} ET")
        print(f"{'='*50}\n")
        
        if not self.is_within_operating_hours():
            return 0
        
        # Fetch alerts
        alerts = self.fetch_alerts()
        
        if not alerts:
            print("No alerts to process")
            return 0
        
        # Track which feeds each alert appears in
        alert_feeds = {}
        alert_entities = {}
        
        for feed_type, entity in alerts:
            alert_hash = self.get_alert_hash(entity)
            if alert_hash:
                if alert_hash not in alert_feeds:
                    alert_feeds[alert_hash] = []
                    alert_entities[alert_hash] = entity
                alert_feeds[alert_hash].append(feed_type)
        
        # Build list of new alerts
        alerts_to_post = []
        for alert_hash, feed_types in alert_feeds.items():
            if alert_hash not in self.posted_ids:
                entity = alert_entities[alert_hash]
                sort_key = int(entity.id) if entity.id.isdigit() else 0
                alerts_to_post.append((sort_key, alert_hash, feed_types, entity))
        
        alerts_to_post.sort(key=lambda x: x[0])
        
        print(f"\nFound {len(alerts_to_post)} new alerts to post")
        
        # Post to Bluesky
        posted_count = 0
        for _, alert_hash, feed_types, entity in alerts_to_post:
            try:
                post_text = self.format_alert(entity, feed_types)
                
                print(f"\nPosting: {post_text[:80]}...")
                self.client.send_post(text=post_text)
                
                self.posted_ids.add(alert_hash)
                self.save_posted_ids()
                posted_count += 1
                
                print("✓ Posted successfully")
                
                if posted_count < len(alerts_to_post):
                    time.sleep(3)
                
            except Exception as e:
                print(f"✗ Error posting: {e}")
        
        print(f"\n{'='*50}")
        print(f"Summary: Posted {posted_count} new alerts")
        print(f"{'='*50}\n")
        
        return posted_count

if __name__ == "__main__":
    try:
        bot = PRTAlertBot()
        bot.run()
    except Exception as e:
        print(f"Fatal error: {e}")
        exit(1)
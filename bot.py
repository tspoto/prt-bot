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
        """Format alert for Bluesky post
        
        Args:
            entity: GTFS alert entity
            feed_types: list of feed types this alert appeared in (e.g., ['bus'] or ['bus', 'train'])
        """
        alert = entity.alert
        
        # Get header text (alert title)
        header = ""
        if alert.header_text.translation:
            header = alert.header_text.translation[0].text
        
        # Get description text (details)
        description = ""
        if alert.description_text.translation:
            description = alert.description_text.translation[0].text
        
        # Combine header and description
        if description and description != header:
            # Remove redundant header prefix if it appears in description
            if ':' in header:
                prefix = header.split(':')[0].strip()
                # Check if the prefix is redundant (appears again after colon)
                rest_of_header = header.split(':', 1)[1].strip() if ':' in header else header
                if prefix.lower() in rest_of_header.lower() and len(prefix) > 3:
                    # Redundant prefix like "1/27 12 OS: 12 OS ..."
                    header = rest_of_header
            
            text = f"{header}: {description}"
        else:
            text = header
        
        # Clean up excessive newlines
        text = text.replace('\\n\\n', ' - ').replace('\\n', ' ').strip()
        
        # Detect Out of Service
        is_out_of_service = bool(re.search(r'\b(OS|O/S|OSS)\b', text, re.IGNORECASE))
        
        # Replace OS/O/S/OSS with "Out of Service"
        text = re.sub(r'\bOSS\b', 'Out of Service', text, flags=re.IGNORECASE)
        text = re.sub(r'\bO/S\b', 'Out of Service', text, flags=re.IGNORECASE)
        text = re.sub(r'\bOS\b', 'Out of Service', text, flags=re.IGNORECASE)
        
        # Format times - add colons to time numbers like 237 → 2:37
        def format_time(match):
            time_str = match.group(0)
            if len(time_str) == 3:  # e.g., 237 → 2:37
                return f"{time_str[0]}:{time_str[1:]}"
            elif len(time_str) == 4:  # e.g., 1126 → 11:26
                return f"{time_str[:2]}:{time_str[2:]}"
            return time_str
        
        # Match 3-4 digit numbers that look like times
        text = re.sub(r'\b([0-2]?\d{3})\b(?=\s*[-–]|\s*[ap]|\s*[IO]B)', format_time, text)
        
        # Replace IB/OB with Inbound/Outbound if we have room
        text_with_full = text.replace(' IB ', ' Inbound ').replace(' OB ', ' Outbound ')
        text_with_full = text_with_full.replace('IB:', 'Inbound:').replace('OB:', 'Outbound:')
        text_with_full = text_with_full.replace('IB-', 'Inbound-').replace('OB-', 'Outbound-')
        
        # Add color emojis for train lines
        text_with_full = re.sub(r'\bRED\b', '🟥 Red', text_with_full, flags=re.IGNORECASE)
        text_with_full = re.sub(r'\bBLUE\b', '🟦 Blue', text_with_full, flags=re.IGNORECASE)
        text_with_full = re.sub(r'\b(SILVER|SLVR)\b', '⬜ Silver', text_with_full, flags=re.IGNORECASE)
        
        # Count routes in message
        route_pattern = r'\b([A-Z]?\d+[A-Z]?)\b'
        routes = re.findall(route_pattern, text_with_full)
        # Filter to only known PRT routes
        routes = [r for r in routes if r in self.known_routes]
        unique_routes = list(set(routes))
        
        # Determine primary feed type (use first one for emoji selection)
        primary_feed = feed_types[0] if feed_types else 'bus'
        
        # Add route emojis (only if 1-2 routes)
        if len(unique_routes) <= 2 and len(unique_routes) > 0:
            route_emoji = '🚌' if primary_feed == 'bus' else '🚊'
            for route in unique_routes:
                # Add emoji after route number with space
                text_with_full = re.sub(rf'\b{re.escape(route)}\b', f'{route} {route_emoji}', text_with_full)
        
        # Use full text if it fits, otherwise use abbreviated
        if len(text_with_full) <= 280:  # Leave room for feed emoji and out of service emoji
            text = text_with_full
        
        # Determine feed emoji prefix
        if len(feed_types) > 1:
            # Alert appears in both feeds
            feed_emoji = '🚌🚊'
        elif 'train' in feed_types:
            # Train alerts always get train emoji
            feed_emoji = '🚊'
        elif len(unique_routes) > 0:
            # Bus alerts with route numbers don't need bus emoji (route has it)
            feed_emoji = ''
        else:
            # Bus alerts without route numbers get bus emoji
            feed_emoji = '🚌'
        
        # Add out of service warning emoji
        if is_out_of_service and feed_emoji:
            text = f"{feed_emoji} ⚠️ {text}"
        elif is_out_of_service:
            text = f"⚠️ {text}"
        elif feed_emoji:
            text = f"{feed_emoji} {text}"
        
        # Final safety check - Bluesky has 300 character limit
        if len(text) > 300:
            text = text[:297] + "..."
        
        return text
    
    def is_within_operating_hours(self):
        """Check if we're between 5 AM and midnight Eastern"""
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)
        
        # Allow 5 AM (hour 5) through 11:59 PM (hour 23)
        if now.hour < 5:
            print(f"Outside operating hours ({now.strftime('%I:%M %p')} ET) - skipping")
            return False
        
        return True
    
    def run(self):
        """Main bot execution"""
        # Check operating hours
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
        
        # Process alerts - track which feeds each alert appears in
        alert_feeds = {}  # hash -> list of feed types
        alert_entities = {}  # hash -> entity
        
        for feed_type, entity in alerts:
            alert_hash = self.get_alert_hash(entity)
            if alert_hash:
                if alert_hash not in alert_feeds:
                    alert_feeds[alert_hash] = []
                    alert_entities[alert_hash] = entity
                alert_feeds[alert_hash].append(feed_type)
        
        # Build list of alerts to post (not yet posted)
        alerts_to_post = []
        for alert_hash, feed_types in alert_feeds.items():
            if alert_hash not in self.posted_ids:
                entity = alert_entities[alert_hash]
                # Sort by API ID for consistent ordering
                sort_key = int(entity.id) if entity.id.isdigit() else 0
                alerts_to_post.append((sort_key, alert_hash, feed_types, entity))
        
        # Sort by API ID
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
                
                # Rate limiting - be nice to Bluesky's servers
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
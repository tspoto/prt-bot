import requests
from google.transit import gtfs_realtime_pb2
from atproto import Client
import time
import json
import os
from datetime import datetime
import pytz

class PRTAlertBot:
    def __init__(self):
        # Get credentials from environment variables
        self.bluesky_handle = os.environ.get('BLUESKY_HANDLE')
        self.bluesky_password = os.environ.get('BLUESKY_PASSWORD')
        
        if not self.bluesky_handle or not self.bluesky_password:
            raise ValueError("Missing BLUESKY_HANDLE or BLUESKY_PASSWORD environment variables")
        
        # Port Authority GTFS-RT Alert Feed
        self.alert_feed_url = 'https://truetime.portauthority.org/gtfsrt-bus/alerts'
        
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
    
    def fetch_alerts(self):
        """Fetch alerts from Port Authority GTFS-RT feed"""
        try:
            response = requests.get(self.alert_feed_url, timeout=10)
            response.raise_for_status()
            
            # Parse GTFS-Realtime protobuf
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            
            alerts = []
            for entity in feed.entity:
                if entity.HasField('alert'):
                    alerts.append(entity)
            
            print(f"✓ Fetched {len(alerts)} alerts from Port Authority")
            return alerts
            
        except Exception as e:
            print(f"✗ Error fetching alerts: {e}")
            return []
    
    def format_alert(self, entity):
        """Format alert for Bluesky post"""
        alert = entity.alert
        
        # Get header text (alert title)
        header = ""
        if alert.header_text.translation:
            header = alert.header_text.translation[0].text
        
        # Get description text (details)
        description = ""
        if alert.description_text.translation:
            description = alert.description_text.translation[0].text
        
        # Get affected routes
        routes = []
        for informed_entity in alert.informed_entity:
            if informed_entity.HasField('route_id'):
                routes.append(informed_entity.route_id)
        
        # Build the post text
        if routes:
            route_text = ", ".join(set(routes))
            text = f"Route {route_text}: {header}"
        else:
            text = header
        
        # Add description if available and there's room
        if description and description != header:
            # Clean up description (remove excessive newlines)
            description = description.replace('\\n', ' ').strip()
            if len(text) + len(" - ") + len(description) <= 300:
                text = f"{text} - {description}"
        
        # Bluesky has 300 character limit
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
        
        # Process alerts
        alerts_to_post = []
        for entity in alerts:
            alert_id = entity.id
            if alert_id and alert_id not in self.posted_ids:
                alerts_to_post.append((alert_id, entity))
        
        print(f"\nFound {len(alerts_to_post)} new alerts to post")
        
        # Post to Bluesky
        posted_count = 0
        for alert_id, entity in alerts_to_post:
            try:
                post_text = self.format_alert(entity)
                
                print(f"\nPosting: {post_text[:80]}...")
                self.client.send_post(text=post_text)
                
                self.posted_ids.add(alert_id)
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
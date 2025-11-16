#!/usr/bin/env python3
"""
Local Zerobus Simulator
Generates portal events continuously to demonstrate 5-10s ingestion latency.
Run this to simulate members using the ART portal in real-time.
"""

import json
import time
import random
from datetime import datetime
import uuid
import os
from pathlib import Path

class ZerobusSimulator:
    """Simulates Zerobus HTTP ingestion endpoint."""

    def __init__(self, output_dir="./data/portal_events"):
        self.output_dir = output_dir
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        self.batch_num = 0

    def generate_event(self):
        """Generate a single portal event."""
        event_type = random.choice(["page_view", "search", "form_start", "form_abandon", "download"])

        event = {
            "event_id": str(uuid.uuid4()),
            "member_id": f"M{random.randint(1, 100000):06d}",
            "session_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "page_url": random.choice([
                "/my-account/balance",
                "/insurance/options",
                "/insurance/tpd-vs-income-protection",
                "/contributions/update-rate",
                "/investment/performance",
                "/contact-us",
                "/statements/download"
            ]),
            "search_query": random.choice([
                "insurance premium",
                "contribution rate",
                "retirement balance",
                "TPD coverage",
                "investment options",
                None, None, None  # Most events don't have search
            ]) if event_type == "search" else None,
            "time_on_page_seconds": random.randint(5, 300),
            "device_type": random.choice(["desktop", "mobile", "tablet"]),
            "referrer_url": random.choice([
                "/home",
                "/my-account",
                None
            ]),
            # Add sentiment hint for demo purposes
            "_sentiment_hint": random.choices(
                ["Positive", "Neutral", "Negative"],
                weights=[0.4, 0.35, 0.25],
                k=1
            )[0]
        }

        return event

    def generate_batch(self, num_events=50):
        """Generate a batch of events."""
        return [self.generate_event() for _ in range(num_events)]

    def write_batch(self, events):
        """Write batch to JSON file (simulates Zerobus HTTP POST to Delta)."""
        self.batch_num += 1
        timestamp = int(time.time())
        filename = f"{self.output_dir}/batch_{self.batch_num:04d}_{timestamp}.json"

        with open(filename, 'w') as f:
            for event in events:
                f.write(json.dumps(event) + '\n')

        return filename

    def start(self, events_per_batch=50, interval_seconds=5):
        """
        Start continuous event generation.

        Args:
            events_per_batch: Number of events per batch
            interval_seconds: Seconds between batches (simulates traffic rate)
        """
        print("="*70)
        print("🚀 ZEROBUS SIMULATOR - ART Member Portal Activity")
        print("="*70)
        print(f"\nConfiguration:")
        print(f"  📁 Output Directory: {self.output_dir}")
        print(f"  📊 Events per Batch: {events_per_batch}")
        print(f"  ⏱️  Batch Interval: {interval_seconds} seconds")
        print(f"  🎯 Simulated Latency: 5-10 seconds to Delta Lake")
        print(f"\nSimulating: Members browsing portal, searching, updating accounts...")
        print("\nPress Ctrl+C to stop\n")

        try:
            while True:
                start_time = time.time()

                # Generate events
                events = self.generate_batch(num_events=events_per_batch)

                # Write to file (simulates HTTP POST to Zerobus)
                filename = self.write_batch(events)

                # Calculate statistics
                sentiment_dist = {}
                for event in events:
                    sentiment = event.get('_sentiment_hint', 'Unknown')
                    sentiment_dist[sentiment] = sentiment_dist.get(sentiment, 0) + 1

                elapsed = time.time() - start_time

                # Display batch info
                print(f"📤 Batch {self.batch_num:04d} | {datetime.now().strftime('%H:%M:%S')}")
                print(f"   Events: {len(events)}")
                print(f"   File: {os.path.basename(filename)}")
                print(f"   Sentiment: Pos={sentiment_dist.get('Positive', 0)} "
                      f"Neu={sentiment_dist.get('Neutral', 0)} "
                      f"Neg={sentiment_dist.get('Negative', 0)}")
                print(f"   Generated in: {elapsed*1000:.0f}ms")
                print(f"   → Zerobus: Will appear in Delta in ~5-10 seconds")
                print()

                # Wait for next interval
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            print(f"\n\n✅ Stopped. Generated {self.batch_num} batches")
            print(f"📊 Total Events: ~{self.batch_num * events_per_batch}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Simulate Zerobus ingestion with continuous portal events"
    )
    parser.add_argument(
        "--output-dir",
        default="./data/portal_events",
        help="Output directory for event files"
    )
    parser.add_argument(
        "--events-per-batch",
        type=int,
        default=50,
        help="Number of events per batch"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Seconds between batches"
    )

    args = parser.parse_args()

    simulator = ZerobusSimulator(output_dir=args.output_dir)
    simulator.start(
        events_per_batch=args.events_per_batch,
        interval_seconds=args.interval
    )


if __name__ == "__main__":
    main()

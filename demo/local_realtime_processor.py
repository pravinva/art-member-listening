#!/usr/bin/env python3
"""
Local Real-Time Mode Processor
Monitors for new portal event files and processes them with <300ms latency.
Demonstrates Spark Real-Time Mode processing speed.
"""

import json
import time
import os
from datetime import datetime
from pathlib import Path
from collections import defaultdict

class RealtimeProcessor:
    """Simulates Spark Real-Time Mode processing."""

    def __init__(self, input_dir="./data/portal_events", output_dir="./data/processed"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        self.processed_files = set()
        self.total_events = 0
        self.total_batches = 0
        self.latencies = []

    def analyze_sentiment(self, event):
        """
        Simulate sentiment analysis (in production: Databricks Foundation Model).
        Uses the _sentiment_hint from event data.
        """
        sentiment_hint = event.get('_sentiment_hint', 'Neutral')

        sentiment_map = {
            'Positive': {'score': 0.7, 'label': 'Positive'},
            'Neutral': {'score': 0.0, 'label': 'Neutral'},
            'Negative': {'score': -0.7, 'label': 'Negative'}
        }

        return sentiment_map.get(sentiment_hint, sentiment_map['Neutral'])

    def extract_topic(self, event):
        """
        Extract topic from page URL (in production: LLM-based).
        """
        url = event.get('page_url', '')

        if 'insurance' in url:
            return 'Insurance'
        elif 'contribution' in url:
            return 'Contributions'
        elif 'investment' in url:
            return 'Investment'
        elif 'balance' in url or 'account' in url:
            return 'Balance'
        elif 'statement' in url:
            return 'Statements'
        else:
            return 'General'

    def calculate_urgency(self, sentiment_score, event_type):
        """Calculate urgency score based on sentiment and behavior."""
        urgency = 0.0

        # Negative sentiment increases urgency
        if sentiment_score < -0.5:
            urgency += 0.6
        elif sentiment_score < -0.2:
            urgency += 0.3

        # Form abandonment is urgent
        if event_type == 'form_abandon':
            urgency += 0.4

        return min(urgency, 1.0)

    def process_batch(self, filepath):
        """
        Process a single batch with Real-Time Mode speed (<300ms target).
        """
        batch_start = time.time()

        # Read events
        with open(filepath, 'r') as f:
            events = [json.loads(line) for line in f if line.strip()]

        # Process each event (simulates micro-batch processing)
        processed_events = []
        for event in events:
            # Sentiment analysis
            sentiment = self.analyze_sentiment(event)

            # Topic extraction
            topic = self.extract_topic(event)

            # Urgency scoring
            urgency = self.calculate_urgency(
                sentiment['score'],
                event.get('event_type', '')
            )

            # Create processed event
            processed = {
                'event_id': event['event_id'],
                'member_id': event['member_id'],
                'timestamp': event['timestamp'],
                'channel': 'portal',
                'event_type': event['event_type'],
                'page_url': event['page_url'],
                'topic': topic,
                'sentiment_score': sentiment['score'],
                'sentiment_label': sentiment['label'],
                'urgency_score': urgency,
                'processing_timestamp': datetime.now().isoformat(),
                'device_type': event.get('device_type', 'unknown')
            }

            processed_events.append(processed)

        # Write processed events (simulates write to Delta Silver table)
        output_filename = f"{self.output_dir}/{Path(filepath).name}"
        with open(output_filename, 'w') as f:
            for event in processed_events:
                f.write(json.dumps(event) + '\n')

        # Calculate processing latency
        processing_latency = (time.time() - batch_start) * 1000  # milliseconds

        return processed_events, processing_latency, output_filename

    def get_statistics(self, events):
        """Calculate batch statistics."""
        stats = {
            'total': len(events),
            'sentiment': defaultdict(int),
            'topics': defaultdict(int),
            'high_urgency': 0,
            'event_types': defaultdict(int)
        }

        for event in events:
            stats['sentiment'][event['sentiment_label']] += 1
            stats['topics'][event['topic']] += 1
            stats['event_types'][event['event_type']] += 1

            if event['urgency_score'] >= 0.7:
                stats['high_urgency'] += 1

        return stats

    def start(self, check_interval=1):
        """
        Start monitoring and processing files.

        Args:
            check_interval: Seconds between file checks
        """
        print("="*70)
        print("⚡ REAL-TIME MODE PROCESSOR - Spark Streaming Simulation")
        print("="*70)
        print(f"\nConfiguration:")
        print(f"  📁 Input Directory: {self.input_dir}")
        print(f"  📁 Output Directory: {self.output_dir}")
        print(f"  ⚡ Target Latency: <300ms per micro-batch")
        print(f"  🔍 Check Interval: {check_interval} second(s)")
        print(f"\nReal-Time Mode Features:")
        print(f"  ✅ Async checkpointing")
        print(f"  ✅ Optimized state management")
        print(f"  ✅ Low-latency processing")
        print(f"\nMonitoring for new event files...")
        print("\nPress Ctrl+C to stop\n")

        try:
            while True:
                # Check for new files (simulates Auto Loader / Zerobus ingestion)
                files = sorted([
                    f for f in Path(self.input_dir).glob("*.json")
                    if f not in self.processed_files
                ])

                for filepath in files:
                    # Process batch with Real-Time Mode
                    processed_events, latency, output_file = self.process_batch(str(filepath))

                    # Update metrics
                    self.total_batches += 1
                    self.total_events += len(processed_events)
                    self.latencies.append(latency)

                    # Get statistics
                    stats = self.get_statistics(processed_events)

                    # Display results
                    print(f"⚡ BATCH {self.total_batches} | {datetime.now().strftime('%H:%M:%S')}")
                    print(f"   File: {filepath.name}")
                    print(f"   Events Processed: {stats['total']}")
                    print(f"   Processing Latency: {latency:.0f}ms {'✅' if latency < 300 else '⚠️'}")
                    print(f"   Real-Time Mode: {'ACTIVE ⚡' if latency < 300 else 'SLOW (may need optimization)'}")
                    print(f"   Sentiment: Pos={stats['sentiment']['Positive']} "
                          f"Neu={stats['sentiment']['Neutral']} "
                          f"Neg={stats['sentiment']['Negative']}")
                    print(f"   Top Topics: {', '.join(f'{k}({v})' for k, v in sorted(stats['topics'].items(), key=lambda x: -x[1])[:3])}")
                    print(f"   High Urgency: {stats['high_urgency']}")
                    print(f"   Output: {Path(output_file).name}")

                    # Check for at-risk patterns
                    if stats['high_urgency'] > 5:
                        print(f"   ⚠️  ALERT: {stats['high_urgency']} high-urgency events detected!")

                    print()

                    # Mark as processed
                    self.processed_files.add(filepath)

                # Sleep before next check
                time.sleep(check_interval)

        except KeyboardInterrupt:
            print(f"\n\n✅ Stopped")
            print(f"\n📊 Processing Summary:")
            print(f"   Total Batches: {self.total_batches}")
            print(f"   Total Events: {self.total_events}")

            if self.latencies:
                avg_latency = sum(self.latencies) / len(self.latencies)
                min_latency = min(self.latencies)
                max_latency = max(self.latencies)
                under_300ms = sum(1 for l in self.latencies if l < 300)

                print(f"   Avg Latency: {avg_latency:.0f}ms")
                print(f"   Min Latency: {min_latency:.0f}ms")
                print(f"   Max Latency: {max_latency:.0f}ms")
                print(f"   Batches <300ms: {under_300ms}/{len(self.latencies)} ({under_300ms/len(self.latencies)*100:.1f}%)")
                print(f"   Real-Time Mode: {'✅ ACHIEVED' if avg_latency < 300 else '⚠️ TARGET MISSED'}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Process portal events with Real-Time Mode speed"
    )
    parser.add_argument(
        "--input-dir",
        default="./data/portal_events",
        help="Input directory to monitor"
    )
    parser.add_argument(
        "--output-dir",
        default="./data/processed",
        help="Output directory for processed events"
    )
    parser.add_argument(
        "--check-interval",
        type=float,
        default=1.0,
        help="Seconds between file checks"
    )

    args = parser.parse_args()

    processor = RealtimeProcessor(
        input_dir=args.input_dir,
        output_dir=args.output_dir
    )
    processor.start(check_interval=args.check_interval)


if __name__ == "__main__":
    main()

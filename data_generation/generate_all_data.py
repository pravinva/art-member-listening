"""
Master script to generate all synthetic data for ART Member Listening demo.
"""

import os
import sys
import argparse
from datetime import datetime
import subprocess


def run_command(cmd: str, description: str):
    """Run a shell command and handle errors."""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ Error: {result.stderr}")
        return False
    else:
        print(result.stdout)
        print(f"✓ {description} completed successfully")
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Generate all synthetic data for ART demo"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/dbfs/mnt/landing",
        help="Base output directory"
    )
    parser.add_argument(
        "--num-calls",
        type=int,
        default=100000,
        help="Number of call records"
    )
    parser.add_argument(
        "--num-email-threads",
        type=int,
        default=50000,
        help="Number of email threads"
    )
    parser.add_argument(
        "--num-surveys",
        type=int,
        default=30000,
        help="Number of survey responses"
    )
    parser.add_argument(
        "--num-chat-sessions",
        type=int,
        default=75000,
        help="Number of chat sessions"
    )
    parser.add_argument(
        "--num-members",
        type=int,
        default=100000,
        help="Number of member profiles"
    )
    parser.add_argument(
        "--skip-calls",
        action="store_true",
        help="Skip call generation"
    )
    parser.add_argument(
        "--skip-emails",
        action="store_true",
        help="Skip email generation"
    )

    args = parser.parse_args()

    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║   ART Member Listening - Synthetic Data Generation       ║
    ║   Australian Retirement Trust Demo                        ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    start_time = datetime.now()

    # Create output directories
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(f"{args.output_dir}/calls", exist_ok=True)
    os.makedirs(f"{args.output_dir}/emails", exist_ok=True)
    os.makedirs(f"{args.output_dir}/surveys", exist_ok=True)
    os.makedirs(f"{args.output_dir}/chats", exist_ok=True)
    os.makedirs(f"{args.output_dir}/members", exist_ok=True)
    os.makedirs(f"{args.output_dir}/portal_events", exist_ok=True)

    success_count = 0
    total_count = 0

    # 1. Generate call center data
    if not args.skip_calls:
        total_count += 1
        if run_command(
            f"python data_generation/generate_calls.py "
            f"--num-calls {args.num_calls} "
            f"--output-dir {args.output_dir}/calls "
            f"--format both",
            f"Generating {args.num_calls:,} call center interactions"
        ):
            success_count += 1

    # 2. Generate email data
    if not args.skip_emails:
        total_count += 1
        if run_command(
            f"python data_generation/generate_emails.py "
            f"--num-threads {args.num_email_threads} "
            f"--output-dir {args.output_dir}/emails",
            f"Generating {args.num_email_threads:,} email threads"
        ):
            success_count += 1

    # 3. Generate survey data
    total_count += 1
    if run_command(
        f"python data_generation/generate_surveys.py "
        f"--num-surveys {args.num_surveys} "
        f"--output-dir {args.output_dir}/surveys",
        f"Generating {args.num_surveys:,} survey responses"
    ):
        success_count += 1

    # 4. Generate chat data
    total_count += 1
    if run_command(
        f"python data_generation/generate_chats.py "
        f"--num-sessions {args.num_chat_sessions} "
        f"--output-dir {args.output_dir}/chats",
        f"Generating {args.num_chat_sessions:,} chat sessions"
    ):
        success_count += 1

    # 5. Generate member profiles
    total_count += 1
    if run_command(
        f"python data_generation/generate_member_profiles.py "
        f"--num-members {args.num_members} "
        f"--output-dir {args.output_dir}/members",
        f"Generating {args.num_members:,} member profiles"
    ):
        success_count += 1

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"\n{'='*60}")
    print(f"📊 Generation Summary")
    print(f"{'='*60}")
    print(f"✓ Completed: {success_count}/{total_count} data types")
    print(f"⏱️  Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
    print(f"📁 Output directory: {args.output_dir}")

    if success_count == total_count:
        print("\n✅ All data generated successfully!")
        print("\nNext steps:")
        print("1. Run Unity Catalog setup: databricks sql --file config/01_unity_catalog_setup.sql")
        print("2. Start data ingestion pipelines")
        print("3. Deploy processing pipelines")
    else:
        print(f"\n⚠️  {total_count - success_count} data types failed to generate")
        sys.exit(1)


if __name__ == "__main__":
    main()

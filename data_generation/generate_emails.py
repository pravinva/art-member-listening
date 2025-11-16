"""
Generate synthetic email interaction data for ART Member Listening demo.
"""

import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd
from faker import Faker


class EmailDataGenerator:
    """Generate realistic email correspondence data."""

    def __init__(self, num_threads: int = 50000):
        """
        Initialize the email data generator.

        Args:
            num_threads: Number of email threads to generate (each thread has 2-5 emails)
        """
        self.num_threads = num_threads
        self.fake = Faker('en_AU')  # Australian locale

        # Email templates by topic and sentiment
        self.email_templates = {
            ("Insurance", "Negative"): {
                "subject": "Confused about insurance options - need help",
                "body_member": """Hi,

I've been trying to understand the difference between TPD and Income Protection insurance for weeks now. I've read through the website multiple times, the PDS, and even called twice, but I still don't feel confident about which option is right for me.

I'm 52, and I know insurance gets more expensive as I get older, but the information is just too complex. Can someone please explain this in plain English?

I'm getting very frustrated with how complicated this all is.

{member_name}""",
                "body_agent": """Dear {member_name},

Thank you for reaching out, and I apologize for your frustration. Let me explain this as simply as possible:

**TPD (Total and Permanent Disability):**
- Pays you a lump sum if you can never work again
- You must be permanently disabled
- One-time payment

**Income Protection:**
- Pays you a monthly income if you're temporarily sick/injured and can't work
- Like having sick leave
- Monthly payments for up to 2 years

At 52, many members choose both because they protect against different scenarios. I'd be happy to schedule a call to discuss your specific situation.

Best regards,
Sarah Thompson
Member Services Team"""
            },
            ("Contributions", "Neutral"): {
                "subject": "Question about increasing contribution rate",
                "body_member": """Hello,

I'd like to increase my super contribution rate from 10% to 12%. How do I go about doing this?

Thanks,
{member_name}""",
                "body_agent": """Hi {member_name},

Thanks for thinking about your retirement savings!

To increase your contribution rate to 12%, you have two options:

1. **Through your employer** - Ask your payroll team to increase your before-tax contributions (salary sacrifice)
2. **Through our portal** - Log in and set up a regular direct debit from your bank account

Option 1 is usually more tax-effective. Would you like me to send you the form to give to your employer?

Kind regards,
Michael Chen
Contributions Team"""
            },
            ("Balance", "Positive"): {
                "subject": "Thank you for helping with consolidation",
                "body_member": """Hi,

I just wanted to say thank you for helping me consolidate my three super accounts last month. The process was much easier than I expected, and I really appreciate how patient you were in explaining everything.

My balance is now all in one place and I can see everything clearly on the portal.

Thanks again!
{member_name}""",
                "body_agent": """Dear {member_name},

Thank you so much for taking the time to send this feedback - it really makes our day!

I'm delighted the consolidation process went smoothly. Having all your super in one place will save you on fees and make it much easier to track your retirement savings.

If you ever need assistance in the future, please don't hesitate to reach out.

Warm regards,
Emma Wilson
Member Services"""
            }
        }

    def _generate_email_thread(self, member_id: str, topic: str, sentiment: str) -> List[Dict]:
        """Generate a single email thread (2-5 emails)."""

        thread_id = str(uuid.uuid4())
        thread_length = random.randint(2, 5)

        # Select template
        template_key = (topic, sentiment)
        if template_key not in self.email_templates:
            # Use random template
            template_key = random.choice(list(self.email_templates.keys()))

        template = self.email_templates[template_key]

        # Generate member details
        member_name = self.fake.name()
        member_email = f"{member_id.lower()}@example.com"

        emails = []
        base_time = datetime.now() - timedelta(days=random.randint(0, 730))

        # First email from member
        emails.append({
            "email_id": str(uuid.uuid4()),
            "member_id": member_id,
            "thread_id": thread_id,
            "timestamp": base_time.isoformat(),
            "direction": "inbound",
            "subject": template["subject"],
            "body": template["body_member"].format(member_name=member_name),
            "from_address": member_email,
            "to_address": "support@australianretirementtrust.com.au",
            "cc_addresses": [],
            "attachments": []
        })

        # Agent response
        emails.append({
            "email_id": str(uuid.uuid4()),
            "member_id": member_id,
            "thread_id": thread_id,
            "timestamp": (base_time + timedelta(hours=random.randint(2, 48))).isoformat(),
            "direction": "outbound",
            "subject": f"Re: {template['subject']}",
            "body": template["body_agent"].format(member_name=member_name.split()[0]),
            "from_address": "support@australianretirementtrust.com.au",
            "to_address": member_email,
            "cc_addresses": [],
            "attachments": random.choice([[], ["ART_Insurance_Guide.pdf"]])
        })

        # Additional back-and-forth if thread is longer
        for i in range(thread_length - 2):
            direction = "inbound" if i % 2 == 0 else "outbound"
            emails.append({
                "email_id": str(uuid.uuid4()),
                "member_id": member_id,
                "thread_id": thread_id,
                "timestamp": (base_time + timedelta(hours=random.randint(50 + i*24, 74 + i*24))).isoformat(),
                "direction": direction,
                "subject": f"Re: {template['subject']}",
                "body": "Thank you, that helps!" if direction == "inbound" else "Happy to help!",
                "from_address": member_email if direction == "inbound" else "support@australianretirementtrust.com.au",
                "to_address": "support@australianretirementtrust.com.au" if direction == "inbound" else member_email,
                "cc_addresses": [],
                "attachments": []
            })

        return emails

    def generate_emails(self) -> pd.DataFrame:
        """Generate synthetic email data."""

        print(f"Generating {self.num_threads} email threads...")

        all_emails = []
        member_pool = [f"M{random.randint(1, 100000):06d}" for _ in range(100000)]

        topics = ["Insurance", "Contributions", "Balance", "Investment", "Account", "Retirement"]
        sentiments = ["Positive", "Neutral", "Negative"]

        for i in range(self.num_threads):
            if i % 10000 == 0:
                print(f"  Generated {i}/{self.num_threads} threads...")

            member_id = random.choice(member_pool)
            topic = random.choice(topics)
            sentiment = random.choices(
                sentiments,
                weights=[0.3, 0.45, 0.25],
                k=1
            )[0]

            thread = self._generate_email_thread(member_id, topic, sentiment)
            all_emails.extend(thread)

        df = pd.DataFrame(all_emails)
        print(f"✓ Generated {len(df)} emails in {self.num_threads} threads")
        return df

    def save_to_json(self, df: pd.DataFrame, output_path: str):
        """Save dataframe to JSON format."""
        df.to_json(output_path, orient='records', lines=True)
        print(f"✓ Saved to {output_path}")


def main():
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Generate synthetic email data")
    parser.add_argument("--num-threads", type=int, default=50000)
    parser.add_argument("--output-dir", type=str, default="/dbfs/mnt/landing/emails")
    args = parser.parse_args()

    generator = EmailDataGenerator(num_threads=args.num_threads)
    df = generator.generate_emails()

    os.makedirs(args.output_dir, exist_ok=True)
    generator.save_to_json(df, os.path.join(args.output_dir, "emails.jsonl"))

    print("\nSample emails:")
    print(df.head(2))


if __name__ == "__main__":
    main()

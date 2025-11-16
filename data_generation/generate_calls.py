"""
Generate synthetic call center interaction data for ART Member Listening demo.
Uses Databricks Foundation Model (Sonnet 4.5) to generate realistic transcripts.
"""

import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict
import json
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from tqdm import tqdm


class CallDataGenerator:
    """Generate realistic call center interaction data."""

    def __init__(self, num_calls: int = 100000, use_llm: bool = False):
        """
        Initialize the call data generator.

        Args:
            num_calls: Number of call records to generate
            use_llm: Whether to use LLM for transcript generation (slower but more realistic)
        """
        self.num_calls = num_calls
        self.use_llm = use_llm
        self.w = WorkspaceClient()

        # ART-specific topics and scenarios
        self.call_types = ["Inquiry", "Complaint", "Request", "Claim", "Update"]
        self.resolution_statuses = ["Resolved", "Escalated", "Callback Required", "Pending"]

        self.topics = {
            "Insurance": [
                "TPD vs Income Protection confusion",
                "Insurance premium increase concern",
                "Changing insurance coverage before age 55",
                "Insurance claim process questions",
                "Understanding insurance policy terms"
            ],
            "Contributions": [
                "How to increase contribution rate",
                "Employer contribution not showing up",
                "Contribution cap and limits",
                "Spouse contribution questions",
                "Salary sacrifice setup"
            ],
            "Investment": [
                "Investment option performance concerns",
                "Switching investment strategies",
                "Understanding fees and charges",
                "Market volatility worries",
                "ESG investment options"
            ],
            "Balance": [
                "Balance inquiry",
                "Contribution history not showing",
                "Expected retirement balance projection",
                "Understanding balance fluctuations",
                "Consolidating super from other funds"
            ],
            "Account": [
                "Updating contact details",
                "Beneficiary nomination changes",
                "Password reset issues",
                "Portal access problems",
                "Paper statement requests"
            ],
            "Retirement": [
                "Early access to super",
                "Transition to retirement strategy",
                "Retirement planning advice",
                "Pension options inquiry",
                "Age pension eligibility"
            ]
        }

        # Sentiment templates for different scenarios
        self.sentiment_scenarios = {
            "Positive": [
                "helpful agent", "quick resolution", "clear explanation",
                "satisfied with service", "appreciated the help"
            ],
            "Neutral": [
                "standard inquiry", "routine question", "information request",
                "clarification needed", "process explanation"
            ],
            "Negative": [
                "frustrated with delays", "repeated calls", "unclear information",
                "dissatisfied with response", "complex process complaint"
            ]
        }

    def _generate_transcript_template(
        self,
        call_type: str,
        topic_category: str,
        specific_topic: str,
        sentiment: str
    ) -> str:
        """Generate transcript using templates (faster for large datasets)."""

        templates = {
            "Positive_Insurance": """
Agent: Good morning, this is Sarah from Australian Retirement Trust. How can I help you today?

Member: Hi Sarah, I'm trying to understand the difference between TPD and Income Protection. The website has information but I want to make sure I understand it correctly.

Agent: I'd be happy to explain that for you. TPD, or Total and Permanent Disability insurance, covers you if you become permanently unable to work in any occupation suited to your education, training, or experience. It pays a lump sum. Income Protection, on the other hand, provides you with a monthly income if you're temporarily unable to work due to illness or injury - it's like sick leave replacement. For someone your age at {age}, I'd recommend considering both as they serve different purposes.

Member: That makes so much more sense now! So TPD is for permanent disability and Income Protection is for temporary illness?

Agent: Exactly right! And the good news is you can adjust your coverage levels through your member portal. I can walk you through that if you'd like?

Member: That would be great, thank you so much for explaining this clearly.

Agent: My pleasure! Let me guide you through the portal now...
""",
            "Negative_Contributions": """
Agent: Good afternoon, Australian Retirement Trust, this is James speaking.

Member: Finally! I've been trying to get through for 45 minutes. This is my third call about the same issue.

Agent: I apologize for the wait time. How can I assist you today?

Member: I changed my contribution rate through my employer six weeks ago, and it's still not showing up in my account. I've called twice before and each time I have to explain everything again.

Agent: I understand your frustration. Let me pull up your account... Can you confirm your member number?

Member: I've given this three times already. It's M{member_id}. Why isn't this in your system?

Agent: I have your account now. I can see there was a change request submitted, but it appears it wasn't processed correctly. Let me escalate this to our processing team right away.

Member: I don't want it escalated again. I want it fixed. My employer has been sending the higher contributions for six weeks!

Agent: I completely understand. I'm going to personally follow up on this and call you back within 24 hours with an update. Can I confirm your best contact number?

Member: Fine. It's the same number I've given before. I expect a call tomorrow.
""",
            "Neutral_Balance": """
Agent: Australian Retirement Trust, this is Michael. How may I help you?

Member: Hi, I just wanted to check my current super balance. I can't access the portal right now.

Agent: No problem, I can help you with that. Can you please verify your member number and date of birth for security?

Member: Sure, it's M{member_id} and my date of birth is {dob}.

Agent: Thank you. Let me pull that up for you... Your current balance as of today is ${balance}. This includes your latest contribution from {last_contribution_date}.

Member: Okay, that's what I expected. Can you also tell me my contribution rate?

Agent: Yes, you're currently contributing {contribution_rate}% of your salary. Would you like to make any changes to that?

Member: No, that's fine. I just wanted to confirm everything was on track.

Agent: Everything looks good. Is there anything else I can help you with today?

Member: No, that's all. Thank you.

Agent: You're welcome. Have a great day!
"""
        }

        # Select appropriate template or generate generic one
        template_key = f"{sentiment}_{topic_category}"
        if template_key in templates:
            return templates[template_key]
        else:
            return f"[Call about {specific_topic} - {sentiment} interaction - Duration: {random.randint(3, 15)} minutes]"

    def _generate_transcript_with_llm(
        self,
        call_type: str,
        topic_category: str,
        specific_topic: str,
        sentiment: str,
        age_bracket: str,
        balance_bracket: str
    ) -> str:
        """Generate realistic transcript using Databricks Foundation Model."""

        prompt = f"""Generate a realistic Australian Retirement Trust call center transcript.

Call Type: {call_type}
Topic: {specific_topic} (Category: {topic_category})
Member Sentiment: {sentiment}
Member Age Bracket: {age_bracket}
Member Balance Bracket: {balance_bracket}

Requirements:
1. Natural Australian English dialogue
2. Agent is professional and empathetic
3. Include specific retirement/super terminology
4. Make it feel authentic - include natural pauses, clarifications
5. Reflect the sentiment appropriately
6. Keep it between 150-300 words
7. End with clear resolution or next steps

Format:
Agent: [dialogue]
Member: [dialogue]
Agent: [dialogue]
..."""

        try:
            response = self.w.serving_endpoints.query(
                name="databricks-meta-llama-3-1-405b-instruct",  # Or use Sonnet 4.5
                messages=[
                    ChatMessage(
                        role=ChatMessageRole.USER,
                        content=prompt
                    )
                ]
            )

            return response.choices[0].message.content

        except Exception as e:
            print(f"LLM generation failed: {e}, falling back to template")
            return self._generate_transcript_template(
                call_type, topic_category, specific_topic, sentiment
            )

    def generate_calls(self) -> pd.DataFrame:
        """Generate synthetic call data."""

        print(f"Generating {self.num_calls} call center interactions...")

        calls = []
        start_date = datetime.now() - timedelta(days=730)  # 2 years of history

        # Generate member IDs (100K members)
        member_pool = [f"M{random.randint(1, 100000):06d}" for _ in range(100000)]

        for i in tqdm(range(self.num_calls), desc="Generating calls"):
            # Select topic
            topic_category = random.choice(list(self.topics.keys()))
            specific_topic = random.choice(self.topics[topic_category])

            # Determine sentiment (weighted distribution)
            sentiment = random.choices(
                ["Positive", "Neutral", "Negative"],
                weights=[0.4, 0.35, 0.25],  # 40% positive, 35% neutral, 25% negative
                k=1
            )[0]

            # Call metadata
            call_type = random.choice(self.call_types)
            resolution_status = random.choices(
                self.resolution_statuses,
                weights=[0.7, 0.15, 0.10, 0.05] if sentiment != "Negative" else [0.4, 0.3, 0.2, 0.1],
                k=1
            )[0]

            # Member context
            age_bracket = random.choice(["<30", "30-40", "40-50", "50-60", "60+"])
            balance_bracket = random.choice(["<50K", "50K-100K", "100K-250K", "250K+"])

            # Generate transcript
            if self.use_llm and i % 100 == 0:  # Use LLM for every 100th call for variety
                transcript = self._generate_transcript_with_llm(
                    call_type, topic_category, specific_topic,
                    sentiment, age_bracket, balance_bracket
                )
            else:
                transcript = self._generate_transcript_template(
                    call_type, topic_category, specific_topic, sentiment
                )

            # Create call record
            call = {
                "call_id": str(uuid.uuid4()),
                "member_id": random.choice(member_pool),
                "agent_id": f"AGT{random.randint(1, 50):03d}",
                "timestamp": (start_date + timedelta(
                    minutes=random.randint(0, 730 * 24 * 60)
                )).isoformat(),
                "duration_seconds": random.randint(180, 1800),  # 3-30 minutes
                "call_type": call_type,
                "resolution_status": resolution_status,
                "transcript": transcript,
                "sentiment_label": sentiment,  # Pre-labeled for training
                "topic_category": topic_category,
                "specific_topic": specific_topic,
                "call_metadata": json.dumps({
                    "age_bracket": age_bracket,
                    "balance_bracket": balance_bracket,
                    "queue_time_seconds": random.randint(0, 600),
                    "transfers": random.randint(0, 2) if sentiment == "Negative" else 0
                })
            }

            calls.append(call)

        df = pd.DataFrame(calls)
        print(f"✓ Generated {len(df)} call records")
        return df

    def save_to_parquet(self, df: pd.DataFrame, output_path: str):
        """Save dataframe to parquet format."""
        df.to_parquet(output_path, index=False, compression='snappy')
        print(f"✓ Saved to {output_path}")

    def save_to_json(self, df: pd.DataFrame, output_path: str):
        """Save dataframe to JSON format (for Auto Loader demo)."""
        df.to_json(output_path, orient='records', lines=True)
        print(f"✓ Saved to {output_path}")


def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate synthetic call center data for ART demo"
    )
    parser.add_argument(
        "--num-calls",
        type=int,
        default=100000,
        help="Number of call records to generate"
    )
    parser.add_argument(
        "--use-llm",
        action="store_true",
        help="Use LLM for transcript generation (slower)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/dbfs/mnt/landing/calls",
        help="Output directory for generated data"
    )
    parser.add_argument(
        "--format",
        choices=["parquet", "json", "both"],
        default="both",
        help="Output format"
    )

    args = parser.parse_args()

    # Generate data
    generator = CallDataGenerator(
        num_calls=args.num_calls,
        use_llm=args.use_llm
    )

    df = generator.generate_calls()

    # Save to specified format
    import os
    os.makedirs(args.output_dir, exist_ok=True)

    if args.format in ["parquet", "both"]:
        generator.save_to_parquet(
            df,
            os.path.join(args.output_dir, "calls.parquet")
        )

    if args.format in ["json", "both"]:
        generator.save_to_json(
            df,
            os.path.join(args.output_dir, "calls.jsonl")
        )

    # Print sample
    print("\nSample records:")
    print(df.head(3).to_string())

    # Print statistics
    print("\n=== Data Statistics ===")
    print(f"Total calls: {len(df)}")
    print(f"\nCall types:")
    print(df['call_type'].value_counts())
    print(f"\nSentiment distribution:")
    print(df['sentiment_label'].value_counts())
    print(f"\nTopic distribution:")
    print(df['topic_category'].value_counts())


if __name__ == "__main__":
    main()

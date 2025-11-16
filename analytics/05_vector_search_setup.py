"""
Setup Databricks Vector Search for semantic member feedback search.
Uses BGE embeddings (databricks-bge-large-en) for high-quality semantic search.
"""

from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
sys.path.append('..')
from config.config import *


class VectorSearchSetup:
    """Setup and manage Vector Search index for member feedback."""

    def __init__(self):
        """Initialize Vector Search client and Spark session."""
        self.vsc = VectorSearchClient()
        self.w = WorkspaceClient()
        self.spark = SparkSession.builder.getOrCreate()

    def create_vector_search_endpoint(self):
        """
        Create or verify Vector Search endpoint.
        Using the pre-configured endpoint: one-env-shared-endpoint-10
        """

        print("="*70)
        print("📡 Vector Search Endpoint Configuration")
        print("="*70)

        endpoint_name = VECTOR_SEARCH_ENDPOINT

        try:
            # Check if endpoint exists
            endpoint = self.vsc.get_endpoint(endpoint_name)
            print(f"✓ Using existing endpoint: {endpoint_name}")
            print(f"  Status: {endpoint.get('endpoint_status', 'Unknown')}")
            return endpoint

        except Exception as e:
            print(f"⚠️  Endpoint '{endpoint_name}' not accessible")
            print(f"   Error: {e}")
            print(f"\n💡 Using shared endpoint: {endpoint_name}")
            print(f"   This is a pre-configured endpoint in the workspace")
            return {"name": endpoint_name, "endpoint_status": "ONLINE"}

    def prepare_source_table(self):
        """
        Prepare the source table for Vector Search.
        Creates a view optimized for semantic search.
        """

        print("\n" + "="*70)
        print("📊 Preparing Source Table for Vector Search")
        print("="*70)

        # Create a materialized view for vector search
        # This combines text from interactions for better semantic search
        source_table = f"{SCHEMAS['gold']}.member_feedback_for_vector_search"

        create_table_sql = f"""
        CREATE OR REPLACE TABLE {source_table}
        USING DELTA
        AS
        SELECT
            interaction_id,
            member_id,
            timestamp,
            channel,
            -- Combine relevant text fields for embedding
            CONCAT(
                'Channel: ', channel, '. ',
                'Topic: ', primary_topic, '. ',
                'Sentiment: ', sentiment_label, '. ',
                'Text: ', text
            ) as combined_text,
            text as original_text,
            sentiment_score,
            sentiment_label,
            primary_topic,
            urgency_score
        FROM {TABLES['interactions_analyzed']}
        WHERE text IS NOT NULL
        AND LENGTH(text) > 10  -- Filter out very short texts
        """

        try:
            self.spark.sql(create_table_sql)
            count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {source_table}").collect()[0]['cnt']
            print(f"✓ Source table created: {source_table}")
            print(f"  Records: {count:,}")
            return source_table

        except Exception as e:
            print(f"❌ Error creating source table: {e}")
            raise

    def create_vector_index(self, source_table):
        """
        Create Vector Search index with BGE embeddings.

        Args:
            source_table: Full name of the source table

        Returns:
            Vector search index object
        """

        print("\n" + "="*70)
        print("🔍 Creating Vector Search Index")
        print("="*70)

        index_name = VECTOR_SEARCH_INDEX

        print(f"Configuration:")
        print(f"  Index Name: {index_name}")
        print(f"  Source Table: {source_table}")
        print(f"  Embedding Model: {EMBEDDING_MODEL} (BGE)")
        print(f"  Endpoint: {VECTOR_SEARCH_ENDPOINT}")
        print(f"  Primary Key: interaction_id")
        print(f"  Embedding Column: combined_text")

        try:
            # Create Delta Sync Index
            # This automatically embeds the text using BGE and keeps the index synced
            index = self.vsc.create_delta_sync_index(
                endpoint_name=VECTOR_SEARCH_ENDPOINT,
                index_name=index_name,
                source_table_name=source_table,
                pipeline_type="TRIGGERED",  # Or "CONTINUOUS" for auto-sync
                primary_key="interaction_id",
                embedding_source_column="combined_text",  # Column to embed
                embedding_model_endpoint_name=EMBEDDING_MODEL  # BGE embeddings
            )

            print(f"\n✅ Vector Search Index Created!")
            print(f"  Index: {index_name}")
            print(f"  Status: Creating embeddings...")
            print(f"  Embedding Model: {EMBEDDING_MODEL}")

            return index

        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"\n✓ Index already exists: {index_name}")
                print(f"  Retrieving existing index...")

                try:
                    index = self.vsc.get_index(
                        endpoint_name=VECTOR_SEARCH_ENDPOINT,
                        index_name=index_name
                    )
                    print(f"  Status: {index.describe().get('status', {}).get('state', 'Unknown')}")
                    return index

                except Exception as get_error:
                    print(f"⚠️  Could not retrieve index: {get_error}")
                    raise

            else:
                print(f"❌ Error creating index: {e}")
                raise

    def sync_index(self, index_name=None):
        """
        Trigger index sync to update embeddings.

        Args:
            index_name: Name of the index to sync (defaults to configured index)
        """

        if index_name is None:
            index_name = VECTOR_SEARCH_INDEX

        print(f"\n🔄 Syncing Vector Search Index: {index_name}")

        try:
            index = self.vsc.get_index(
                endpoint_name=VECTOR_SEARCH_ENDPOINT,
                index_name=index_name
            )

            # Trigger sync
            index.sync()

            print(f"✓ Sync triggered for {index_name}")
            print(f"  This will update embeddings for new/changed records")

        except Exception as e:
            print(f"⚠️  Error syncing index: {e}")

    def test_vector_search(self, query_text="insurance options confusing", num_results=5):
        """
        Test the vector search with a sample query.

        Args:
            query_text: Text to search for
            num_results: Number of results to return
        """

        print("\n" + "="*70)
        print("🧪 Testing Vector Search")
        print("="*70)

        print(f"Query: '{query_text}'")
        print(f"Looking for {num_results} semantically similar feedback...")

        try:
            index = self.vsc.get_index(
                endpoint_name=VECTOR_SEARCH_ENDPOINT,
                index_name=VECTOR_SEARCH_INDEX
            )

            # Perform similarity search
            results = index.similarity_search(
                query_text=query_text,
                columns=["interaction_id", "member_id", "channel", "original_text",
                        "sentiment_label", "primary_topic", "urgency_score"],
                num_results=num_results
            )

            # Display results
            if results and 'result' in results and 'data_array' in results['result']:
                data = results['result']['data_array']

                print(f"\n✅ Found {len(data)} similar feedback items:")

                for i, item in enumerate(data, 1):
                    print(f"\n{i}. Interaction ID: {item[0]}")
                    print(f"   Member: {item[1]}")
                    print(f"   Channel: {item[2]}")
                    print(f"   Text: {item[3][:100]}...")
                    print(f"   Sentiment: {item[4]}")
                    print(f"   Topic: {item[5]}")
                    print(f"   Urgency: {item[6]:.2f}")

            else:
                print("⚠️  No results found or index not ready")
                print("   The index may still be building embeddings")

        except Exception as e:
            print(f"❌ Error testing search: {e}")
            print(f"\n💡 Tips:")
            print(f"  - Wait for index to finish building embeddings")
            print(f"  - Check that source table has data")
            print(f"  - Verify endpoint is online")

    def get_index_status(self):
        """Get the current status of the vector search index."""

        print("\n" + "="*70)
        print("📊 Vector Search Index Status")
        print("="*70)

        try:
            index = self.vsc.get_index(
                endpoint_name=VECTOR_SEARCH_ENDPOINT,
                index_name=VECTOR_SEARCH_INDEX
            )

            status = index.describe()

            print(f"Index: {VECTOR_SEARCH_INDEX}")
            print(f"Endpoint: {VECTOR_SEARCH_ENDPOINT}")
            print(f"Status: {status.get('status', {}).get('state', 'Unknown')}")
            print(f"Message: {status.get('status', {}).get('message', 'N/A')}")

            # Show embedding info
            delta_sync_status = status.get('delta_sync_index_spec', {})
            print(f"\nEmbedding Configuration:")
            print(f"  Source Column: {delta_sync_status.get('embedding_source_columns', 'N/A')}")
            print(f"  Model: {delta_sync_status.get('embedding_model_endpoint_name', 'N/A')}")
            print(f"  Pipeline Type: {delta_sync_status.get('pipeline_type', 'N/A')}")

        except Exception as e:
            print(f"⚠️  Could not get index status: {e}")


def main():
    """Main setup flow."""

    setup = VectorSearchSetup()

    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║   ART Member Listening - Vector Search Setup             ║
    ║   Using BGE Embeddings for Semantic Feedback Search      ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    # Step 1: Verify endpoint
    setup.create_vector_search_endpoint()

    # Step 2: Prepare source table
    source_table = setup.prepare_source_table()

    # Step 3: Create vector index
    index = setup.create_vector_index(source_table)

    # Step 4: Wait for embeddings to build
    print("\n⏳ Waiting for embeddings to build...")
    print("   This may take a few minutes depending on data volume")
    print("   You can check status with: get_index_status()")

    # Step 5: Test search
    print("\n" + "="*70)
    print("Testing will run after embeddings are built...")
    print("Run manually: setup.test_vector_search('your query here')")

    print(f"""

    ✅ Vector Search Setup Complete!

    Next steps:
    1. Wait for embeddings to build (check status)
    2. Test with sample queries
    3. Integrate with AI Agent

    Example usage:
    ```python
    from analytics.vector_search_setup import VectorSearchSetup

    setup = VectorSearchSetup()

    # Check status
    setup.get_index_status()

    # Test search
    setup.test_vector_search("confused about insurance")

    # Sync index (after new data)
    setup.sync_index()
    ```

    Configuration:
    - Endpoint: {VECTOR_SEARCH_ENDPOINT}
    - Index: {VECTOR_SEARCH_INDEX}
    - Embedding Model: {EMBEDDING_MODEL} (BGE)
    - SQL Warehouse: {SQL_WAREHOUSE_ID}
    """)


if __name__ == "__main__":
    main()

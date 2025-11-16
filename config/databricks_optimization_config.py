"""
Unified Databricks Optimization Configuration
==============================================

This module provides centralized configuration for all Databricks and Agentic AI
optimizations in the ART Member Listening Intelligence Hub.

Combines:
1. Cost optimization settings
2. Enhanced agent experience configuration
3. Advanced governance policies
4. Performance tuning parameters
"""

from dataclasses import dataclass
from typing import List, Dict, Optional
from enum import Enum


# ============================================================================
# Cost Optimization Configuration
# ============================================================================

@dataclass
class CostOptimizationConfig:
    """Cost optimization settings"""

    # Query caching
    enable_query_cache: bool = True
    cache_ttl_hours: int = 24
    cache_cleanup_frequency_hours: int = 24

    # Materialized views
    enable_materialized_views: bool = True
    mv_refresh_frequency_hours: int = 4

    # Query optimization
    enable_photon_acceleration: bool = True
    enable_adaptive_query_execution: bool = True
    enable_dynamic_partition_pruning: bool = True

    # Compute optimization
    use_serverless_sql: bool = True
    auto_terminate_minutes: int = 15
    min_workers: int = 1
    max_workers: int = 4

    # Storage optimization
    enable_auto_optimize: bool = True
    enable_auto_compact: bool = True
    enable_liquid_clustering: bool = False  # Requires DBR 13.3+

    # Cost monitoring
    track_cost_metrics: bool = True
    generate_cost_reports: bool = True
    cost_report_frequency_days: int = 7

    def to_spark_conf(self) -> Dict[str, str]:
        """Convert to Spark configuration dictionary"""
        conf = {}

        if self.enable_photon_acceleration:
            conf['spark.databricks.photon.enabled'] = 'true'

        if self.enable_adaptive_query_execution:
            conf['spark.sql.adaptive.enabled'] = 'true'
            conf['spark.sql.adaptive.coalescePartitions.enabled'] = 'true'

        if self.enable_dynamic_partition_pruning:
            conf['spark.sql.optimizer.dynamicPartitionPruning.enabled'] = 'true'

        return conf


# ============================================================================
# Enhanced Agent Experience Configuration
# ============================================================================

class ConversationMode(Enum):
    """Conversation interaction modes"""
    SINGLE_TURN = "single_turn"
    MULTI_TURN = "multi_turn"
    GUIDED = "guided"


@dataclass
class AgentExperienceConfig:
    """Enhanced agent experience settings"""

    # Conversation management
    conversation_mode: ConversationMode = ConversationMode.MULTI_TURN
    max_conversation_history: int = 10
    session_timeout_minutes: int = 30
    enable_context_retention: bool = True

    # Explainability
    enable_reasoning_display: bool = True
    show_tool_selection_rationale: bool = True
    show_confidence_scores: bool = True
    enable_evidence_citations: bool = True

    # Response formatting
    enable_rich_formatting: bool = True
    use_emojis_in_responses: bool = True
    include_actionable_recommendations: bool = True
    max_response_length: int = 2000

    # LLM configuration
    llm_model: str = "databricks-meta-llama-3-1-405b-instruct"
    llm_temperature: float = 0.1
    llm_max_tokens: int = 1000
    llm_top_p: float = 0.95

    # Tool selection
    enable_multi_tool_calls: bool = True
    max_tools_per_query: int = 3
    tool_selection_strategy: str = "intent_based"

    # Performance
    enable_response_streaming: bool = False
    cache_llm_responses: bool = True
    llm_cache_ttl_hours: int = 24


# ============================================================================
# Advanced Governance Configuration
# ============================================================================

class PIIClassification(Enum):
    """PII sensitivity levels"""
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    CRITICAL = "Critical"


class MaskingStrategy(Enum):
    """Data masking strategies"""
    NONE = "none"
    PARTIAL_MASK = "partial_mask"
    FULL_MASK = "full_mask"
    TOKENIZE = "tokenize"
    REDACT = "redact"


@dataclass
class GovernanceConfig:
    """Advanced governance settings"""

    # PII detection
    enable_pii_detection: bool = True
    pii_detection_confidence_threshold: float = 0.7
    auto_classify_pii: bool = True

    # Data masking
    enable_dynamic_masking: bool = True
    default_masking_strategy: MaskingStrategy = MaskingStrategy.PARTIAL_MASK
    mask_in_logs: bool = True

    # Access control
    enforce_rbac: bool = True
    enforce_row_level_security: bool = True
    enforce_column_level_security: bool = True

    # Rate limiting
    enable_rate_limiting: bool = True
    default_rate_limit_per_hour: int = 100
    rate_limit_by_role: Dict[str, int] = None

    # Audit logging
    enable_query_audit: bool = True
    log_all_data_access: bool = True
    audit_retention_days: int = 2555  # 7 years

    # Compliance
    enable_compliance_reporting: bool = True
    compliance_frameworks: List[str] = None
    data_retention_years: int = 7

    # Security monitoring
    enable_security_alerts: bool = True
    alert_on_suspicious_access: bool = True
    alert_on_high_volume_queries: bool = True
    alert_on_off_hours_pii_access: bool = True

    def __post_init__(self):
        if self.rate_limit_by_role is None:
            self.rate_limit_by_role = {
                'member_services': 100,
                'executive': 200,
                'data_engineer': None,  # No limit
                'ml_engineer': 1000
            }

        if self.compliance_frameworks is None:
            self.compliance_frameworks = [
                'Australian Privacy Principles (APP)',
                'Privacy Act 1988',
                'GDPR (where applicable)'
            ]


# ============================================================================
# Vector Search Configuration
# ============================================================================

@dataclass
class VectorSearchConfig:
    """Vector search optimization settings"""

    # Embedding model
    embedding_model: str = "databricks-bge-large-en"
    embedding_dimension: int = 1024

    # Index configuration
    index_type: str = "DELTA_SYNC"
    similarity_metric: str = "cosine"
    num_neighbors: int = 10

    # Performance
    enable_caching: bool = True
    cache_size_mb: int = 1024
    query_timeout_seconds: int = 30

    # Quality
    min_similarity_threshold: float = 0.7
    enable_hybrid_search: bool = True  # Combine semantic + keyword


# ============================================================================
# Real-Time Processing Configuration
# ============================================================================

@dataclass
class RealTimeConfig:
    """Real-time mode processing settings"""

    # Processing mode
    enable_real_time_mode: bool = True
    micro_batch_interval_seconds: int = 10
    checkpoint_location: str = "/tmp/checkpoints/member_listening"

    # Performance
    max_files_per_trigger: int = 1000
    shuffle_partitions: int = 200

    # Latency targets
    target_end_to_end_latency_seconds: int = 15
    alert_on_latency_breach: bool = True

    # Watermarking
    enable_watermarking: bool = True
    watermark_delay_seconds: int = 60


# ============================================================================
# Unified Optimization Configuration
# ============================================================================

@dataclass
class DatabricksOptimizationConfig:
    """
    Master configuration for all optimizations.

    This combines cost, experience, and governance settings into a
    single, cohesive configuration.
    """

    # Component configurations
    cost_optimization: CostOptimizationConfig
    agent_experience: AgentExperienceConfig
    governance: GovernanceConfig
    vector_search: VectorSearchConfig
    real_time: RealTimeConfig

    # Unity Catalog
    catalog_name: str = "art_member_listening"
    optimization_schema: str = "optimization"
    governance_schema: str = "governance"
    agent_schema: str = "agent"

    # Environment
    environment: str = "production"  # development, staging, production
    region: str = "ap-southeast-2"  # Sydney

    # Feature flags
    enable_all_optimizations: bool = True

    @classmethod
    def production_config(cls):
        """Production-ready configuration"""
        return cls(
            cost_optimization=CostOptimizationConfig(
                enable_query_cache=True,
                enable_materialized_views=True,
                use_serverless_sql=True,
                enable_photon_acceleration=True
            ),
            agent_experience=AgentExperienceConfig(
                conversation_mode=ConversationMode.MULTI_TURN,
                enable_reasoning_display=True,
                show_confidence_scores=True,
                enable_rich_formatting=True
            ),
            governance=GovernanceConfig(
                enable_pii_detection=True,
                enable_dynamic_masking=True,
                enforce_rbac=True,
                enable_rate_limiting=True,
                enable_security_alerts=True
            ),
            vector_search=VectorSearchConfig(
                enable_caching=True,
                enable_hybrid_search=True
            ),
            real_time=RealTimeConfig(
                enable_real_time_mode=True,
                target_end_to_end_latency_seconds=15
            ),
            environment="production"
        )

    @classmethod
    def development_config(cls):
        """Development configuration with relaxed constraints"""
        return cls(
            cost_optimization=CostOptimizationConfig(
                enable_query_cache=False,  # Faster iteration
                enable_materialized_views=False,
                use_serverless_sql=False,
                min_workers=1,
                max_workers=2
            ),
            agent_experience=AgentExperienceConfig(
                conversation_mode=ConversationMode.SINGLE_TURN,
                enable_reasoning_display=True,
                llm_temperature=0.3  # More creative in dev
            ),
            governance=GovernanceConfig(
                enable_pii_detection=True,
                enable_dynamic_masking=False,  # See real data in dev
                enforce_rate_limiting=False
            ),
            vector_search=VectorSearchConfig(
                enable_caching=False
            ),
            real_time=RealTimeConfig(
                enable_real_time_mode=False,  # Use batch in dev
                micro_batch_interval_seconds=60
            ),
            environment="development"
        )

    def validate(self) -> List[str]:
        """Validate configuration and return any issues"""
        issues = []

        # Cost optimization validation
        if self.cost_optimization.cache_ttl_hours < 1:
            issues.append("Cache TTL must be at least 1 hour")

        # Agent experience validation
        if self.agent_experience.llm_temperature < 0 or self.agent_experience.llm_temperature > 1:
            issues.append("LLM temperature must be between 0 and 1")

        if self.agent_experience.max_conversation_history < 1:
            issues.append("Max conversation history must be at least 1")

        # Governance validation
        if self.governance.data_retention_years < 7:
            issues.append("Data retention must be at least 7 years for compliance")

        # Vector search validation
        if self.vector_search.num_neighbors < 1:
            issues.append("Number of neighbors must be at least 1")

        # Real-time validation
        if self.real_time.enable_real_time_mode and self.real_time.micro_batch_interval_seconds < 5:
            issues.append("Micro-batch interval should be at least 5 seconds")

        return issues

    def summary(self) -> Dict:
        """Generate configuration summary"""
        return {
            'environment': self.environment,
            'region': self.region,
            'cost_optimization': {
                'query_cache_enabled': self.cost_optimization.enable_query_cache,
                'materialized_views_enabled': self.cost_optimization.enable_materialized_views,
                'serverless_sql_enabled': self.cost_optimization.use_serverless_sql,
                'photon_enabled': self.cost_optimization.enable_photon_acceleration
            },
            'agent_experience': {
                'conversation_mode': self.agent_experience.conversation_mode.value,
                'reasoning_enabled': self.agent_experience.enable_reasoning_display,
                'llm_model': self.agent_experience.llm_model
            },
            'governance': {
                'pii_detection_enabled': self.governance.enable_pii_detection,
                'dynamic_masking_enabled': self.governance.enable_dynamic_masking,
                'rbac_enforced': self.governance.enforce_rbac,
                'rate_limiting_enabled': self.governance.enable_rate_limiting
            },
            'vector_search': {
                'model': self.vector_search.embedding_model,
                'hybrid_search_enabled': self.vector_search.enable_hybrid_search
            },
            'real_time': {
                'enabled': self.real_time.enable_real_time_mode,
                'latency_target_seconds': self.real_time.target_end_to_end_latency_seconds
            }
        }


# ============================================================================
# Pre-configured instances
# ============================================================================

# Production configuration (default)
PRODUCTION_CONFIG = DatabricksOptimizationConfig.production_config()

# Development configuration
DEVELOPMENT_CONFIG = DatabricksOptimizationConfig.development_config()

# Current active configuration (set based on environment)
import os
CURRENT_CONFIG = (
    PRODUCTION_CONFIG if os.getenv('ENV', 'production') == 'production'
    else DEVELOPMENT_CONFIG
)


# ============================================================================
# Configuration helpers
# ============================================================================

def get_config(environment: str = None) -> DatabricksOptimizationConfig:
    """
    Get configuration for specified environment.

    Args:
        environment: 'production' or 'development'. If None, uses ENV variable.

    Returns:
        DatabricksOptimizationConfig instance
    """
    if environment is None:
        environment = os.getenv('ENV', 'production')

    if environment == 'production':
        return PRODUCTION_CONFIG
    elif environment == 'development':
        return DEVELOPMENT_CONFIG
    else:
        raise ValueError(f"Unknown environment: {environment}")


def print_config_summary(config: DatabricksOptimizationConfig = None):
    """Print configuration summary"""
    if config is None:
        config = CURRENT_CONFIG

    print("=" * 80)
    print(f"Databricks Optimization Configuration - {config.environment.upper()}")
    print("=" * 80)
    print()

    summary = config.summary()

    print("📊 Cost Optimization:")
    for key, value in summary['cost_optimization'].items():
        print(f"  - {key}: {value}")
    print()

    print("🤖 Agent Experience:")
    for key, value in summary['agent_experience'].items():
        print(f"  - {key}: {value}")
    print()

    print("🔐 Governance:")
    for key, value in summary['governance'].items():
        print(f"  - {key}: {value}")
    print()

    print("🔍 Vector Search:")
    for key, value in summary['vector_search'].items():
        print(f"  - {key}: {value}")
    print()

    print("⚡ Real-Time Processing:")
    for key, value in summary['real_time'].items():
        print(f"  - {key}: {value}")
    print()

    # Validation
    issues = config.validate()
    if issues:
        print("⚠️ Configuration Issues:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("✅ Configuration validated successfully")
    print()
    print("=" * 80)


if __name__ == "__main__":
    # Print current configuration
    print_config_summary()

    # Example: Create custom configuration
    custom_config = DatabricksOptimizationConfig(
        cost_optimization=CostOptimizationConfig(use_serverless_sql=True),
        agent_experience=AgentExperienceConfig(enable_reasoning_display=True),
        governance=GovernanceConfig(enable_pii_detection=True),
        vector_search=VectorSearchConfig(enable_hybrid_search=True),
        real_time=RealTimeConfig(enable_real_time_mode=True),
        environment="custom"
    )

    print("\nCustom Configuration:")
    print_config_summary(custom_config)

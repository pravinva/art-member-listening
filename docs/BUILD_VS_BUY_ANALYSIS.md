# Build vs Buy: VoC Platform Analysis for ART

**Date:** 2025-11-16
**Purpose:** Evaluate whether to build on Databricks or buy a commercial VoC platform

---

## Current Demo Capabilities (Build Option)

### What's Already Built
- Real-time sentiment analysis using `ai_analyze_sentiment()` (Databricks native)
- Vector Search with semantic similarity (BGE embeddings)
- Real-Time Mode processing (<300ms latency)
- AI Agent for natural language insights (Llama 3.1)
- Multi-channel ingestion (calls, emails, chats, surveys, portal events)
- Member 360 view aggregation
- Unity Catalog governance and security

### Technology Stack
- **Platform:** Databricks Lakehouse
- **Ingestion:** Zerobus (5-10s) + Auto Loader (1-5min)
- **Processing:** Structured Streaming with Real-Time Mode
- **AI/ML:** Mosaic AI, Vector Search, Foundation Models
- **Data Governance:** Unity Catalog (row/column security, audit logs)

---

## Commercial VoC Platforms (Buy Options)

### 1. Qualtrics XM Platform
**Pricing:** $50K-150K/year (varies by features, users, and volume)
- **Strengths:** Industry leader, pre-built integrations, advanced text analytics
- **Weaknesses:** Expensive, limited customization, survey-focused
- **Australian Presence:** Yes (AWS Sydney)

### 2. Medallia Experience Cloud
**Pricing:** $80K-200K/year (enterprise pricing)
- **Strengths:** Real-time analytics, strong in financial services, AI-powered insights
- **Weaknesses:** High cost, complex setup, vendor lock-in
- **Australian Presence:** Yes (established in AU superannuation sector)

### 3. Clarabridge (Qualtrics)
**Pricing:** $60K-120K/year
- **Strengths:** Best-in-class text analytics, omnichannel
- **Weaknesses:** Recently acquired by Qualtrics, integration complexity
- **Australian Presence:** Limited

### 4. Verint Experience Management
**Pricing:** $40K-100K/year
- **Strengths:** Good for contact centers, speech analytics
- **Weaknesses:** Less modern UI, limited AI capabilities
- **Australian Presence:** Yes

---

## Total Cost of Ownership (3-Year)

### Build Option (Databricks)
| Component | Year 1 | Year 2-3 (annual) | 3-Year Total |
|-----------|--------|-------------------|--------------|
| Databricks incremental compute/storage | $30K | $35K | $100K |
| Development (2 engineers × 2 months) | $60K | $0 | $60K |
| Ongoing maintenance (0.5 FTE) | $25K | $50K | $125K |
| **Total** | **$115K** | **$85K** | **$285K** |

### Buy Option (VoC Platform - avg pricing)
| Component | Year 1 | Year 2-3 (annual) | 3-Year Total |
|-----------|--------|-------------------|--------------|
| Platform license | $80K | $85K | $250K |
| Implementation/professional services | $50K | $0 | $50K |
| Annual support/training | $15K | $20K | $55K |
| Integration development | $30K | $10K | $50K |
| Databricks for additional analytics | $20K | $25K | $70K |
| **Total** | **$195K** | **$140K** | **$475K** |

**3-Year Savings (Build):** ~$190K

---

## Decision Matrix

| Criteria | Build (Databricks) | Buy (VoC Platform) | Winner |
|----------|-------------------|-------------------|---------|
| **Cost** (3-year) | $285K | $475K | Build |
| **Time to Value** | 2-3 months | 3-6 months | Build |
| **Customization** | Full control | Limited | Build |
| **Australian Data Residency** | Native (already in AU) | Depends on vendor | Build |
| **Integration with Existing Stack** | Seamless (already on Databricks) | Requires middleware | Build |
| **Scalability** | Infinite (Databricks) | License-based limits | Build |
| **AI/ML Capabilities** | State-of-the-art (Foundation Models) | Vendor-specific (limited) | Build |
| **Governance & Security** | Unity Catalog (native) | Separate governance | Build |
| **Ongoing Vendor Risk** | Low (open lakehouse) | High (vendor lock-in) | Build |
| **Pre-built Dashboards/Reports** | Custom (must build) | Extensive library | Buy |
| **Support & Training** | Internal/Databricks community | Dedicated vendor support | Buy |

**Build: 9 | Buy: 2**

---

## Recommendation: BUILD

### Why Build on Databricks?

1. **Already 70% Complete** - Real Databricks features implemented (sentiment analysis, Vector Search, Real-Time Mode)
2. **Lower TCO** - $190K savings over 3 years
3. **Better Integration** - ART already uses Databricks for analytics
4. **Superior AI** - Access to latest foundation models (Llama 3.1 405B, DBRX)
5. **No Vendor Lock-in** - Open lakehouse architecture
6. **Australian Data Sovereignty** - Data stays in existing AWS Sydney region
7. **Unlimited Scale** - No per-user or per-interaction licensing

### Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Development complexity | 2-month timeline with 2 engineers (already proven) |
| Ongoing maintenance | 0.5 FTE sufficient; Databricks handles platform |
| Missing pre-built features | Build only what's needed; iterate based on feedback |
| Internal support | Leverage Databricks support + growing internal expertise |

### What You Don't Get with Build
- Pre-built industry benchmarking
- Vendor-managed upgrades (though Databricks handles platform)
- Dedicated account team for VoC best practices

---

## Implementation Roadmap (Build Option)

**Phase 1 (Weeks 1-4): Production Pilot**
- Finalize data pipelines for all 8 channels
- Deploy AI Agent with tool functions
- Launch executive & operations dashboards
- Onboard 10 pilot users

**Phase 2 (Weeks 5-8): Scale & Refine**
- Add advanced analytics (trend detection, predictive at-risk scoring)
- Integrate with CRM for automated workflows
- Expand to 50+ users
- Gather feedback and iterate

**Phase 3 (Weeks 9-12): Full Production**
- Deploy to all member services teams
- Add self-service analytics (Genie)
- Automate intervention workflows
- Measure ROI and optimize

**Total Timeline:** 3 months to full production

---

## Decision Factors to Consider

**Choose Build if:**
- ✅ You want full control and customization
- ✅ You value AI/ML innovation
- ✅ You already use Databricks extensively
- ✅ Budget constraints favor lower TCO

**Choose Buy if:**
- ❌ You need immediate out-of-box functionality
- ❌ You lack internal development resources
- ❌ You want vendor-managed solution
- ❌ Compliance requires a specialized VoC vendor

---

## Bottom Line

**Recommendation:** Build on Databricks

**Rationale:** ART already has 70% of the solution working with real Databricks features. Building saves $190K over 3 years, provides superior AI capabilities, seamless integration with existing stack, and avoids vendor lock-in. The demo proves technical feasibility and the 3-month roadmap is achievable.

**Next Steps:**
1. Executive approval for 2-month build phase ($60K dev + $30K platform)
2. Form project team (2 engineers, 1 product owner, 1 member services SME)
3. Kick off Phase 1 (Production Pilot)
4. Measure success metrics at 3 months, re-evaluate if needed

---

**Questions? Contact:** Pravin Varadhan

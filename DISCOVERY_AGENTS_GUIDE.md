# Discovery Agents Guide

This document explains how the Data Source Discovery system uses AI-powered agents to automatically find, evaluate, and configure new data sources.

## Overview

The Data Source Discovery system is a **LangGraph-based workflow** that orchestrates **7 sequential agents** working together to:

1. Find potential data sources matching user requirements
2. Evaluate access methods and data availability
3. Select the best source
4. Extract technical documentation
5. Test actual connectivity
6. Acquire API keys if needed
7. Save the configuration for future use

```
User Request
    ↓
┌─────────────────┐
│  Search Agent   │ → Find candidate data sources
└────────┬────────┘
         ↓
┌─────────────────┐
│ Examination     │ → Evaluate access methods for each source
│ Agent           │
└────────┬────────┘
         ↓
┌─────────────────┐
│ Selection Agent │ → Pick the best source (with human confirmation)
└────────┬────────┘
         ↓
┌─────────────────┐
│ Documentation   │ → Extract technical access details
│ Agent           │
└────────┬────────┘
         ↓
┌─────────────────┐
│ Testing Agent   │ → Verify connectivity and data retrieval
└────────┬────────┘
         ↓
    [Needs API Key?]
         ↓ yes
┌─────────────────┐
│ API Key Agent   │ → Acquire credentials
└────────┬────────┘
         ↓
┌─────────────────┐
│ Configuration   │ → Save to database
│ Agent           │
└─────────────────┘
```

## How the LLM Works in This Process

All agents use **Anthropic's Claude** (via `langchain_anthropic.ChatAnthropic`) for intelligent decision-making. The LLM serves as the "brain" that interprets, analyzes, and makes decisions at each step.

### LLM Configuration

```python
# From config.py
DISCOVERY_LLM_MODEL = "claude-sonnet-4-20250514"  # Configurable via environment
ANTHROPIC_API_KEY = "..."                          # Required
```

### Temperature Settings

Each agent uses a specific "temperature" setting that controls how creative vs. deterministic the LLM's responses are:

| Agent | Temperature | Reasoning |
|-------|-------------|-----------|
| SearchAgent | 0.3 | More creative for generating diverse search queries |
| ExaminationAgent | 0.2 | Balanced analysis of source capabilities |
| SelectionAgent | 0.1 | Deterministic selection decisions |
| DocumentationAgent | 0.1 | Precise extraction of technical details |
| TestingAgent | 0.1 | Strict validation of responses |
| APIKeyAgent | 0.1 | Accurate analysis of registration requirements |
| ConfigurationAgent | N/A | Minimal LLM usage (mostly database operations) |

### LLM Prompt Pattern

All agents follow a consistent pattern:
1. **System Prompt**: Defines the agent's role and capabilities
2. **Task Prompt**: Provides specific context and instructions
3. **JSON Output**: LLM returns structured JSON for programmatic processing

Example from DocumentationAgent:
```python
response = self.llm.invoke([
    SystemMessage(content=DOCUMENTATION_SYSTEM_PROMPT),
    HumanMessage(content=f"Extract access documentation for: {source_url}")
])
# Parse JSON from response
documentation = json.loads(response.content)
```

---

## Agent Deep Dive

### 1. Search Agent

**Purpose**: Find candidate data sources matching the user's description.

**Location**: `core/discovery/agents/search_agent.py`

**How the LLM is Used**:

1. **Query Generation**: The LLM transforms user descriptions into optimized search queries
   ```
   User: "I need agricultural crop yield data"
   LLM generates: ["USDA crop yield API", "agricultural statistics API", ...]
   ```

2. **Result Scoring**: The LLM evaluates each search result and assigns relevance scores (0-1)

**Data Sources Searched**:
- General web search (via Tavily API)
- Data.gov catalog
- APIs.guru directory

**Output**: List of `DataSourceCandidate` objects with:
- Name, URL, description
- Source type (government, academic, commercial)
- Relevance score (LLM-determined)

**Smart Feature**: Checks existing configured sources first to avoid discovering duplicates.

---

### 2. Examination Agent

**Purpose**: Evaluate what access methods each source provides.

**Location**: `core/discovery/agents/examination_agent.py`

**How the LLM is Used**:

The LLM analyzes fetched webpage content to determine:
- Does this source actually provide the data the user needs?
- What access methods are available (API, web service, file download)?
- Where is the API documentation located?

**Process**:
1. Fetch each candidate URL
2. Detect access methods via URL patterns and page content
3. Use LLM to verify findings and assess data relevance

**Output**: `ExaminedSource` objects containing:
```python
{
    "has_api": True,
    "has_web_service": False,
    "has_download": True,
    "provides_desired_data": True,
    "api_url": "https://api.example.com/v1",
    "documentation_url": "https://example.com/docs"
}
```

---

### 3. Selection Agent

**Purpose**: Choose the best data source from examined candidates.

**Location**: `core/discovery/agents/selection_agent.py`

**How the LLM is Used**:

After algorithmic pre-scoring, the LLM makes the final selection among top candidates by considering:
- How well does the data match user requirements?
- Quality and reliability of the source
- Ease of access (API preferred over downloads)

**Scoring Algorithm** (pre-LLM):
```
API available:        +100 points
Web service:          +50 points
Download only:        +25 points
Government source:    +20 bonus
Academic source:      +15 bonus
Has documentation:    +5 bonus
Relevance score:      ×10 multiplier
```

**Human-in-the-Loop**: When `require_confirmation=True`, the workflow pauses to let users confirm or change the selection.

---

### 4. Documentation Agent

**Purpose**: Extract complete technical documentation for accessing the selected source.

**Location**: `core/discovery/agents/documentation_agent.py`

**How the LLM is Used**:

This is one of the most LLM-intensive agents. The LLM:
1. Reads API documentation pages
2. Extracts structured information about endpoints, authentication, and data formats
3. Maps the source to a known connector type (if applicable)

**Information Extracted**:
```python
{
    "source_name": "USDA NASS QuickStats",
    "base_url": "https://quickstats.nass.usda.gov/api",
    "access_method": "REST_API",
    "authentication": {
        "type": "API_KEY",
        "key_location": "query_parameter",
        "key_name": "key"
    },
    "endpoints": [
        {
            "path": "/api/api_GET",
            "method": "GET",
            "parameters": ["source_desc", "commodity_desc", "year"]
        }
    ],
    "rate_limits": {"requests_per_minute": 100},
    "data_format": "JSON",
    "mapped_connector_type": "usda_nass"
}
```

**Connector Type Mapping**:
The LLM identifies if the source matches a known connector:
- `usda_nass` - USDA agricultural statistics
- `census` - US Census Bureau
- `fbi_crime` - FBI crime statistics
- `local_file` - Local file connector
- `discovered` - Generic discovered source

---

### 5. Testing Agent

**Purpose**: Verify the source is actually accessible and returns useful data.

**Location**: `core/discovery/agents/testing_agent.py`

**How the LLM is Used**:

The LLM validates API responses:
1. Does the response contain actual data (not just errors)?
2. Is the data relevant to what the user requested?
3. What's the confidence level in this assessment?

**Test Process**:
```python
1. Build test request from documentation
2. Execute HTTP request (with retry logic)
3. Check response status
4. Use LLM to validate response content
5. Return TestResults
```

**Error Handling**:
- **401/403 errors**: Pauses workflow, triggers API key acquisition
- **Rate limiting**: Exponential backoff retry
- **Timeouts**: Configurable retry count

**Configuration**:
```python
DISCOVERY_TEST_RETRIES = 3      # Max retry attempts
DISCOVERY_TEST_BACKOFF = 2.0    # Backoff multiplier
DISCOVERY_REQUEST_TIMEOUT = 30  # Seconds
```

---

### 6. API Key Agent

**Purpose**: Acquire API credentials when testing reveals authentication is required.

**Location**: `core/discovery/agents/api_key_agent.py`

**How the LLM is Used**:

The LLM analyzes API documentation to understand:
- Registration process requirements
- Where to find registration forms
- Expected email notification patterns

**Strategies**:
1. **Check Existing**: Look for stored keys in `APIKeyStore`
2. **Email Polling**: Monitor for API key emails (via Arcade service)
3. **Manual Request**: Ask user to provide credentials

**Human-in-the-Loop**: If automatic acquisition fails, the workflow pauses and requests manual input.

---

### 7. Configuration Agent

**Purpose**: Save the discovered data source configuration to MongoDB.

**Location**: `core/discovery/agents/configuration_agent.py`

**LLM Usage**: Minimal - primarily database operations.

**Stored Configuration**:
```python
{
    "source_id": "usda_nass_quickstats",
    "source_name": "USDA NASS QuickStats",
    "connector_type": "usda_nass",
    "authentication": {...},
    "endpoints": [...],
    "test_results": {...},
    "discovery_metadata": {
        "user_description": "agricultural crop yields",
        "discovered_at": "2024-...",
        "original_url": "https://..."
    }
}
```

---

## State Management

All agents share a common `DiscoveryState` object that flows through the pipeline:

```python
DiscoveryState = {
    # Workflow tracking
    "workflow_id": "uuid-...",
    "current_step": "testing",

    # Agent outputs (accumulated)
    "search_results": [...],        # From SearchAgent
    "examined_sources": [...],      # From ExaminationAgent
    "selected_source": {...},       # From SelectionAgent
    "access_documentation": {...},  # From DocumentationAgent
    "test_results": {...},          # From TestingAgent

    # Human-in-the-loop
    "waiting_for_human_input": False,
    "human_input_request": None,
    "pause_reason": None,

    # Error handling
    "error": None,
    "interrupted": False
}
```

### State Persistence

State is saved to MongoDB after each agent completes, enabling:
- **Pause/Resume**: Workflows can stop for human input and continue later
- **Recovery**: Failed workflows can be investigated and retried
- **Monitoring**: Track progress of long-running discoveries

---

## Human-in-the-Loop Integration

The system can pause at several points to request human input:

| Pause Point | Reason | Required Input |
|-------------|--------|----------------|
| Selection | Confirm source choice | Selection confirmation or override |
| Testing (401/403) | Authentication required | API key or credentials |
| API Key Acquisition | Automatic acquisition failed | Manual API key |
| Any Agent | Recoverable error | Retry, skip, or cancel |

**Pause/Resume Flow**:
```python
# 1. Agent triggers pause
state["waiting_for_human_input"] = True
state["pause_reason"] = PauseReason.NEEDS_API_KEY
state["human_input_request"] = HumanInputRequest(
    type="API_KEY",
    message="Please provide your API key",
    options=[...]
)

# 2. Workflow stops and waits

# 3. User provides input via API
POST /api/v1/discovery/{workflow_id}/resume
{"api_key": "user-provided-key"}

# 4. Workflow continues from current step
```

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/discovery` | POST | Start new discovery workflow |
| `/api/v1/discovery/{id}` | GET | Get workflow status |
| `/api/v1/discovery/{id}/resume` | POST | Resume with human input |
| `/api/v1/discovery/workflows/paused` | GET | List paused workflows |
| `/api/v1/discovery/{id}/cancel` | POST | Cancel workflow |

**Starting a Discovery**:
```bash
curl -X POST http://localhost:5000/api/v1/discovery \
  -H "Content-Type: application/json" \
  -d '{"description": "I need US crime statistics by state"}'
```

**Response**:
```json
{
  "workflow_id": "abc123",
  "status": "running",
  "current_step": "search"
}
```

---

## Prompt Engineering

All prompts are centralized in `core/discovery/prompts.py`. Key characteristics:

1. **Role Definition**: Each prompt clearly defines the agent's purpose
2. **Output Format**: JSON schemas specified for structured responses
3. **Decision Criteria**: Explicit guidance on how to evaluate options
4. **Error Handling**: Instructions for edge cases

**Example - Selection Agent Prompt**:
```
You are a data source selection expert. Your task is to choose
the best data source from the candidates.

Consider:
- How well does the data match user requirements?
- Prefer API access over web scraping or downloads
- Prefer government/academic sources for reliability
- Consider documentation quality

Return JSON:
{
  "selected_index": 0,
  "reasoning": "..."
}
```

---

## Error Handling

### Error Types

```python
WorkflowError = {
    "agent_name": "TestingAgent",
    "step": "testing",
    "issue": "Connection timeout",
    "details": "Failed after 3 retries",
    "recoverable": True
}
```

### Recovery Strategies

| Scenario | Action |
|----------|--------|
| Network timeout | Retry with exponential backoff |
| 401/403 response | Pause for API key |
| LLM parsing error | Fall back to heuristics |
| Recoverable error | Pause for human guidance |
| Non-recoverable | Stop workflow, mark failed |

---

## Configuration Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DISCOVERY_LLM_MODEL` | `claude-sonnet-4-20250514` | LLM model to use |
| `ANTHROPIC_API_KEY` | Required | Anthropic API key |
| `DISCOVERY_MAX_SEARCH_RESULTS` | 10 | Max candidates to examine |
| `DISCOVERY_TEST_RETRIES` | 3 | HTTP retry attempts |
| `DISCOVERY_TEST_BACKOFF` | 2.0 | Retry backoff multiplier |
| `DISCOVERY_REQUEST_TIMEOUT` | 30 | HTTP timeout (seconds) |

---

## Architecture Summary

The Discovery Agents system demonstrates several key patterns:

1. **LLM as Decision Engine**: Claude analyzes unstructured content (web pages, documentation) and makes intelligent decisions
2. **Agent Specialization**: Each agent has a focused responsibility with appropriate LLM temperature
3. **State Accumulation**: Information builds through the pipeline, with each agent adding to shared state
4. **Human-in-the-Loop**: System gracefully pauses when human judgment or credentials are needed
5. **Resilient Execution**: Retries, fallbacks, and state persistence ensure workflows can recover from failures

This architecture enables the system to autonomously discover and configure data sources while maintaining human oversight at critical decision points.

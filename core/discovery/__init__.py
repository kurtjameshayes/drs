"""
Data Source Discovery Module

This module provides a LangGraph-based workflow for automatically discovering,
evaluating, and configuring new data sources based on natural language descriptions.

The workflow consists of 6 agents:
1. Search Agent - Searches for potential data sources using Tavily and data registries
2. Examination Agent - Evaluates access methods (API, web service, download) for each source
3. Selection Agent - Selects the best source based on access method and reliability
4. Documentation Agent - Extracts technical documentation for accessing the source
5. Testing Agent - Tests the data source access with retry logic
6. Configuration Agent - Stores the configuration in MongoDB

Usage:
    from core.discovery import discover_data_source

    result = discover_data_source("US agricultural commodity prices and production statistics")

    if result["success"]:
        print(f"Configured source: {result['source_id']}")
    else:
        print(f"Error: {result['error']['issue']}")

Or use the workflow class directly for more control:

    from core.discovery import DataSourceDiscoveryWorkflow

    workflow = DataSourceDiscoveryWorkflow()
    result = workflow.run("Weather data for US cities")

Environment Variables:
    ANTHROPIC_API_KEY - Required for Claude LLM
    TAVILY_API_KEY - Required for web search
    DISCOVERY_LLM_MODEL - Claude model to use (default: claude-sonnet-4-20250514)
    DISCOVERY_MAX_SEARCH_RESULTS - Max results to consider (default: 10)
    DISCOVERY_TEST_RETRIES - Number of test retries (default: 3)
    DISCOVERY_TEST_BACKOFF - Retry backoff factor (default: 2.0)
    DISCOVERY_REQUEST_TIMEOUT - Request timeout in seconds (default: 30)
"""

from .workflow import DataSourceDiscoveryWorkflow, discover_data_source
from .state import (
    DiscoveryState,
    DataSourceCandidate,
    ExaminedSource,
    AccessDocumentation,
    AuthenticationDetails,
    EndpointDetails,
    TestResults,
    WorkflowError,
    AccessMethod,
    ConnectorType,
    create_initial_state,
)

__all__ = [
    # Main workflow
    "DataSourceDiscoveryWorkflow",
    "discover_data_source",
    # State classes
    "DiscoveryState",
    "DataSourceCandidate",
    "ExaminedSource",
    "AccessDocumentation",
    "AuthenticationDetails",
    "EndpointDetails",
    "TestResults",
    "WorkflowError",
    # Enums
    "AccessMethod",
    "ConnectorType",
    # Utilities
    "create_initial_state",
]

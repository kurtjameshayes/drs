# Skill: API Documentation Analysis

## Purpose
Thoroughly analyze API and service documentation to determine exact access methodology.

## Context
You are a technical documentation specialist extracting detailed information needed to programmatically access a data source.

## Task
Given API documentation:

1. **Extract authentication details** - How to authenticate? API key? OAuth? Basic auth?
2. **Identify endpoints** - What endpoints are available? What do they do?
3. **Document parameters** - What parameters does each endpoint accept?
4. **Determine format** - JSON, XML, CSV? Response structure?
5. **Extract examples** - Find example requests and responses
6. **Note constraints** - Rate limits, pagination, data limits

## Available Tools
- fetch_url: Fetch documentation pages
- parse_openapi: Parse OpenAPI/Swagger specifications
- parse_html: Extract content from documentation pages

## Documentation Analysis Checklist

### Authentication Section
- Look for "Authentication", "Auth", "Getting Started", "API Keys"
- Extract header name (e.g., "Authorization", "X-API-Key")
- Determine format (e.g., "Bearer {token}", "token={key}")
- Find registration/key acquisition URL
- Note any auth limitations or scopes

### Endpoints Section
- List all available endpoints
- For each endpoint:
  - HTTP method (GET, POST, PUT, DELETE)
  - Full URL path
  - Required and optional parameters
  - Expected response format
  - Example request and response
  - Error codes and meanings

### Rate Limits Section
- Requests per minute/hour/day
- Concurrent request limits
- Any quota systems
- Burst allowances

### Data Format Section
- Response format (JSON, XML, CSV)
- Pagination method (offset, cursor, page-based)
- Field names and types
- Null value handling

## Output Format

Return complete documentation as JSON:
```json
{
  "base_url": "https://api.example.com",
  "access_method": "api|web_service|download|contact_required|unknown",
  "authentication": {
    "required": true/false,
    "auth_type": "api_key|oauth|basic|none",
    "auth_header": "Authorization",
    "auth_format": "Bearer {token}",
    "registration_url": "https://...",
    "notes": "Important auth notes"
  },
  "endpoints": [
    {
      "url": "/v1/data",
      "method": "GET",
      "description": "Fetch data",
      "parameters": [
        {
          "name": "format",
          "type": "string",
          "required": false,
          "values": ["json", "csv"]
        }
      ],
      "required_params": ["api_key"],
      "response_format": "json",
      "example_request": "GET /v1/data?format=json",
      "example_response": "{\"data\": []}"
    }
  ],
  "rate_limits": "1000 requests per hour",
  "data_format": "json",
  "update_frequency": "Daily",
  "terms_of_use_url": "https://...",
  "mapped_connector_type": "discovered|usda_nass|census|fbi_crime",
  "notes": "Additional important information"
}
```

## Common API Patterns

### RESTful APIs
- Use HTTP methods meaningfully
- Resource-based endpoints (/data, /records, /search)
- JSON responses
- Standard HTTP status codes

### GraphQL APIs
- Single endpoint (/graphql)
- Query language for data selection
- Introspection available
- Often POST-only

### Government APIs
- Often REST-based
- Usually free with registration
- OpenAPI documentation common
- Rate limits enforced

"""
State definitions for the LangGraph data source discovery workflow.

This module defines all the typed state structures used throughout the
discovery workflow, including data classes for candidates, examined sources,
documentation, test results, and errors.
"""

from typing import TypedDict, List, Optional, Dict, Any, Literal
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum


class AccessMethod(str, Enum):
    """Enumeration of data source access methods."""
    API = "api"
    WEB_SERVICE = "web_service"
    DOWNLOAD = "download"
    UNKNOWN = "unknown"


class ConnectorType(str, Enum):
    """Known connector types that can be mapped to."""
    USDA_NASS = "usda_nass"
    CENSUS = "census"
    FBI_CRIME = "fbi_crime"
    LOCAL_FILE = "local_file"
    DISCOVERED = "discovered"  # Fallback for unmapped sources


@dataclass
class DataSourceCandidate:
    """
    Represents a potential data source found during search.
    """
    name: str
    url: str
    description: str
    source_type: str = ""  # e.g., "government", "commercial", "academic"
    relevance_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataSourceCandidate":
        return cls(**data)


@dataclass
class ExaminedSource:
    """
    Represents a data source after examination, with access method details.
    """
    candidate: DataSourceCandidate
    has_api: bool = False
    has_web_service: bool = False
    has_download: bool = False
    provides_desired_data: bool = False
    api_url: Optional[str] = None
    documentation_url: Optional[str] = None
    access_notes: str = ""
    examination_timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    @property
    def best_access_method(self) -> AccessMethod:
        """Determine the best available access method (priority: API > Web Service > Download)."""
        if self.has_api:
            return AccessMethod.API
        elif self.has_web_service:
            return AccessMethod.WEB_SERVICE
        elif self.has_download:
            return AccessMethod.DOWNLOAD
        return AccessMethod.UNKNOWN

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["best_access_method"] = self.best_access_method.value
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExaminedSource":
        # Handle nested candidate
        if isinstance(data.get("candidate"), dict):
            data["candidate"] = DataSourceCandidate.from_dict(data["candidate"])
        # Remove computed field if present
        data.pop("best_access_method", None)
        return cls(**data)


@dataclass
class AuthenticationDetails:
    """Authentication requirements for a data source."""
    required: bool = False
    auth_type: str = ""  # "api_key", "oauth", "basic", "none"
    auth_header: str = ""  # e.g., "Authorization", "X-API-Key"
    auth_format: str = ""  # e.g., "Bearer {token}", "{key}"
    registration_url: Optional[str] = None
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AuthenticationDetails":
        return cls(**data)


@dataclass
class EndpointDetails:
    """Details about an API endpoint."""
    url: str
    method: str = "GET"
    parameters: Dict[str, Any] = field(default_factory=dict)
    required_params: List[str] = field(default_factory=list)
    optional_params: List[str] = field(default_factory=list)
    response_format: str = "json"
    example_request: str = ""
    example_response: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EndpointDetails":
        return cls(**data)


@dataclass
class AccessDocumentation:
    """
    Complete documentation for accessing a data source.
    """
    source_name: str
    base_url: str
    access_method: AccessMethod
    authentication: AuthenticationDetails
    endpoints: List[EndpointDetails] = field(default_factory=list)
    rate_limits: Dict[str, Any] = field(default_factory=dict)
    data_format: str = "json"
    update_frequency: str = ""
    terms_of_use_url: Optional[str] = None
    notes: str = ""
    mapped_connector_type: ConnectorType = ConnectorType.DISCOVERED

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "source_name": self.source_name,
            "base_url": self.base_url,
            "access_method": self.access_method.value,
            "authentication": self.authentication.to_dict(),
            "endpoints": [e.to_dict() for e in self.endpoints],
            "rate_limits": self.rate_limits,
            "data_format": self.data_format,
            "update_frequency": self.update_frequency,
            "terms_of_use_url": self.terms_of_use_url,
            "notes": self.notes,
            "mapped_connector_type": self.mapped_connector_type.value,
        }
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AccessDocumentation":
        data["access_method"] = AccessMethod(data.get("access_method", "unknown"))
        data["authentication"] = AuthenticationDetails.from_dict(data.get("authentication", {}))
        data["endpoints"] = [EndpointDetails.from_dict(e) for e in data.get("endpoints", [])]
        data["mapped_connector_type"] = ConnectorType(data.get("mapped_connector_type", "discovered"))
        return cls(**data)


@dataclass
class TestResults:
    """
    Results from testing access to a data source.
    """
    success: bool
    status_code: Optional[int] = None
    response_time_ms: Optional[float] = None
    data_received: bool = False
    data_matches_description: bool = False
    sample_data: Optional[Dict[str, Any]] = None
    error_message: str = ""
    attempts: int = 1
    test_timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TestResults":
        return cls(**data)


@dataclass
class WorkflowError:
    """
    Error information when the workflow is interrupted.
    """
    agent_name: str
    step: str
    issue: str
    details: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    recoverable: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkflowError":
        return cls(**data)


class HumanInputType(str, Enum):
    """Types of human input that may be required."""
    API_KEY = "api_key"
    OAUTH_TOKEN = "oauth_token"
    USERNAME_PASSWORD = "username_password"
    CONFIRMATION = "confirmation"
    SELECTION = "selection"
    SELECTION_CONFIRMATION = "selection_confirmation"
    ERROR_GUIDANCE = "error_guidance"
    MISSING_INFO = "missing_info"
    GUIDANCE = "guidance"
    EXISTING_SOURCE_FOUND = "existing_source_found"


@dataclass
class HumanInputRequest:
    """
    Request for human input when workflow is paused.
    """
    input_type: HumanInputType
    field_name: str
    description: str
    required: bool = True
    registration_url: Optional[str] = None
    additional_info: Dict[str, Any] = field(default_factory=dict)
    # For selection confirmation
    options: List[Dict[str, Any]] = field(default_factory=list)
    recommended_option: Optional[int] = None  # Index of recommended option
    # For error/guidance scenarios
    error_context: Optional[str] = None
    suggested_actions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "input_type": self.input_type.value,
            "field_name": self.field_name,
            "description": self.description,
            "required": self.required,
            "registration_url": self.registration_url,
            "additional_info": self.additional_info,
            "options": self.options,
            "recommended_option": self.recommended_option,
            "error_context": self.error_context,
            "suggested_actions": self.suggested_actions,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HumanInputRequest":
        data["input_type"] = HumanInputType(data.get("input_type", "api_key"))
        # Handle optional fields that might not be in old data
        if "options" not in data:
            data["options"] = []
        if "recommended_option" not in data:
            data["recommended_option"] = None
        if "error_context" not in data:
            data["error_context"] = None
        if "suggested_actions" not in data:
            data["suggested_actions"] = []
        return cls(**data)


class DiscoveryState(TypedDict, total=False):
    """
    The main state object passed through the LangGraph workflow.

    This TypedDict defines all possible state fields. Not all fields
    are required at every step - they are populated as the workflow progresses.
    """
    # Workflow tracking
    workflow_id: str  # Unique ID for this workflow execution

    # Input
    user_description: str

    # Search Agent outputs
    search_results: List[Dict[str, Any]]  # List of DataSourceCandidate dicts
    search_completed: bool

    # Examination Agent outputs
    examined_sources: List[Dict[str, Any]]  # List of ExaminedSource dicts
    current_examination_index: int
    examination_completed: bool

    # Selection Agent outputs
    selected_source: Optional[Dict[str, Any]]  # ExaminedSource dict
    selection_completed: bool

    # Documentation Agent outputs
    access_documentation: Optional[Dict[str, Any]]  # AccessDocumentation dict
    documentation_completed: bool

    # Testing Agent outputs
    test_results: Optional[Dict[str, Any]]  # TestResults dict
    test_passed: bool
    testing_completed: bool

    # API Key Acquisition fields
    api_key_acquired: bool
    api_key_source: Optional[str]  # "automatic", "existing", "manual"
    api_key_acquisition_attempted: bool
    api_key_acquisition_error: Optional[str]
    email_check_attempts: int

    # Configuration Agent outputs
    config_id: Optional[str]
    source_id: Optional[str]
    configuration_completed: bool

    # Error handling
    error: Optional[Dict[str, Any]]  # WorkflowError dict
    interrupted: bool

    # Human-in-the-loop fields
    waiting_for_human_input: bool
    human_input_request: Optional[Dict[str, Any]]  # HumanInputRequest dict
    human_input_received: Optional[Dict[str, Any]]  # Input provided by user
    pause_reason: Optional[str]  # PauseReason value for the current pause

    # Selection confirmation fields
    selection_options: Optional[List[Dict[str, Any]]]  # Available options for user selection
    selection_confirmed: bool  # Whether user confirmed the selection
    user_selected_index: Optional[int]  # User's selected option index (if different from recommended)

    # Existing source handling
    existing_sources_found: Optional[List[Dict[str, Any]]]  # Existing configured sources that match
    use_existing_source: Optional[bool]  # Whether to use an existing source
    selected_existing_source_id: Optional[str]  # ID of the selected existing source

    # Workflow metadata
    workflow_start_time: str
    workflow_end_time: Optional[str]
    current_step: str  # Track which step the workflow is on


def create_initial_state(user_description: str, workflow_id: str = None) -> DiscoveryState:
    """
    Create an initial state for the discovery workflow.

    Args:
        user_description: The user's description of the desired data source
        workflow_id: Optional workflow ID. If not provided, one will be generated.

    Returns:
        Initialized DiscoveryState
    """
    import uuid

    if workflow_id is None:
        workflow_id = f"wf_{uuid.uuid4().hex[:12]}"

    return DiscoveryState(
        workflow_id=workflow_id,
        user_description=user_description,
        search_results=[],
        search_completed=False,
        examined_sources=[],
        current_examination_index=0,
        examination_completed=False,
        selected_source=None,
        selection_completed=False,
        access_documentation=None,
        documentation_completed=False,
        test_results=None,
        test_passed=False,
        testing_completed=False,
        api_key_acquired=False,
        api_key_source=None,
        api_key_acquisition_attempted=False,
        api_key_acquisition_error=None,
        email_check_attempts=0,
        config_id=None,
        source_id=None,
        configuration_completed=False,
        error=None,
        interrupted=False,
        waiting_for_human_input=False,
        human_input_request=None,
        human_input_received=None,
        pause_reason=None,
        selection_options=None,
        selection_confirmed=False,
        user_selected_index=None,
        existing_sources_found=None,
        use_existing_source=None,
        selected_existing_source_id=None,
        workflow_start_time=datetime.utcnow().isoformat(),
        workflow_end_time=None,
        current_step="initialized",
    )

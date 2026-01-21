"""
Skill Registry for managing LLM agent capabilities.

Skills describe what an agent can do and are loaded conditionally based on task context.
This enables dynamic, task-specific LLM guidance without modifying core agent code.
"""

import os
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class Skill:
    """Represents a single skill/capability for an agent."""

    name: str  # Unique identifier (e.g., 'web_search', 'parse_html')
    description: str  # Brief description of what it does
    agent_types: List[str]  # Which agents can use it (e.g., ['search', 'examination'])
    task_keywords: List[str]  # Keywords that trigger this skill (e.g., ['search', 'find'])
    prompt_template: str  # How to describe it to the LLM
    requires_config: Optional[str] = None  # Config key needed (e.g., 'TAVILY_API_KEY')


class SkillRegistry:
    """
    Registry for managing skills and matching them to agents/tasks.

    Skills are loaded from disk (SKILL.md files in skills directory) and can be
    selected based on agent type and task keywords.
    """

    def __init__(self):
        self.skills: Dict[str, Skill] = {}
        self._load_default_skills()
        self._load_skills_from_disk()

    def _load_default_skills(self):
        """Load built-in skills that are always available."""

        self.skills['web_search'] = Skill(
            name='web_search',
            description='Search the web for information',
            agent_types=['search', 'examination', 'documentation'],
            task_keywords=['search', 'find', 'discover', 'locate', 'look for'],
            prompt_template='You can use web_search to find relevant information online and identify data sources across the internet.',
        )

        self.skills['registry_search'] = Skill(
            name='registry_search',
            description='Search known data registries',
            agent_types=['search', 'examination'],
            task_keywords=['registry', 'catalog', 'index', 'directory'],
            prompt_template='You can search known data registries (Data.gov, APIs.guru, GitHub, etc.) to find established data sources.',
        )

        self.skills['parse_html'] = Skill(
            name='parse_html',
            description='Parse and analyze HTML content',
            agent_types=['examination', 'documentation'],
            task_keywords=['html', 'page', 'content', 'parse', 'analyze'],
            prompt_template='You can parse and analyze HTML to extract form fields, API documentation, endpoints, and other relevant page content.',
        )

        self.skills['api_analysis'] = Skill(
            name='api_analysis',
            description='Analyze API documentation and structure',
            agent_types=['documentation', 'examination'],
            task_keywords=['api', 'endpoint', 'rest', 'graphql', 'openapi', 'swagger'],
            prompt_template='You can analyze API documentation including REST, GraphQL, and OpenAPI specifications to understand authentication, endpoints, and parameters.',
        )

        self.skills['browser_automation'] = Skill(
            name='browser_automation',
            description='Automate browser actions for form filling and interaction',
            agent_types=['api_key'],
            task_keywords=['form', 'fill', 'submit', 'register', 'signup', 'automate', 'browser'],
            prompt_template='You can automate browser actions to fill out and submit registration forms, navigate sites, and extract data from dynamic pages.',
        )

        self.skills['email_monitoring'] = Skill(
            name='email_monitoring',
            description='Monitor email for verification codes and messages',
            agent_types=['api_key'],
            task_keywords=['email', 'verify', 'verification', 'code', 'confirm'],
            prompt_template='You can monitor email accounts for verification codes and extract authentication tokens from verification messages.',
        )

        self.skills['form_field_identification'] = Skill(
            name='form_field_identification',
            description='Identify and classify form fields',
            agent_types=['examination', 'api_key'],
            task_keywords=['form', 'field', 'input', 'required', 'optional'],
            prompt_template='You can identify HTML form fields, classify them as required or optional, detect field types, and understand form structure.',
        )

        self.skills['fetch_content'] = Skill(
            name='fetch_content',
            description='Fetch and analyze web page content',
            agent_types=['examination', 'documentation', 'testing'],
            task_keywords=['fetch', 'get', 'retrieve', 'download', 'request'],
            prompt_template='You can fetch web pages and extract relevant content including text, HTML structure, and metadata.',
        )

        self.skills['test_endpoint'] = Skill(
            name='test_endpoint',
            description='Test API endpoints and verify responses',
            agent_types=['testing'],
            task_keywords=['test', 'verify', 'check', 'validate', 'request', 'endpoint'],
            prompt_template='You can make HTTP requests to test API endpoints, validate responses, check authentication, and verify data formats.',
        )

        self.skills['error_recovery'] = Skill(
            name='error_recovery',
            description='Analyze errors and recommend recovery strategies',
            agent_types=['testing', 'workflow_decision'],
            task_keywords=['error', 'fail', 'recover', 'retry', 'problem', 'issue'],
            prompt_template='You can analyze errors and failures, identify root causes, and recommend appropriate recovery or retry strategies.',
        )

    def _load_skills_from_disk(self):
        """Load additional skills from SKILL.md files in the skills directory."""
        skills_dir = Path(__file__).parent / 'skills'

        if not skills_dir.exists():
            logger.debug(f"Skills directory not found at {skills_dir}")
            return

        # Iterate through skill subdirectories
        for skill_dir in skills_dir.iterdir():
            if not skill_dir.is_dir():
                continue

            skill_file = skill_dir / 'SKILL.md'
            if not skill_file.exists():
                continue

            try:
                # For now, we just note that the skill exists
                # In a production system, we'd parse the SKILL.md file to extract metadata
                skill_name = skill_dir.name
                logger.debug(f"Found skill directory: {skill_name}")

                # Load basic metadata (could be extended to parse SKILL.md)
                # For now, skills from disk are informational - they're loaded
                # into the system by explicit reference from agents
            except Exception as e:
                logger.warning(f"Error loading skill from {skill_file}: {e}")

    def get_skills_for_agent(self, agent_type: str) -> List[Skill]:
        """
        Get all skills available to a specific agent type.

        Args:
            agent_type: Type of agent (e.g., 'search', 'examination', 'api_key')

        Returns:
            List of Skill objects available to this agent
        """
        return [s for s in self.skills.values() if agent_type in s.agent_types]

    def get_skills_for_task(
        self,
        agent_type: str,
        task_description: str,
        min_score: float = 0.5
    ) -> List[Skill]:
        """
        Get skills that are relevant to a specific task.

        Uses keyword matching to determine which skills are needed.

        Args:
            agent_type: Type of agent
            task_description: Description of the task to perform
            min_score: Minimum relevance score (0.0-1.0)

        Returns:
            List of relevant Skill objects, sorted by relevance
        """
        task_lower = task_description.lower()
        scored_skills: List[tuple[Skill, float]] = []

        # Get all skills for this agent type
        agent_skills = self.get_skills_for_agent(agent_type)

        # Score each skill based on keyword matches
        for skill in agent_skills:
            score = 0.0
            matches = 0

            # Count keyword matches
            for keyword in skill.task_keywords:
                if keyword.lower() in task_lower:
                    matches += 1
                    score += 0.3  # Each match adds score

            # Normalize score
            if matches > 0:
                score = min(1.0, score)  # Cap at 1.0

            if score >= min_score:
                scored_skills.append((skill, score))

        # Sort by score (highest first)
        scored_skills.sort(key=lambda x: x[1], reverse=True)

        # Return just the skills
        return [skill for skill, score in scored_skills]

    def get_skill(self, skill_name: str) -> Optional[Skill]:
        """Get a single skill by name."""
        return self.skills.get(skill_name)

    def format_skills_for_prompt(self, skills: List[Skill]) -> str:
        """
        Format a list of skills for inclusion in an LLM prompt.

        Args:
            skills: List of Skill objects

        Returns:
            Formatted string suitable for inclusion in system/human messages
        """
        if not skills:
            return ""

        lines = ["AVAILABLE CAPABILITIES:"]
        for skill in skills:
            lines.append(f"- {skill.name}: {skill.prompt_template}")

        return "\n".join(lines)

    def get_all_skills(self) -> Dict[str, Skill]:
        """Get all registered skills."""
        return self.skills.copy()


# Singleton instance for use across the application
_registry = None


def get_skill_registry() -> SkillRegistry:
    """Get or create the global skill registry."""
    global _registry
    if _registry is None:
        _registry = SkillRegistry()
    return _registry

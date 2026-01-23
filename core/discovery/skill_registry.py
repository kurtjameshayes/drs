"""
Skill Registry for managing LLM agent capabilities.

Skills describe what an agent can do and are loaded conditionally based on task context.
This enables dynamic, task-specific LLM guidance without modifying core agent code.
"""

import os
import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

logger = logging.getLogger(__name__)


@dataclass
class Skill:
    """Represents a single skill/capability for an agent."""

    name: str  # Unique identifier (e.g., 'web_search', 'parse_html')
    description: str  # Brief description of what it does
    agent_types: List[str]  # Which agents can use it (e.g., ['search', 'examination'])
    task_keywords: List[str]  # Keywords that trigger this skill (e.g., ['search', 'find'])
    prompt_template: Optional[str] = None  # How to describe it to the LLM (inline)
    prompt_file: Optional[str] = None  # Path to SKILL.md file (file-based, takes precedence)
    requires_config: Optional[str] = None  # Config key needed (e.g., 'TAVILY_API_KEY')


class SkillRegistry:
    """
    Registry for managing skills and matching them to agents/tasks.

    Skills are loaded from disk (SKILL.md files in skills directory) and can be
    selected based on agent type and task keywords.

    Skills can use either:
    - prompt_template: Inline text description
    - prompt_file: Path to SKILL.md file (takes precedence)
    """

    def __init__(self):
        self.skills: Dict[str, Skill] = {}
        self._file_cache: Dict[str, str] = {}  # Cache for loaded SKILL.md files
        self._load_default_skills()
        self._load_skills_from_disk()

    def _load_default_skills(self):
        """
        Load built-in skills that are always available.

        This method is now primarily a placeholder. Skills are loaded from the filesystem
        via _load_skills_from_disk(). You can add fallback/core skills here if needed.
        """
        # All skills are now loaded from SKILL.md files in the skills/ directory
        # This allows for easier maintenance and updates without code changes
        pass

    def _load_skill_file_content(self, file_path: str) -> Optional[str]:
        """
        Load and cache the content of a SKILL.md file.

        Args:
            file_path: Relative path to SKILL.md file (e.g., 'core/discovery/skills/api-acquisition-request-page-analyze/SKILL.md')

        Returns:
            File content as string, or None if file not found
        """
        # Check cache first
        if file_path in self._file_cache:
            return self._file_cache[file_path]

        try:
            # Resolve path relative to project root
            full_path = Path(__file__).parent.parent.parent / file_path

            if not full_path.exists():
                logger.warning(f"Skill file not found: {full_path}")
                return None

            # Read file content
            with open(full_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Cache it
            self._file_cache[file_path] = content
            logger.debug(f"Loaded skill file: {file_path}")

            return content

        except Exception as e:
            logger.error(f"Failed to load skill file {file_path}: {e}")
            return None

    def _parse_skill_file(self, skill_file: Path) -> Optional[tuple[Dict, str]]:
        """
        Parse a SKILL.md file to extract YAML frontmatter and content.

        Args:
            skill_file: Path to SKILL.md file

        Returns:
            Tuple of (metadata_dict, content) or None if parsing fails

        Expected format:
            ---
            name: skill_name
            description: Brief description
            agent_types: [agent1, agent2]
            task_keywords: [keyword1, keyword2]
            requires_config: optional_config_key
            ---
            # Rest of markdown content
        """
        try:
            with open(skill_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # Check for YAML frontmatter (must start with ---)
            if not content.startswith('---'):
                logger.warning(f"Skill file {skill_file} missing YAML frontmatter")
                return None

            # Extract frontmatter and content
            # Pattern: ---\n...yaml...\n---\n...content...
            parts = content.split('---', 2)
            if len(parts) < 3:
                logger.warning(f"Skill file {skill_file} has malformed YAML frontmatter")
                return None

            frontmatter_str = parts[1].strip()
            skill_content = parts[2].strip()

            # Parse YAML frontmatter
            if yaml is None:
                logger.error("PyYAML is not installed. Cannot parse skill metadata. Install with: pip install pyyaml")
                return None

            try:
                metadata = yaml.safe_load(frontmatter_str)
            except yaml.YAMLError as e:
                logger.error(f"Failed to parse YAML frontmatter in {skill_file}: {e}")
                return None

            # Validate required fields
            required_fields = ['name', 'description', 'agent_types', 'task_keywords']
            for field in required_fields:
                if field not in metadata:
                    logger.warning(f"Skill file {skill_file} missing required field: {field}")
                    return None

            return metadata, skill_content

        except Exception as e:
            logger.error(f"Error parsing skill file {skill_file}: {e}")
            return None

    def _load_skills_from_disk(self):
        """Load skills from SKILL.md files in the skills directory."""
        skills_dir = Path(__file__).parent / 'skills'

        if not skills_dir.exists():
            logger.debug(f"Skills directory not found at {skills_dir}")
            return

        loaded_count = 0

        # Iterate through skill subdirectories
        for skill_dir in skills_dir.iterdir():
            if not skill_dir.is_dir():
                continue

            skill_file = skill_dir / 'SKILL.md'
            if not skill_file.exists():
                continue

            try:
                # Parse the SKILL.md file
                result = self._parse_skill_file(skill_file)
                if result is None:
                    continue

                metadata, content = result

                # Extract metadata fields
                skill_name = metadata['name']
                description = metadata['description']
                agent_types = metadata.get('agent_types', [])
                task_keywords = metadata.get('task_keywords', [])
                requires_config = metadata.get('requires_config', None)

                # Ensure lists are actually lists
                if isinstance(agent_types, str):
                    agent_types = [agent_types]
                if isinstance(task_keywords, str):
                    task_keywords = [task_keywords]

                # Create the skill object
                skill = Skill(
                    name=skill_name,
                    description=description,
                    agent_types=agent_types,
                    task_keywords=task_keywords,
                    prompt_template=content,  # Store the full content as template
                    prompt_file=None,  # We already loaded the content
                    requires_config=requires_config
                )

                # Register the skill
                self.skills[skill_name] = skill
                loaded_count += 1
                logger.info(f"Loaded skill from disk: {skill_name} (agents: {', '.join(agent_types)})")

            except Exception as e:
                logger.warning(f"Error loading skill from {skill_file}: {e}")

        logger.info(f"Loaded {loaded_count} skills from filesystem")

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

        For each skill, uses prompt_file content if available, otherwise uses prompt_template.

        Args:
            skills: List of Skill objects

        Returns:
            Formatted string suitable for inclusion in system/human messages
        """
        if not skills:
            return ""

        skill_contents = []

        for skill in skills:
            # Prefer file-based prompt over inline template
            if skill.prompt_file:
                file_content = self._load_skill_file_content(skill.prompt_file)
                if file_content:
                    skill_contents.append(file_content)
                    continue

            # Fall back to inline template
            if skill.prompt_template:
                skill_contents.append(f"## {skill.name.replace('_', ' ').title()}\n{skill.prompt_template}")

        # Join all skill contents with section breaks
        return "\n\n---\n\n".join(skill_contents) if skill_contents else ""

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

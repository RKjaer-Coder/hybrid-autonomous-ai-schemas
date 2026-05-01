from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from immune.patterns.policy_signatures import CONSTRUCTION_ALLOWLIST, NETWORK_TOOLS
from skills.config import IntegrationConfig

PROFILE_CONFIG_SKILL_KEY = "hybrid_autonomous_ai"
DEFAULT_LOCAL_MODEL = "hybrid-autonomous-ai-local"
DEFAULT_STRONG_MODEL = "hybrid-autonomous-ai-strong"
DEFAULT_LOCAL_BASE_URL = f"http://127.0.0.1:{min(CONSTRUCTION_ALLOWLIST.permitted_ports)}/v1"

EXPECTED_DANGEROUS_COMMAND_FAMILIES: dict[str, tuple[str, ...]] = {
    "rm_rf": ("rm -rf", "rm\\s+(-[rrf]+\\s+)*[/~]"),
    "chmod_777": ("chmod 777", "chmod\\s+[0-7]*777"),
    "sudo_root": ("sudo", "su root", "su\\s+root", "doas"),
    "disk_ops": ("mkfs", "fdisk", "dd if=", "of=/dev"),
    "firewall_ops": ("iptables", "ufw", "firewall-cmd"),
}


def nested_get(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def contains_subset(actual: Any, expected: Any) -> bool:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return False
        return all(key in actual and contains_subset(actual[key], value) for key, value in expected.items())
    if isinstance(expected, list):
        if not isinstance(actual, list):
            return False
        return all(any(contains_subset(candidate, item) for candidate in actual) for item in expected)
    return actual == expected


@dataclass(frozen=True)
class HermesProfileContract:
    """Single source of truth for the repo-owned Hermes profile/config contract."""

    config: IntegrationConfig
    repo_root: str
    approval_mode: str = "manual"

    @property
    def repo_root_path(self) -> Path:
        return Path(self.repo_root).expanduser().resolve()

    @property
    def representative_dangerous_commands(self) -> list[str]:
        return [variants[0] for variants in EXPECTED_DANGEROUS_COMMAND_FAMILIES.values()]

    def runtime_mapping(self) -> dict[str, Any]:
        return {
            "repo_root": str(self.repo_root_path),
            "data_dir": self.config.data_dir,
            "skills_dir": self.config.skills_dir,
            "checkpoints_dir": self.config.checkpoints_dir,
            "alerts_dir": self.config.alerts_dir,
            "logs_dir": str(Path(self.config.data_dir).expanduser().resolve().parent / "logs"),
        }

    def network_controls(self) -> dict[str, Any]:
        split = urlsplit(self.config.proxy_bind_url)
        return {
            "proxy_bind_url": self.config.proxy_bind_url,
            "proxy_bind_host": split.hostname or "127.0.0.1",
            "proxy_bind_port": split.port or 18080,
            "outbound_allowlist": {
                "domains": list(self.config.outbound_allowlist_domains),
                "ports": list(self.config.outbound_allowlist_ports),
                "schemes": sorted(CONSTRUCTION_ALLOWLIST.permitted_schemes),
            },
            "seed_network_tools": sorted(NETWORK_TOOLS),
        }

    def gateway_mapping(self) -> dict[str, Any]:
        return {
            "url": self.config.hermes_gateway_url,
            "enabled": True,
            "expected_tools": ["web_search", "web_fetch", "image_generation", "browser_automation", "tts"],
        }

    def workspace_mapping(self) -> dict[str, Any]:
        return {
            "url": self.config.hermes_workspace_url,
            "enabled": True,
            "preferred_surfaces": [
                "models",
                "chat",
                "plugins",
                "gates",
                "execution_traces",
                "quarantines",
                "replay_readiness",
                "runtime_halt_state",
                "milestone_health",
            ],
        }

    def local_provider_mapping(self) -> dict[str, Any]:
        return {
            "provider": "lm_studio",
            "base_url": DEFAULT_LOCAL_BASE_URL,
            "doctor_required": True,
            "defer_without_hermes": True,
        }

    def curator_mapping(self) -> dict[str, Any]:
        return {
            "enabled": False,
            "mode": "report_first",
            "status_required": True,
            "report_required": True,
            "pinned_skills_mutable": False,
            "pinned_skills": ["immune_system", "financial_router", "operator_interface"],
        }

    def plugin_hooks_mapping(self) -> dict[str, Any]:
        return {
            "required_hooks": ["pre_tool_call", "pre_approval_request", "post_approval_response"],
            "fail_closed_for": ["g3_paid_spend", "sheriff_block", "runtime_halt"],
        }

    def skill_config(self) -> dict[str, Any]:
        return {
            "profile_name": self.config.profile_name,
            "repo_contract_version": 1,
            "construction_phase": self.config.construction_phase,
            "runtime": self.runtime_mapping(),
            "network_controls": self.network_controls(),
            "gateway": self.gateway_mapping(),
            "workspace": self.workspace_mapping(),
            "routing": {
                "local_endpoint": DEFAULT_LOCAL_BASE_URL,
                "local_model": DEFAULT_LOCAL_MODEL,
                "fallback_endpoint": DEFAULT_LOCAL_BASE_URL,
                "strong_model_strategy": "frontier-if-configured-else-local",
                "max_api_spend_usd": self.config.max_api_spend_usd,
            },
            "local_provider": self.local_provider_mapping(),
            "curator": self.curator_mapping(),
            "plugin_hooks": self.plugin_hooks_mapping(),
            "dangerous_command_families": {
                name: list(variants)
                for name, variants in EXPECTED_DANGEROUS_COMMAND_FAMILIES.items()
            },
        }

    def config_document(self) -> dict[str, Any]:
        return {
            "model": {
                "provider": "custom",
                "default": DEFAULT_LOCAL_MODEL,
                "base_url": DEFAULT_LOCAL_BASE_URL,
                "api_key": "local-construction",
            },
            "fallback_model": {
                "provider": "main",
                "model": DEFAULT_STRONG_MODEL,
            },
            "approvals": {
                "mode": self.approval_mode,
            },
            "terminal": {
                "backend": "local",
            },
            "checkpoints": {
                "enabled": True,
                "max_snapshots": 20,
            },
            "dangerous_commands": self.representative_dangerous_commands,
            "skills": {
                "config": {
                    PROFILE_CONFIG_SKILL_KEY: self.skill_config(),
                }
            },
        }

    def spec_profile_document(self) -> dict[str, Any]:
        return {
            "profile": self.config.profile_name,
            "routing": {
                "local_model": {
                    "provider": "custom",
                    "default": DEFAULT_LOCAL_MODEL,
                    "base_url": DEFAULT_LOCAL_BASE_URL,
                },
                "strong_model": {
                    "strategy": "frontier-if-configured-else-local",
                    "fallback_model": {
                        "provider": "main",
                        "model": DEFAULT_STRONG_MODEL,
                    },
                },
            },
            "limits": {
                "max_api_spend_usd": self.config.max_api_spend_usd,
            },
            "approvals": {
                "mode": self.approval_mode,
            },
            "dangerous_commands": self.representative_dangerous_commands,
            "runtime": self.runtime_mapping(),
            "network_controls": self.network_controls(),
            "gateway": self.gateway_mapping(),
            "workspace": self.workspace_mapping(),
            "local_provider": self.local_provider_mapping(),
            "curator": self.curator_mapping(),
            "plugin_hooks": self.plugin_hooks_mapping(),
        }

    def generated_checks(
        self,
        actual_config_doc: dict[str, Any] | None,
        actual_spec_profile_doc: dict[str, Any] | None,
    ) -> dict[str, bool]:
        expected_config_doc = self.config_document()
        expected_spec_profile_doc = self.spec_profile_document()
        return {
            "config_yaml_shape": actual_config_doc is not None and contains_subset(actual_config_doc, expected_config_doc),
            "spec_profile_yaml_shape": actual_spec_profile_doc is not None
            and contains_subset(actual_spec_profile_doc, expected_spec_profile_doc),
            "profile_name": nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "profile_name")
            == self.config.profile_name,
            "repo_contract_version": nested_get(
                actual_config_doc or {},
                "skills",
                "config",
                PROFILE_CONFIG_SKILL_KEY,
                "repo_contract_version",
            )
            == self.skill_config()["repo_contract_version"],
            "local_model": nested_get(actual_config_doc or {}, "model", "provider") == "custom"
            and nested_get(actual_config_doc or {}, "model", "default") == DEFAULT_LOCAL_MODEL
            and nested_get(actual_config_doc or {}, "model", "base_url") == DEFAULT_LOCAL_BASE_URL,
            "fallback_model": nested_get(actual_config_doc or {}, "fallback_model", "provider") == "main"
            and nested_get(actual_config_doc or {}, "fallback_model", "model") == DEFAULT_STRONG_MODEL,
            "max_api_spend_zero": nested_get(
                actual_config_doc or {},
                "skills",
                "config",
                PROFILE_CONFIG_SKILL_KEY,
                "routing",
                "max_api_spend_usd",
            )
            == self.config.max_api_spend_usd,
            "approvals_manual": nested_get(actual_config_doc or {}, "approvals", "mode") == self.approval_mode,
            "dangerous_commands": contains_subset(
                nested_get(actual_config_doc or {}, "dangerous_commands") or [],
                self.representative_dangerous_commands,
            ),
            "runtime_paths": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "runtime") or {},
                self.runtime_mapping(),
            ),
            "network_controls": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "network_controls") or {},
                self.network_controls(),
            ),
            "gateway_mapping": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "gateway") or {},
                self.gateway_mapping(),
            ),
            "gateway_expected_tools": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "gateway", "expected_tools") or [],
                self.gateway_mapping()["expected_tools"],
            ),
            "workspace_mapping": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "workspace") or {},
                self.workspace_mapping(),
            ),
            "workspace_preferred_surfaces": contains_subset(
                nested_get(
                    actual_config_doc or {},
                    "skills",
                    "config",
                    PROFILE_CONFIG_SKILL_KEY,
                    "workspace",
                    "preferred_surfaces",
                )
                or [],
                self.workspace_mapping()["preferred_surfaces"],
            ),
            "local_provider_lm_studio": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "local_provider") or {},
                self.local_provider_mapping(),
            ),
            "curator_report_first": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "curator") or {},
                self.curator_mapping(),
            ),
            "plugin_hooks_v012": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "plugin_hooks") or {},
                self.plugin_hooks_mapping(),
            ),
        }

    def live_config_checks(self, actual_config_doc: dict[str, Any] | None) -> dict[str, bool]:
        expected_config_doc = self.config_document()
        return {
            "config_probe_shape": actual_config_doc is not None and contains_subset(actual_config_doc, expected_config_doc),
            "profile_name": nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "profile_name")
            == self.config.profile_name,
            "repo_contract_version": nested_get(
                actual_config_doc or {},
                "skills",
                "config",
                PROFILE_CONFIG_SKILL_KEY,
                "repo_contract_version",
            )
            == self.skill_config()["repo_contract_version"],
            "local_model": nested_get(actual_config_doc or {}, "model", "provider") == "custom"
            and nested_get(actual_config_doc or {}, "model", "default") == DEFAULT_LOCAL_MODEL
            and nested_get(actual_config_doc or {}, "model", "base_url") == DEFAULT_LOCAL_BASE_URL,
            "fallback_model": nested_get(actual_config_doc or {}, "fallback_model", "provider") == "main"
            and nested_get(actual_config_doc or {}, "fallback_model", "model") == DEFAULT_STRONG_MODEL,
            "max_api_spend_zero": nested_get(
                actual_config_doc or {},
                "skills",
                "config",
                PROFILE_CONFIG_SKILL_KEY,
                "routing",
                "max_api_spend_usd",
            )
            == self.config.max_api_spend_usd,
            "approvals_manual": nested_get(actual_config_doc or {}, "approvals", "mode") == self.approval_mode,
            "dangerous_commands": contains_subset(
                nested_get(actual_config_doc or {}, "dangerous_commands") or [],
                self.representative_dangerous_commands,
            ),
            "runtime_paths": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "runtime") or {},
                self.runtime_mapping(),
            ),
            "network_controls": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "network_controls") or {},
                self.network_controls(),
            ),
            "gateway_mapping": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "gateway") or {},
                self.gateway_mapping(),
            ),
            "gateway_expected_tools": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "gateway", "expected_tools") or [],
                self.gateway_mapping()["expected_tools"],
            ),
            "workspace_mapping": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "workspace") or {},
                self.workspace_mapping(),
            ),
            "workspace_preferred_surfaces": contains_subset(
                nested_get(
                    actual_config_doc or {},
                    "skills",
                    "config",
                    PROFILE_CONFIG_SKILL_KEY,
                    "workspace",
                    "preferred_surfaces",
                )
                or [],
                self.workspace_mapping()["preferred_surfaces"],
            ),
            "local_provider_lm_studio": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "local_provider") or {},
                self.local_provider_mapping(),
            ),
            "curator_report_first": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "curator") or {},
                self.curator_mapping(),
            ),
            "plugin_hooks_v012": contains_subset(
                nested_get(actual_config_doc or {}, "skills", "config", PROFILE_CONFIG_SKILL_KEY, "plugin_hooks") or {},
                self.plugin_hooks_mapping(),
            ),
        }

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class IntegrationConfig:
    """Configuration for the Hermes integration layer."""

    data_dir: str = "~/.hermes/data/"
    skills_dir: str = "~/.hermes/skills/hybrid-autonomous-ai/"
    checkpoints_dir: str = "~/.hermes/skills/hybrid-autonomous-ai/checkpoints/"
    alerts_dir: str = "~/.hermes/alerts/"
    max_api_spend_usd: float = 0.00
    construction_phase: bool = True
    profile_name: str = "hybrid-autonomous-ai"
    proxy_bind_url: str = "http://127.0.0.1:18080"
    outbound_allowlist_domains: tuple[str, ...] = ("localhost", "127.0.0.1", "::1")
    outbound_allowlist_ports: tuple[int, ...] = (11434, 8080, 8443)
    hermes_gateway_url: str = "http://127.0.0.1:8080"
    hermes_workspace_url: str = "http://127.0.0.1:3000"

    def resolve_paths(self) -> "IntegrationConfig":
        return IntegrationConfig(
            data_dir=str(Path(self.data_dir).expanduser()),
            skills_dir=str(Path(self.skills_dir).expanduser()),
            checkpoints_dir=str(Path(self.checkpoints_dir).expanduser()),
            alerts_dir=str(Path(self.alerts_dir).expanduser()),
            max_api_spend_usd=self.max_api_spend_usd,
            construction_phase=self.construction_phase,
            profile_name=self.profile_name,
            proxy_bind_url=self.proxy_bind_url,
            outbound_allowlist_domains=tuple(self.outbound_allowlist_domains),
            outbound_allowlist_ports=tuple(self.outbound_allowlist_ports),
            hermes_gateway_url=self.hermes_gateway_url,
            hermes_workspace_url=self.hermes_workspace_url,
        )

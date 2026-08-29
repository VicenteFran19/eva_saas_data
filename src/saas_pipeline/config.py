"""Carga de configuracion jerarquica: base -> env -> tenant -> overrides de CLI.

La resolucion sigue el orden definido en la arquitectura (seccion 5.8):
  config/base.yaml            <- defaults
  config/env/<env>.yaml       <- overrides por ambiente
  config/tenants/<tenant>.yaml <- overrides por tenant (si aplica; "all" no carga tenant.yaml)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from omegaconf import OmegaConf

CONFIG_ROOT = Path(__file__).resolve().parents[2] / "config"


def load_config(
    env: str,
    tenant: str,
    start_date: str | None = None,
    end_date: str | None = None,
    fail_fast: bool | None = None,
    fail_on_critical: bool | None = None,
):
    """Compone la configuracion final para una corrida.

    Orden de merge (cada nivel sobreescribe al anterior): base -> env -> tenant -> CLI.
    """
    base = OmegaConf.load(CONFIG_ROOT / "base.yaml")

    env_path = CONFIG_ROOT / "env" / f"{env}.yaml"
    if not env_path.exists():
        raise FileNotFoundError(f"No existe config de ambiente: {env_path}")
    env_cfg = OmegaConf.load(env_path)

    merged = OmegaConf.merge(base, env_cfg)

    if tenant != "all":
        tenant_path = CONFIG_ROOT / "tenants" / f"{tenant}.yaml"
        if not tenant_path.exists():
            raise FileNotFoundError(
                f"No existe config para tenant '{tenant}': {tenant_path}. "
                "Ver docs/onboarding-tenant.md para dar de alta un tenant nuevo."
            )
        tenant_cfg = OmegaConf.load(tenant_path)
        merged = OmegaConf.merge(merged, tenant_cfg)

    cli_overrides = {"execution": {"tenant": tenant}}
    if start_date:
        cli_overrides["execution"]["start_date"] = start_date
    if end_date:
        cli_overrides["execution"]["end_date"] = end_date
    if fail_fast is not None:
        cli_overrides["execution"]["fail_fast"] = fail_fast
    if fail_on_critical is not None:
        cli_overrides["quality"] = {"fail_on_critical": fail_on_critical}

    merged = OmegaConf.merge(merged, OmegaConf.create(cli_overrides))
    OmegaConf.resolve(merged)
    return merged


def list_available_tenants() -> list[str]:
    """Tenants dados de alta (un yaml por tenant en config/tenants/)."""
    return sorted(p.stem for p in (CONFIG_ROOT / "tenants").glob("*.yaml"))


@dataclass(frozen=True)
class LayerPaths:
    """Paths resueltos para un tenant y tabla dados, siguiendo la convencion de 5.2."""

    tenant: str
    table: str
    bronze_base: str
    silver_base: str
    gold_base: str
    quarantine_root: str
    quality_logs: str

    def bronze_table(self) -> str:
        return f"{self.bronze_base}/{self.tenant}/{self.table}"

    def silver_table(self) -> str:
        return f"{self.silver_base}/{self.tenant}/{self.table}"

    def gold_table(self) -> str:
        return f"{self.gold_base}/{self.tenant}/{self.table}"

    def bronze_quarantine(self) -> str:
        return f"{self.quarantine_root}/bronze_quarantine/{self.tenant}/{self.table}"

    def silver_quarantine(self) -> str:
        return f"{self.quarantine_root}/silver_quarantine/{self.tenant}/{self.table}"


def paths_for(cfg, tenant: str, table: str) -> LayerPaths:
    return LayerPaths(
        tenant=tenant,
        table=table,
        bronze_base=cfg.paths.bronze,
        silver_base=cfg.paths.silver,
        gold_base=cfg.paths.gold,
        quarantine_root=cfg.paths.quarantine_root,
        quality_logs=cfg.paths.quality_logs,
    )

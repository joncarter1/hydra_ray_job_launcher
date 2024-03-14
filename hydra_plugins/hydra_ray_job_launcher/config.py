"""Registering configuration for the launcher plugin with Hydra."""

import os
from dataclasses import dataclass, field
from typing import Any

from hydra.core.config_store import ConfigStore

DEFAULT_ENV_VARS = {'USER': os.environ['USER']}
DEFAULT_EXCLUDES = [
    '/outputs',
    '/multirun',
    '/lightning_logs',
]  # Folders relative to workdir to ignore shipping to cluster


@dataclass
class RuntimeEnvConf:
    conda: Any | None = None
    py_modules: list[str] | None = None
    env_vars: dict[str, str] = field(default_factory=lambda: DEFAULT_ENV_VARS.copy())
    working_dir: str = '.'
    excludes: list[str] = field(default_factory=lambda: DEFAULT_EXCLUDES.copy())


@dataclass
class RayInitConfig:
    """Configuration for the head node when calling ray.init"""

    runtime_env: RuntimeEnvConf = field(default_factory=RuntimeEnvConf)
    num_cpus: int | None = None
    num_gpus: int | None = None
    resources: dict[str, float] | None = None  # Custom resources (e.g. TPUs?)
    log_to_driver: bool = False


@dataclass
class RayJobConfig:
    """Configuration for ray.remote tasks within Hydra jobs."""

    runtime_env: RuntimeEnvConf | None = None  # Default to runtime env from the ray.init call
    num_cpus: int | None = None
    num_gpus: int | None = None
    resources: dict[str, float] | None = None  # Custom resources (e.g. TPUs?)


@dataclass
class RayConfig:
    init_cfg: RayInitConfig = field(default_factory=RayInitConfig)
    job_cfg: RayJobConfig = field(default_factory=RayJobConfig)


@dataclass
class RayJobLauncherConf:
    _target_: str = 'hydra_plugins.hydra_ray_job_launcher.launcher.RayJobLauncher'
    address: str = 'http://localhost:8265'  # Connect to a local Ray cluster.
    ray: RayConfig = field(default_factory=RayConfig)  # Ray specific configuration
    tail_logs: bool = False  # Tail job logs asynchronously after submission.
    tolerate_error: bool = True  # Tolerate errors in any of the remote Ray tasks
    max_parallel: int | None = None  # Maximum parallel Hydra jobs


ConfigStore.instance().store(
    group='hydra/launcher',
    name='ray_job',
    node=RayJobLauncherConf,
    provider='hydra_ray_job_launcher',
)

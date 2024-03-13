"""
Launcher plugin interface for executing Hydra jobs on a Ray cluster using the JobSubmissionClient.
"""

import logging
import os
import tempfile
from typing import Optional, Sequence

from hydra.core.utils import JobReturn, JobStatus
from hydra.plugins.launcher import Launcher
from hydra.types import HydraContext, TaskFunction
from omegaconf import DictConfig, OmegaConf, open_dict
from ray.job_submission import JobSubmissionClient

from ._launch_utils import construct_working_dir
from .config import RayConfig
from .tracing import tail_job_logs

logger = logging.getLogger(__name__)


class RayJobLauncher(Launcher):
    """Hydra plugin for executing jobs on a Ray cluster using the Job submission client.

    The script and configuration is pickled, shipped to and executed on the Ray cluster.
    """

    def __init__(
        self,
        address: str,
        ray: RayConfig,
        tail_logs: bool = False,
        tolerate_error: bool = True,
        max_parallel: Optional[int] = None,
    ) -> None:
        self.address = address
        self.ray_config = ray
        self.tail_logs = tail_logs
        self.tolerate_error = tolerate_error
        self.max_parallel = max_parallel
        self.config: Optional[DictConfig] = None
        self.task_function: Optional[TaskFunction] = None
        self.hydra_context: Optional[HydraContext] = None

    def setup(
        self,
        *,
        hydra_context: HydraContext,
        task_function: TaskFunction,
        config: DictConfig,
    ) -> None:
        """
        Sets this launcher instance up.
        """
        self.config = config
        self.hydra_context = hydra_context
        self.task_function = task_function

    def launch(self, job_overrides: Sequence[Sequence[str]], initial_job_idx: int) -> Sequence[JobReturn]:
        """Launch jobs on the Ray cluster.

        :param job_overrides: a batch of job arguments
        :param initial_job_idx: Initial job idx. used by sweepers that executes several batches
        """
        logger.info(f'Launching with {job_overrides=} and {initial_job_idx=}')
        # Create Hydra sweep configurations
        sweep_configs = []
        for idx, overrides in enumerate(job_overrides):
            sweep_config = self.hydra_context.config_loader.load_sweep_config(self.config, list(overrides))
            with open_dict(sweep_config):  # Give jobs a number and ID
                sweep_config.hydra.job.num = idx
                sweep_config.hydra.job.id = f'{sweep_config.hydra.job.name}_{idx}'
            sweep_configs.append(sweep_config)
        runtime_env = self.ray_config.init_cfg.runtime_env
        working_dir = runtime_env.working_dir
        excludes = runtime_env.excludes
        logger.info(f'Connecting to Ray cluster: {self.address}')

        client = JobSubmissionClient(address=self.address)
        # Create tmp dir for runtime environment.
        # Submit Ray job
        with tempfile.TemporaryDirectory() as tmpdirname:
            # Make a copy of the working tree + necessary pickles for submitting to cluster.
            output_dir = os.path.join(tmpdirname, 'ray')
            construct_working_dir(
                working_dir=working_dir,
                output_dir=output_dir,
                excludes=excludes,
                hydra_context=self.hydra_context,
                sweep_configs=sweep_configs,
                task_function=self.task_function,
                max_parallel=self.max_parallel,
            )
            runtime_env.working_dir = output_dir
            # The _remote_invoke.py script calls our main function on the cluster.
            # We add the name of the original Hydra script to the entrypoint for observability.
            job_id = client.submit_job(
                entrypoint=f'python _remote_invoke.py {self.task_function.__name__}',
                runtime_env=OmegaConf.to_container(runtime_env, resolve=True),
            )
            logger.info(f'Job submitted - {job_id}!')
        if self.tail_logs:
            tail_job_logs(client=client, job_id=job_id)

        return [
            JobReturn(
                status=JobStatus.COMPLETED,
                _return_value=1,
            )
            for _ in sweep_configs
        ]

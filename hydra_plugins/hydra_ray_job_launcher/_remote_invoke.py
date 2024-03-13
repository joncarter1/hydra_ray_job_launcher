"""Remote execution of a pickled Hydra function and configs on a Ray cluster.

Based on Facebook's old implementation: https://github.com/facebookresearch/hydra/blob/main/plugins/hydra_ray_launcher/hydra_plugins/hydra_ray_launcher/_remote_invoke.py
(From Hydra distributed under MIT license: see_launch_utils.py)
"""

import logging
from pathlib import Path
from typing import Any

import ray
from _launch_utils import add_common_job_config, launch_job_on_ray, load_job_spec_pickle, load_task_function_pickle
from hydra.core.hydra_config import HydraConfig
from hydra.core.singleton import Singleton
from hydra.core.utils import JobReturn, setup_globals
from omegaconf import OmegaConf
from ray.exceptions import RayTaskError

logger = logging.getLogger(__name__)


def handle_run(
    completed_run: Any,
    tolerate_error: bool = True,
) -> None:
    """Handle results of a Hydra job executed as a Ray remote task.

    It is recommended that the Hydra LogJobReturn callback is configured
    to log the configuration that led to the failure, e.g.:
    ```
    hydra:
        callbacks:
            log:
                _target_: hydra.experimental.callbacks.LogJobReturnCallback
    ```
    The Oxehealth Slack callback is also capable of logging remote job failures.

    Args:
    completed_run: Completed Ray remote task.
    tolerate_error: Whether to tolerate errors in any of the jobs.
        If False, the entire Hydra multi-run will fail.
    """
    try:
        completed_run: JobReturn = ray.get(completed_run)
    except RayTaskError as e:
        if not tolerate_error:
            raise e
        logger.warning('Failed run:')
        logger.warning(e)
    return None


def launch_jobs() -> None:
    """Launch Hydra jobs on the Ray cluster.
    This function is called on the head of the Ray cluster.

    It is invoked using the Ray Job submission client when using the Hydra launcher:
    ```
    from ray.job_submission import JobSubmissionClient

    client = JobSubmissionClient(RAY_JOB_API_ADDRESS)
    job_id = client.submit_job(entrypoint='python _remote_invoke.py script.py')
    ```
    It is not intended to be used directly.
    """
    temp_dir = '.'
    job_spec = load_job_spec_pickle(temp_dir=temp_dir)
    hydra_context = job_spec.hydra_context
    singleton_state = job_spec.singleton_state
    task_function = load_task_function_pickle(temp_dir=temp_dir)
    sweep_dir = None

    ray.init()

    job_refs = []
    max_parallel = job_spec.max_parallel
    for i, sweep_config in enumerate(job_spec.sweep_configs):
        setup_globals()
        Singleton.set_state(singleton_state)
        HydraConfig.instance().set_config(sweep_config)

        tolerate_error = sweep_config.hydra.launcher.tolerate_error
        ray_remote_cfg = sweep_config.hydra.launcher.ray.job_cfg
        ray_remote_cfg = OmegaConf.to_container(ray_remote_cfg, resolve=True)
        ray_remote_cfg = add_common_job_config(ray_remote_cfg, job_spec)
        if not sweep_dir:
            sweep_dir = Path(str(HydraConfig.get().sweep.dir))
            sweep_dir.mkdir(parents=True, exist_ok=True)

        # Execute Hydra jobs as Ray remote tasks. Throttle parallelism.
        if max_parallel is not None:
            if len(job_refs) > max_parallel:
                num_ready_needed = i - max_parallel
                ray.wait(job_refs, num_returns=num_ready_needed)
        job_ref = launch_job_on_ray(hydra_context, ray_remote_cfg, sweep_config, task_function, singleton_state)
        job_refs.append(job_ref)

    ongoing_runs = job_refs

    while bool(ongoing_runs):
        (completed_run,), ongoing_runs = ray.wait(ongoing_runs, num_returns=1)
        handle_run(
            completed_run,
            tolerate_error=tolerate_error,
        )
    # Stop Ray processes
    ray.shutdown(_exiting_interpreter=True)


if __name__ == '__main__':
    launch_jobs()

"""Launcher utils.

Based on Facebook's implementation:
https://github.com/facebookresearch/hydra/blob/main/plugins/hydra_ray_launcher/hydra_plugins/hydra_ray_launcher

Modified under the MIT License:

Copyright (c) Facebook, Inc. and its affiliates.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import logging
import os
import sys
from dataclasses import dataclass
from shutil import copy2, copytree, ignore_patterns
from typing import Any, Dict, List, Optional

import cloudpickle
import ray
from hydra.core.hydra_config import HydraConfig
from hydra.core.singleton import Singleton
from hydra.core.utils import JobReturn, run_job, setup_globals
from hydra.types import HydraContext, TaskFunction
from omegaconf import DictConfig

logger = logging.getLogger(__name__)

DEFAULT_EXCLUDES = ['multirun', 'outputs', 'logs', '*.log', '*.egg-info']


@dataclass
class JobSpec:
    function_name: str
    hydra_context: HydraContext
    sweep_configs: List[DictConfig]
    singleton_state: Any
    encrypted_token: Optional[str] = None
    max_parallel: Optional[int] = None


def construct_working_dir(
    working_dir: str,
    output_dir: str,
    excludes: List[str],
    hydra_context: HydraContext,
    sweep_configs: List[DictConfig],
    task_function: TaskFunction,
    max_parallel: Optional[int] = None,
):
    """Construct the working directory that will be shipped to the Ray cluster.

    1. Copies original Ray working directory into temp location.
    2. Adds copy of run script for unpickling the task function.
    3. Adds job pickles to working directory.
    TODO: Check working directory size minus excludes first!
    """
    if excludes is None:
        all_excludes = DEFAULT_EXCLUDES
    else:
        all_excludes = excludes + DEFAULT_EXCLUDES
    ignore = ignore_patterns(*all_excludes)

    copytree(working_dir, output_dir, ignore=ignore)
    # Get name of function for import prior to unpickling on cluster
    function_name = task_function.__name__
    job_spec = JobSpec(
        function_name=function_name,
        hydra_context=hydra_context,
        sweep_configs=sweep_configs,
        singleton_state=Singleton.get_state(),
        max_parallel=max_parallel,
    )
    # Pickle job specification
    with open(os.path.join(output_dir, 'remote_hydra_job.pkl'), 'wb') as file:
        cloudpickle.dump(job_spec, file)
    # Pickle the task function
    with open(os.path.join(output_dir, 'task_function.pkl'), 'wb') as file:
        cloudpickle.dump(task_function, file)
    # Copy launcher files into the temp. working directory
    for remote_file in ['_remote_invoke.py', '_launch_utils.py']:
        file_path = os.path.join(os.path.dirname(__file__), remote_file)
        copy2(file_path, os.path.join(output_dir, remote_file))
    # Copy entrypoint module to cluster working directory
    entrypoint = sys.argv[0]
    copy2(entrypoint, os.path.join(output_dir, '_remote_module.py'))


def load_job_spec_pickle(temp_dir) -> JobSpec:
    """Load the job spec pickle on the Ray cluster."""
    with open(os.path.join(temp_dir, 'remote_hydra_job.pkl'), 'rb') as f:
        return cloudpickle.load(f)  # nosec


def load_task_function_pickle(temp_dir) -> TaskFunction:
    """Load the pickled task function on the Ray cluster."""
    with open(os.path.join(temp_dir, 'task_function.pkl'), 'rb') as f:
        return cloudpickle.load(f)  # nosec


def _run_job(
    hydra_context: HydraContext,
    sweep_config: DictConfig,
    task_function: TaskFunction,
    singleton_state: Dict[Any, Any],
) -> JobReturn:
    """Launch job using task function."""
    setup_globals()
    Singleton.set_state(singleton_state)
    HydraConfig.instance().set_config(sweep_config)
    result = run_job(
        hydra_context=hydra_context,
        task_function=task_function,
        config=sweep_config,
        job_dir_key='hydra.sweep.dir',
        job_subdir_key='hydra.sweep.subdir',
    )
    # Raise any errors inside the job so that they caught by Ray (and marked as failures)
    if isinstance(exc := result._return_value, Exception):
        raise exc
    else:
        return result


def add_common_job_config(job_cfg: dict[str, Any], job_spec: JobSpec):
    """Add common environment variables to the job runtime environment.

    Currently just adds a Slack thread ts as an env var so all Slack messaging
    within the application can use it.
    """
    _job_cfg = job_cfg.copy()
    if 'runtime_env' not in job_cfg or _job_cfg['runtime_env'] is None:
        _job_cfg['runtime_env'] = {}
    if 'env_vars' not in _job_cfg['runtime_env']:
        _job_cfg['runtime_env']['env_vars'] = {}

    return _job_cfg


def launch_job_on_ray(
    hydra_context: HydraContext,
    ray_remote_cfg: dict[str, Any],
    sweep_config: DictConfig,
    task_function: TaskFunction,
    singleton_state: Any,
) -> Any:
    """Launch a Hydra job on a Ray cluster."""
    if bool(ray_remote_cfg):
        run_job_ray = ray.remote(**ray_remote_cfg)(_run_job)
    else:
        run_job_ray = ray.remote(_run_job)
    return run_job_ray.remote(
        hydra_context=hydra_context,
        sweep_config=sweep_config,
        task_function=task_function,
        singleton_state=singleton_state,
    )

Hydra Ray Job Launcher
========

Hydra launcher plugin for running Hydra jobs as tasks on a Ray cluster.

This is a minor modification of Facebook's original implementation which is broken:
https://github.com/facebookresearch/hydra/tree/main/plugins/hydra_ray_launcher
and combines ideas from the ray and ray_aws launchers.

Job submission is done via the job submission client, rather than the interactive client as done in Facebook's implementation.

Example usage
--------
Running a script on a Ray cluster at the specific address:
```bash
python script.py --multirun hydra/launcher=ray_job hydra.launcher.address=http://ip:port
```

Environment set-up
--------

```
conda env create --file env/environment.yaml
pre-commit install
```


TODO: File a PR to the official implementation...

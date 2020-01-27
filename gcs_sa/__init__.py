#!/usr/bin/env python3
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Google Cloud Storage smart archiver main entry point.
"""
import logging
import warnings
import click

from gcs_sa.config import config_to_string, set_config
from gcs_sa.utils import set_program_log_level
from gcs_sa.cli.cooldown import cooldown_command
from gcs_sa.cli.install import install_command
from gcs_sa.cli.warmup import warmup_command

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials")

logging.basicConfig()
LOG = logging.getLogger(__name__)


@click.group()
@click.option("-c",
              "--config_file",
              required=False,
              help="Path to the configuration file to use.",
              default="./default.cfg")
@click.option("-l",
              "--log_level",
              required=False,
              help="Set log level. Overrides configuration.",
              default=None)
@click.pass_context
def main(context: object = object(), **kwargs) -> None:
    """
    Smart archiver for GCS, which moves objects to storage classes based
    on access patterns to optimize for cost.
    """
    context.obj = kwargs


def init(config_file: str = "./default.cfg", log_level: str = None) -> None:
    """
    Top-level initialization.

    Keyword Arguments:
        config_file {str} -- Path to config file. (default: {"./default.cfg"})
        log_level {str} -- Desired log level. (default: {None})
    """
    config = set_config(config_file)
    print("Configuration parsed: \n{}".format(config_to_string(config)))
    set_program_log_level(log_level, config)


@main.command()
@click.pass_context
def cooldown(context: object) -> None:
    """
    Process cool-down evaluations. A query will be done to only
    find cool-down candidate objects.
    """
    init(**context.obj)
    return cooldown_command()


@main.command()
@click.pass_context
def warmup(context: object) -> None:
    """
    Process warm-up evaluations. A query will be done to only
    find warm-up candidate objects.
    """
    init(**context.obj)
    return warmup_command()


@main.command()
@click.pass_context
@click.option("-y",
              "--yes",
              help="Agree to everything, no interactivity.",
              is_flag=True)
def install(context: object, yes: bool = False) -> None:
    """
    Enable IAM audit logging and a BQ sink for those logs. Installation takes seconds, and is idempotent (existing resources that meet the requirements will be used).
    """
    init(**context.obj)
    return install_command(yes)


if __name__ == "__main__":
    main()

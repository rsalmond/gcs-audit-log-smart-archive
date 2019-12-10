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
from gcs_sa.cli.catchup import catchup_command
from gcs_sa.cli.cooldown import cooldown_command
from gcs_sa.cli.doboth import doboth_command
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
def doboth(context: object) -> None:
    """
    Process both warm-up and cool-down evaluations for all objects.
    """
    init(**context.obj)
    return doboth_command()


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
@click.argument('buckets', nargs=-1, required=False, default=None)
@click.pass_context
def catchup(context: object, buckets: [str] = None) -> None:
    """
    Build the catchup table with all objects in your bucket(s).

    Optionally, provide a list of buckets (without gs://) to limit the scope
    to. By default, all buckets in the configured project will be processed
    into the table.

    This listing of objects can be UNIONed with the access log by setting the
    CATCHUP_TABLE value in the configuration file. If an object
    is only in the catchup table, its create date will be treated as last
    access. If the object is in the access log, it will be processed as usual.
    """
    init(**context.obj)
    return catchup_command(buckets)


if __name__ == "__main__":
    main()

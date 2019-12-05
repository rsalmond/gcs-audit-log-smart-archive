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

from gcs_sa.config import config_to_string, get_config
from gcs_sa.utils import set_program_log_level
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
def main(config_file, log_level) -> None:
    """
    Smart archiver for GCS, which moves objects to storage classes based
    on access patterns to optimize for cost.
    """
    config = get_config(config_file)
    print("Configuration parsed: \n{}".format(config_to_string(config)))
    set_program_log_level(log_level, config)


@main.command()
def cooldown():
    """
    Process cool-down evaluations. A query will be done to only
    find cool-down candidate objects.
    """
    return cooldown_command()


@main.command()
def doboth():
    """
    Process both warm-up and cool-down evaluations for all objects.
    """
    return doboth_command()


@main.command()
def warmup():
    """
    Process warm-up evaluations. A query will be done to only
    find warm-up candidate objects.
    """
    return warmup_command()

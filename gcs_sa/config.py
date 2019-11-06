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
Get and validate configuration for the smart archiver from
arguments and config files.
"""

# TODO: Validation, set_config for testing

import io
from argparse import Namespace
from configparser import ConfigParser
from gcs_sa.utils import memoize

CONFIG = None


def get_config(args: Namespace = None) -> ConfigParser:
    """Get the configuration by locating a config file, and building a
    configuration with items from these sources, in ascending priority:

    * Program defaults
    * The config file
    * Command argument overrides, where permitted

    The configuration file is only parsed if args is not None. Otherwise, a
    stored configuration file value is returned, which is initialized to None.

    Returns:
        dict -- The final configuration values.
    """
    global CONFIG

    if args:
        CONFIG = ConfigParser()
        CONFIG.read(args.config_file)
    check_configured(CONFIG)
    return CONFIG


def config_to_string(config: ConfigParser) -> str:
    """ConfigParser seems to only do nice formatting when writing
    to a file pointer. This function turns that output into a string.

    Arguments:
        config {ConfigParser} -- The ConfigParser object to represent.

    Returns:
        str -- The ConfigParser.write() output, as a string.
    """
    config_status = io.StringIO()
    config.write(config_status)
    config_status.seek(0)
    return config_status.read()

@memoize
def check_configured(config: ConfigParser) -> None:
    """Check that none of the values equal the sentinel "CONFIGURE_ME".

    Arguments:
        config {ConfigParser} -- The parsed configuration.

    Returns:
        None -- No errors were raised.

    Raises:
        ValueError -- Upon first encounter with a value == "CONFIGURE_ME".
    """
    for section in config.sections():
        for option in config[section]:
            value = config.get(section, option)
            if value == "CONFIGURE_ME":
                raise ValueError("Invalid configuration {}.{}={}".format
                                 (section, option, value))
    return

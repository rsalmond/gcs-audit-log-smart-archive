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
Get command line arguments.
"""

from argparse import ArgumentParser, Namespace

from gcs_sa.logging import validate_log_level


def get_args() -> Namespace:
    """Get the arguments given at the command line.

    Returns:
        dict -- Arguments and their values.
    """
    argp = ArgumentParser()
    argp.add_argument("-c",
                      "--config_file",
                      required=False,
                      help="Path to the configuration file to use.",
                      default="./default.cfg")
    argp.add_argument("-l",
                      "--log_level",
                      required=False,
                      help="Set log level. Overrides configuration.",
                      default=None)
    return _validate_args(argp.parse_args())


def _validate_args(args: Namespace) -> Namespace:
    """Validate arguments.

    Arguments:
        args {Namespace} -- The arguments to validate.

    Raises:
        ValueError: Invalid config file.
        ValueError: Invalid log level.

    Returns:
        Namespace -- The validated arguments.
    """
    if args.config_file:
        try:
            test = open(args.config_file, "r")
            test.close()
        except Exception as error:
            raise ValueError("Can't open config file: {}".format(
                args.config_file)) from error
    if args.log_level:
        if not validate_log_level(args.log_level):
            raise ValueError("{} is not a valid log level.".format(
                args.log_level))
    return args

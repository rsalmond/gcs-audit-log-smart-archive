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
Utility functions not specific to any submodule.
"""

from functools import wraps
from typing import Callable


def memoize(func: Callable) -> Callable:
    """Decorator to memoize a function.

    Arguments:
        func {func} -- The function to memoize.

    Returns:
        func -- A function with results cached for specific arguments.
    """
    # Define the dictionary for memoized responses
    memos = func.memos = {}

    @wraps(func)
    def memoized(*args, **kwargs):
        # devise a key based on string representation of arguments given
        # in this function call
        call = str(args) + str(kwargs)
        # if the result isn't stored, call the function and store it
        if call not in memos:
            memos[call] = func(*args, **kwargs)
        # return the stored value
        return memos[call]

    return memoized

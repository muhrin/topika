from __future__ import absolute_import

# Disable variable name conventions because otherwise all the variables
# in this file would have to be upper case
# pylint: disable=invalid-name

author_info = (('Martin Uhrin', 'martin.uhrin@gmail.com'),)

package_info = "A backport of aio-pika for Tornado to support python 2.7+."
package_license = "Apache Software License"

version_info = (0, 1, 3)

__author__ = ", ".join("{} <{}>".format(*info) for info in author_info)
__version__ = ".".join(tuple(map(str, version_info)))

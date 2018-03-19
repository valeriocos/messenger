#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2017 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335, USA.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>
#

import unittest

from perceval.backends.core.git import Git
from messenger.connectors import (ESConnector,
                                  FileConnector,
                                  GeneratorConnector,
                                  RedisConnector)
from messenger.messenger import Messenger


class TestMessenger(unittest.TestCase):

    def test_redis_es(self):

        redis = RedisConnector("redis://localhost/8")
        es = ESConnector("localhost", 9200, "items", create=True)

        m = Messenger(redis, es)
        m.transfer()

    def test_gen_es(self):

        git = Git('https://github.com/chaoss/grimoirelab-perceval.git', '/tmp/grimoirelab-perceval')
        commits = [commit for commit in git.fetch()]

        gen = GeneratorConnector(commits)
        es = ESConnector("localhost", 9200, "items", create=True)

        m = Messenger(gen, es)
        m.transfer(keep_alive=False)

    def test_file_es(self):

        fl = FileConnector("./grimoirelab-perceval.json")
        es = ESConnector("localhost", 9200, "items", create=True)

        m = Messenger(fl, es)
        m.transfer(keep_alive=False)


if __name__ == "__main__":
    unittest.main(warnings='ignore')
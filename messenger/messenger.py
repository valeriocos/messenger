# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2016 Bitergia
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
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>

import asyncio
import logging

from .connectors import READ_DONE

logger = logging.getLogger(__name__)


class Messenger:
    """Messenger class to transfer data from a source to a target storage.

    :param source_conn: a Connector object to interact with the source storage
    :param target_conn: a Connector object to interact with the target storage
    """

    version = '0.0.1'

    def __init__(self, source_conn, target_conn):
        self.source_conn = source_conn
        self.target_conn = target_conn

    def transfer(self, keep_alive=True):
        """Transfer the data from the source to the target storages.

        :param keep_alive: a flag to keeps listening to the source storage
        """

        data_queue = asyncio.Queue()
        loop = asyncio.get_event_loop()

        while True:
            try:
                loop.create_task(self.source_conn.read(data_queue))
                loop.run_until_complete(self.target_conn.write(data_queue))

                if not keep_alive:
                    break

            except KeyboardInterrupt:
                if data_queue.qsize() != 0:
                    data_queue.put(READ_DONE)
                    loop.run_until_complete(self.target_conn.write(data_queue))
                break

        if data_queue.qsize() != 0:
            logger.warning("%s items have been lost before closing the transfer", (data_queue.qsize))

        loop.close()

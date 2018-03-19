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

import json
import pickle

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import redis

from arthur.common import Q_STORAGE_ITEMS
from messenger.errors import ElasticError


READ_DONE = "read_done"


class Connector:
    """Abstract class for connectors.

    Base class to connect to data storages, which can span from Redis servers
    and ElasticSearch to generators and files.

    :param source: path of the data source (e.g., http link, file path)
    """

    def __init__(self, source):
        self.source = source


class FileConnector(Connector):
    """"""

    def __init__(self, path):
        super().__init__(path)

    async def read(self, data_queue):
        """Read data from a generator"""

        with open(self.source, 'r') as f:
            raw_item = ""
            for line in f:
                raw_item = raw_item + line

                if line == "}\n":
                    item = json.loads(raw_item)
                    await data_queue.put(item)
                    raw_item = ""

        await data_queue.put(READ_DONE)


class GeneratorConnector(Connector):
    def __init__(self, generator):
        super().__init__("")
        self.source = generator

    async def read(self, data_queue):
        """Read data from a generator"""

        for item in self.source:
            await data_queue.put(item)

        await data_queue.put(READ_DONE)


class RedisConnector(Connector):
    def __init__(self, url):
        super().__init__(url)
        self.conn = redis.StrictRedis.from_url(url)

    async def read(self, data_queue):
        """Read data from Redis queue"""

        pipe = self.conn.pipeline()
        pipe.lrange(Q_STORAGE_ITEMS, 0, -1)
        pipe.ltrim(Q_STORAGE_ITEMS, 1, 0)
        items = pipe.execute()[0]

        for item in items:
            item = pickle.loads(item)
            await data_queue.put(item)

        await data_queue.put(READ_DONE)


class ESConnector(Connector):

    BULK_SIZE = 100
    ITEM_TYPE = 'item'
    MAPPING_TEMPLATE = """
    { 
      "mappings": { 
        "%s": { 
            "dynamic": false,
            "properties":{  
                "origin": {  
                    "type": "keyword"
                },
                "tag": {
                    "type": "keyword"
                }
            }
        }
      }
    }
    """

    def __init__(self, host, port, index, item_type=ITEM_TYPE, create=False):
        url = ':'.join([host, str(port)])
        super().__init__(url)
        self.conn = Elasticsearch([{'host': host, 'port': port}])
        self.index = index
        self.item_type = item_type

        if not self.index:
            raise ElasticError
        
        if create:
            self.create_index()

    def create_index(self):
        """Clean the index to work with"""

        mapping = json.loads(ESConnector.MAPPING_TEMPLATE % self.item_type)

        if self.conn.indices.exists(index=self.index):
            res = self.conn.indices.delete(index=self.index)

            if not res['acknowledged']:
                raise ElasticError(cause="Index not deleted")

        res = self.conn.indices.create(index=self.index, body=mapping)

        if not res['acknowledged']:
            raise ElasticError(cause="Index not created")

    async def write(self, data_queue):
        """Write data to ElasticSearch"""

        items = []
        while True:
            item = await data_queue.get()

            if item == READ_DONE:
                self.__process_items(items)
                items.clear()
                data_queue.task_done()
                break

            items.append(item)
            data_queue.task_done()

            if len(items) == ESConnector.BULK_SIZE:
                self.__process_items(items)
                items.clear()

        if items:
            self.__process_items(items)
            items.clear()

    def __process_items(self, items):
        digest_items = []

        for item in items:
            es_item = {
                '_index': self.index,
                '_type': self.item_type,
                '_id': item['uuid'],
                '_source': item
            }

            digest_items.append(es_item)

        self.__write_to_es(digest_items)

    def __write_to_es(self, items):

        index = self.index

        errors = helpers.bulk(self.conn, items)[1]
        self.conn.indices.refresh(index=index)

        if errors:
            raise ElasticError(cause="Lost items from Arthur to ES (%s, %s). Error %s"
                                     % (errors[0]))

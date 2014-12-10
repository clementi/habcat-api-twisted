from twisted.web import server, resource
from twisted.internet import reactor, endpoints
from twisted.enterprise import adbapi

import json
import os
from repo import HabstarRepo


data_file_path = './data/habcat.sqlite'
dbpool = adbapi.ConnectionPool('sqlite3', data_file_path, check_same_thread=False)


class Habstar(resource.Resource):
    isLeaf = True

    def render_GET(self, request):
        def write_data(data):
            request.setHeader('content-type', 'application/json')
            request.write(json.dumps(data))
            request.finish()

        repo = HabstarRepo(dbpool)
        d = repo.get_habstars_within_distance_to(6, 10)
        d.addCallback(write_data)

        return server.NOT_DONE_YET

port = os.environ.get('PORT', '80')

endpoints.serverFromString(reactor, "tcp:" + port).listen(server.Site(Habstar()))
reactor.run()
from twisted.web import server, resource
from twisted.internet import reactor, endpoints, defer
from twisted.enterprise import adbapi

import json
import os
import re
from repo import HabstarRepo


data_file_path = './data/habcat.sqlite'
dbpool = adbapi.ConnectionPool('sqlite3', data_file_path, check_same_thread=False)


class Habstar(resource.Resource):
    isLeaf = True

    def render_GET(self, request):
        def write_data(data):
            if data:
                request.setHeader('Content-Type', 'application/json')
                request.setHeader('Access-Control-Allow-Origin', '*')
                request.write(json.dumps(data))
            else:
                request.setResponseCode(404)
                request.write(json.dumps({
                    'error': 'Not found'
                }))
            request.finish()

        repo = HabstarRepo(dbpool)

        d = defer.Deferred()

        hip_num_match = re.match("^/(\d+)$", request.path)
        if hip_num_match:
            d = repo.get_habstar(hip_num_match.groups()[0])
        elif request.path == '/':
            args = request.args
            action = (args.get('a') or ['browse'])[0]
            if action == 'browse':
                page_num = int((args.get('p') or ['1'])[0])
                d = repo.get_habstars(page_num)
            elif action == 'similar_mag':
                mag = args.get('m')
                page_num = int((args.get('p') or ['1'])[0])
                if mag and len(mag) != 0:
                    d = repo.get_habstars_with_similar_magnitude_to(mag[0], page_num)
            elif action == 'similar_color':
                color = args.get('c')
                page_num = int((args.get('p') or ['1'])[0])
                if color and len(color) != 0:
                    d = repo.get_habstars_with_similar_color_to(color[0], page_num)
            elif action == 'dist':
                dist = float((args.get('d') or ['10'])[0])
                ref_hip_num = args.get('r')[0]
                page_num = int((args.get('p') or ['1'])[0])
                d = repo.get_habstars_within_distance_to(ref_hip_num, dist, page_num)
        else:
            d = repo.get_habstar(-1)

        d.addCallback(write_data)

        return server.NOT_DONE_YET

port = os.environ.get('PORT', '80')

endpoints.serverFromString(reactor, "tcp:" + port).listen(server.Site(Habstar()))
reactor.run()

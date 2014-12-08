from twisted.web import server, resource
from twisted.internet import reactor, endpoints
from twisted.enterprise import adbapi

import json
import os


data_file_path = './data/habcat.sqlite'
dbpool = adbapi.ConnectionPool('sqlite3', data_file_path, check_same_thread=False)


class Habstar(resource.Resource):
    isLeaf = True

    def render_GET(self, request):
        def get_data():
            return dbpool.runQuery('SELECT * FROM habstar ORDER BY hipparchos_num ASC LIMIT 20;')

        def get_coord_system():
            coord_sys = request.args.get('c')
            if coord_sys:
                return coord_sys[0]
            else:
                return 'cel'

        def get_coordinates(coord_system, row):
            if coord_system == 'cart':
                return [row[16], row[17], row[18]]
            elif coord_system == 'cel':
                return [
                    row[15],
                    [row[1], row[2], row[3]],
                    [row[4], row[5], row[6]]
                ]
            else:
                return None

        def on_result(data):
            coord_sys = get_coord_system()

            if coord_sys not in ['cel', 'cart']:
                request.setResponseCode(500)
                request.setHeader('content-type', 'application/json')
                request.write(json.dumps({
                    'error': "Unknown coordinate system '{}'".format(coord_sys)
                }))

                request.finish()
                return

            result = []

            for row in data:
                coordinates = get_coordinates(coord_sys, row)
                if coordinates:
                    result.append({
                        'hip': row[0],
                        'loc': coordinates,
                        'mag': row[7],
                        'dist': row[15],
                        'parx': [row[8], row[9]],
                        'bmv': [row[10], row[11]],
                        'ccdm': row[12],
                        'hd': row[13],
                        'bd': row[14]
                    })

            request.setHeader('content-type', 'application/json')
            request.write(json.dumps(result))
            request.finish()

        d = get_data()
        d.addCallback(on_result)

        return server.NOT_DONE_YET


port = os.environ.get('PORT', 5000)

endpoints.serverFromString(reactor, "tcp:" + port).listen(server.Site(Habstar()))
reactor.run()
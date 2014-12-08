from twisted.web import server, resource
from twisted.internet import reactor, endpoints
from twisted.enterprise import adbapi

import json


data_file_path = './data/habcat.sqlite'
dbpool = adbapi.ConnectionPool('sqlite3', data_file_path, check_same_thread=False)


class Habstar(resource.Resource):
    isLeaf = True

    def render_GET(self, request):
        def get_data():
            return dbpool.runQuery('SELECT * FROM habstar LIMIT 1;')

        def on_result(data):
            result = []
            for row in data:
                if request.args.get('c') == 'cel':
                    loc = [
                        row[15],
                        [row[1], row[2], row[3]],
                        [row[4], row[5], row[6]]
                    ]
                else:
                    loc = [row[16], row[17], row[18]]

                result.append({
                    'hip': row[0],
                    'loc': loc,
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


endpoints.serverFromString(reactor, "tcp:5000").listen(server.Site(Habstar()))
reactor.run()
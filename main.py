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
        action = (request.args.get('a') or ['browse'])[0]

        page = int((request.args.get('p') or ['1'])[0])
        page_size = 20
        page_count = 857

        request.setHeader('content-type', 'application/json')

        if page <= 0 or page > page_count:
            request.setResponseCode(500)
            request.write(json.dumps({
                'error_code': 'PAGE_NUM_OUT_OF_RANGE',
                'message': 'The page number {} is out of range.'.format(page)
            }))
            request.finish()
            return server.NOT_DONE_YET

        def get_data():
            if action == 'browse':
                return dbpool.runQuery(
                    'SELECT * FROM habstar ORDER BY hipparchos_num ASC LIMIT ? OFFSET ?;',
                    (page_size, (page - 1) * page_size))
            elif action == 'get':
                hip_num = (request.args.get('hip') or ['6'])[0]
                return dbpool.runQuery(
                    'SELECT * FROM habstar WHERE hipparchos_num = ?', (hip_num))

        def on_result(data):
            if action == 'browse':
                result = {
                    'page': page,
                    'total_pages': page_count,
                    'habstars': []
                }
                for row in data:
                    result['habstars'].append({
                        'hip': row[0],
                        'loc_cel': {
                            'ra': [row[1], row[2], row[3]],
                            'dec': [row[4], row[5], row[6]]
                        },
                        'loc_cart': [row[16], row[17], row[18]],
                        'mag': row[7],
                        'parx': {
                            'val': row[8],
                            'sigma': row[9]
                        },
                        'bmv': {
                            'val': row[10],
                            'sigma': row[11]
                        },
                        'ccdm': row[12],
                        'hd': row[13],
                        'bd': row[14],
                        'dist': row[15]
                    })
            elif action == 'get':
                result = {
                    'hip': data[0][0],
                        'loc_cel': {
                            'ra': [data[0][1], data[0][2], data[0][3]],
                            'dec': [data[0][4], data[0][5], data[0][6]]
                        },
                        'loc_cart': [data[0][16], data[0][17], data[0][18]],
                        'mag': data[0][7],
                        'parx': {
                            'val': data[0][8],
                            'sigma': data[0][9]
                        },
                        'bmv': {
                            'val': data[0][10],
                            'sigma': data[0][11]
                        },
                        'ccdm': data[0][12],
                        'hd': data[0][13],
                        'bd': data[0][14],
                        'dist': data[0][15]
                }

            request.write(json.dumps(result))
            request.finish()

        d = get_data()
        d.addCallback(on_result)

        return server.NOT_DONE_YET


port = os.environ.get('PORT', '80')

endpoints.serverFromString(reactor, "tcp:" + port).listen(server.Site(Habstar()))
reactor.run()
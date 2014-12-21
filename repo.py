import itertools
import math


PAGE_SIZE = 20


class HabstarRepo(object):
    def __init__(self, dbpool):
        self._dbpool = dbpool

    def get_habstar(self, hip_num):
        query = 'SELECT * FROM habstar WHERE hipparchos_num = ?;'
        return self._dbpool.runQuery(query, (hip_num,)).addCallback(self._build_habstar)

    def _build_habstar(self, data):
        if len(data):
            row = data[0]
            hip_num, ra_hours, ra_minutes, ra_seconds, dec_degrees, dec_minutes, dec_seconds, johnson_mag, parx_mas, \
                sigma_parx_mas, bmv, sigma_bmv, ccdm, hd, bd, dist_pc, x_pc, y_pc, z_pc = row

            return {
                'hip': hip_num,
                'loc_cel': {
                    'ra': [ra_hours, ra_minutes, ra_seconds],
                    'dec': [dec_degrees, dec_minutes, dec_seconds]
                },
                'loc_cart_pc': [x_pc, y_pc, z_pc],
                'mag': johnson_mag,
                'parx_mas': {
                    'v': parx_mas,
                    's': sigma_parx_mas
                },
                'bmv': {
                    'v': bmv,
                    's': sigma_bmv
                },
                'ccdm': ccdm,
                'hd': hd,
                'bd': bd,
                'dist_pc': dist_pc
            }
        return None

    def get_habstars_within_distance_to(self, hip_num, distance, page_num):
        distance_query = """SELECT * FROM habstar
JOIN (SELECT x_pc AS ref_x, y_pc AS ref_y, z_pc AS ref_z FROM habstar WHERE hipparchos_num = ?)
WHERE x_pc > ref_x - ? AND x_pc < ref_x + ?
  AND y_pc > ref_y - ? AND y_pc < ref_y + ?
  AND z_pc > ref_z - ? AND z_pc < ref_z + ?;"""
        return self._dbpool.runQuery(
            distance_query,
            (hip_num, distance, distance, distance, distance, distance, distance)).addCallback(
                lambda data: self._build_habstars_by_distance(data, distance, page_num))

    def _build_habstars_by_distance(self, data, distance, page_num):
        habstars_page = {
            'page': page_num,
            'results': []
        }
        
        habstars = self._get_habstars_by_distance(data, distance)
        habstars_page['total_pages'] = int(math.ceil(len(habstars) / float(PAGE_SIZE)))

        # Do paging here
        paged_habstars = itertools.islice(habstars, (page_num - 1) * PAGE_SIZE, page_num * PAGE_SIZE)

        habstars_page['results'] = list(paged_habstars)

        return habstars_page

    def _build_habstar_by_distance(self, row):
        hip_num, ra_hours, ra_minutes, ra_seconds, dec_degrees, dec_minutes, dec_seconds, johnson_mag, parx_mas, \
            sigma_parx_mas, bmv, sigma_bmv, ccdm, hd, bd, dist_pc, x_pc, y_pc, z_pc, ref_x, ref_y, ref_z = row
        dist = self._distance(x_pc, y_pc, z_pc, ref_x, ref_y, ref_z)
        return {
            'hip': hip_num,
            'loc_cel': {
                'ra': [ra_hours, ra_minutes, ra_seconds],
                'dec': [dec_degrees, dec_minutes, dec_seconds]
            },
            'loc_cart_pc': [x_pc, y_pc, z_pc],
            'mag': johnson_mag,
            'parx_mas': {
                'v': parx_mas,
                's': sigma_parx_mas
            },
            'bmv': {
                'v': bmv,
                's': sigma_bmv
            },
            'ccdm': ccdm,
            'hd': hd,
            'bd': bd,
            'dist_pc': dist_pc,
            'ref_dist_pc': dist
        }

    def _distance(self, x1, y1, z1, x2, y2, z2):
        import math
        return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2 + (z1 - z2) ** 2)

    def get_habstars_with_similar_color_to(self, color, page_num):
        upper_color = float(color) * 1.05
        lower_color = float(color) * 0.95
        color_query = 'SELECT (SELECT COUNT() FROM habstar WHERE b_minus_v < ? AND b_minus_v > ?) AS total_habstars, * FROM habstar WHERE b_minus_v < ? AND b_minus_v > ? LIMIT ? OFFSET ?;'
        return self._dbpool.runQuery(color_query, (upper_color, lower_color, upper_color, lower_color, PAGE_SIZE, (page_num - 1) * PAGE_SIZE)).addCallback(lambda data: self._build_habstars(data, page_num))

    def _build_habstars(self, data, page_num):
        habstars_page = {
            'page': page_num,
            'total_pages': int(math.ceil(data[0][0] / float(PAGE_SIZE))),
            'results': []
        }
        for row in data:
            total_habstars, hip_num, ra_hours, ra_minutes, ra_seconds, dec_degrees, dec_minutes, dec_seconds, \
                johnson_mag, parx_mas, sigma_parx_mas, bmv, sigma_bmv, ccdm, hd, bd, dist_pc, x_pc, y_pc, z_pc = row
            habstars_page['results'].append({
                'hip': hip_num,
                'loc_cel': {
                    'ra': [ra_hours, ra_minutes, ra_seconds],
                    'dec': [dec_degrees, dec_minutes, dec_seconds]
                },
                'loc_cart_pc': [x_pc, y_pc, z_pc],
                'mag': johnson_mag,
                'parx_mas': {
                    'v': parx_mas,
                    's': sigma_parx_mas
                },
                'bmv': {
                    'v': bmv,
                    's': sigma_bmv
                },
                'ccdm': ccdm,
                'hd': hd,
                'bd': bd,
                'dist_pc': dist_pc
            })
        return habstars_page

    def get_habstars_with_similar_magnitude_to(self, mag, page_num):
        upper_mag = float(mag) * 1.05
        lower_mag = float(mag) * 0.95
        mag_query = 'SELECT (SELECT COUNT() FROM habstar WHERE johnson_mag < ? AND johnson_mag > ?) AS total_habstars, * FROM habstar WHERE johnson_mag < ? AND johnson_mag > ? LIMIT ? OFFSET ?;'
        return self._dbpool.runQuery(mag_query, (upper_mag, lower_mag, upper_mag, lower_mag, PAGE_SIZE, (page_num - 1) * PAGE_SIZE)).addCallback(lambda data: self._build_habstars(data, page_num))

    def get_habstars(self, page_num):
        query = 'SELECT (SELECT COUNT() FROM habstar) AS total_habstars, * FROM habstar LIMIT ? OFFSET ?;'
        return self._dbpool.runQuery(query, (PAGE_SIZE, (page_num - 1) * PAGE_SIZE)).addCallback(lambda data: self._build_habstars(data, page_num))

    def _get_habstars_by_distance(self, data, distance):
        return filter(lambda habstar: habstar['ref_dist_pc'] < distance, map(self._build_habstar_by_distance, data))
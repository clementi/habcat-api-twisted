class HabstarRepo(object):
    def __init__(self, dbpool):
        self._dbpool = dbpool

    def get_habstar(self, hip_num):
        query = 'SELECT * FROM habstar WHERE hipparchos_num = ?;'
        return self._dbpool.runQuery(query, (hip_num,)).addCallback(self._build_habstar)

    def _build_habstar(self, data):
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

    def get_habstars_within_distance_to(self, hip_num, distance):
        distance_query = """SELECT * FROM habstar
JOIN (SELECT x_pc AS ref_x, y_pc AS ref_y, z_pc AS ref_z FROM habstar WHERE hipparchos_num = ?)
WHERE x_pc > ref_x - ? AND x_pc < ref_x + ?
  AND y_pc > ref_y - ? AND y_pc < ref_y + ?
  AND z_pc > ref_z - ? AND z_pc < ref_z + ?;"""
        return self._dbpool.runQuery(
            distance_query,
            (hip_num, distance, distance, distance, distance, distance, distance)).addCallback(
                lambda data: self._build_habstars_by_distance(data, distance))

    def _build_habstars_by_distance(self, data, distance):
        habstars = []
        for row in data:
            hip_num, ra_hours, ra_minutes, ra_seconds, dec_degrees, dec_minutes, dec_seconds, johnson_mag, parx_mas, \
                sigma_parx_mas, bmv, sigma_bmv, ccdm, hd, bd, dist_pc, x_pc, y_pc, z_pc, ref_x, ref_y, ref_z = row
            dist = self._distance(x_pc, y_pc, z_pc, ref_x, ref_y, ref_z)
            if dist < distance:
                habstars.append({
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
                })
        return habstars

    def _distance(self, x1, y1, z1, x2, y2, z2):
        import math
        return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2 + (z1 - z2) ** 2)

    def get_habstars_with_similar_color_to(self, color):
        upper_color = color * 1.05
        lower_color = color * 0.95
        color_query = 'SELECT * FROM habstar WHERE b_minus_v < ? AND b_minus_v > ?'
        return self._dbpool.runQuery(color_query, (upper_color, lower_color)).addCallback(self._build_habstars)

    def _build_habstars(self, data):
        habstars = []
        for row in data:
            hip_num, ra_hours, ra_minutes, ra_seconds, dec_degrees, dec_minutes, dec_seconds, johnson_mag, parx_mas, \
                sigma_parx_mas, bmv, sigma_bmv, ccdm, hd, bd, dist_pc, x_pc, y_pc, z_pc = row
            habstars.append({
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
        return habstars
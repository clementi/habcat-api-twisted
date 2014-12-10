class HabstarRepo(object):
    def __init__(self, dbpool):
        self._dbpool = dbpool

    def get_habstar(self, hip_num):
        query = 'SELECT * FROM habstar WHERE hipparchos_num = ?;'
        return self._dbpool.runQuery(query, (hip_num,)).addCallback(self._build_habstar)

    def _build_habstar(self, data):
        row = data[0]
        return {
            'hip': row[0]
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
            dist = self._distance(row[16], row[17], row[18], row[19], row[20], row[21])
            if dist < distance:
                habstars.append({
                    'hip': row[0],
                    'ref_dist_pc': dist
                })
        return habstars

    def _distance(self, x1, y1, z1, x2, y2, z2):
        import math
        return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2 + (z1 - z2) ** 2)
from datetime import datetime, timezone


def instant(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('时间缺少时区')
    return parsed.astimezone(timezone.utc)


def intersects(first, second):
    if first[0] > first[2] or second[0] > second[2]:
        raise ValueError('普通经度范围不能倒置')
    return first[0] <= second[2] and second[0] <= first[2] and first[1] <= second[3] and second[1] <= first[3]


def collect(payload):
    start, end = map(instant, payload['interval'])
    records = []
    for item in payload['items']:
        observed = instant(item['properties']['datetime'])
        if start <= observed < end and intersects(item['bbox'], payload['bbox']):
            records.append(dict(key=item['id'], collection=item['collection'], observed=observed.isoformat(),
                                assets=item['assets'], bbox=item['bbox']))
    return records

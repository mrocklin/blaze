from blaze.datadescriptor import CSV_DDesc, JSON_DDesc
from blaze.datadescriptor.util import filetext
import json

def test_csv_json():
    with filetext('1,1\n2,2\n') as csv_fn, filetext('') as json_fn:
        schema = '2 * int'
        csv = CSV_DDesc(csv_fn, schema=schema)
        json = JSON_DDesc(json_fn, mode='w', schema=schema)

        json.extend(csv)

        assert list(json) == [[1, 1], [2, 2]]


def test_json_csv_structured():
    data = [{'x': 1, 'y': 1}, {'x': 2, 'y': 2}]
    text = '\n'.join(map(json.dumps, data))

    with filetext(text) as json_fn, filetext('') as csv_fn:
        schema = '{x: int, y: int}'
        js = JSON_DDesc(json_fn, schema=schema)
        csv = CSV_DDesc(csv_fn, mode='rw+', schema=schema)

        csv.extend(js)

        assert list(csv) == [{'x': 1, 'y': 1}, {'x': 2, 'y': 2}]

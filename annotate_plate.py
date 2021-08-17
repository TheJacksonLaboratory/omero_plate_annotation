import argparse
import ezomero
import logging
import pandas as pd
from omero.rtypes import rstring
from omero.sys import Parameters


OMERO_SERVER = 'bhomero01lp'
OMERO_GROUP = 'invitro_arsenic'
NAMESPACE = 'jax.org/omeroutils/invitro_arsenic/plate_metadata/v0'
DTYPES = {'plate_name': str,
          'row': int,
          'column': int,
          'individual': str,
          'concentration': float,
          'compound': str}


def get_plate_id(conn, plate_name):
    q = conn.getQueryService()
    params = Parameters()
    params.map = {'pname': rstring(plate_name)}
    results = q.projection(
        "SELECT pl.id FROM Plate pl"
        " WHERE pl.name=:pname",
        params,
        conn.SERVICE_OPTS)
    if len(results) == 0:
        raise ValueError(f'No plate found with name {plate_name}')
    elif len(results) > 1:
        raise ValueError(f'Multiple plates found with name {plate_name}')
    else:
        return [r[0].val for r in results][0]


def main(csv, force):
    # connect to OMERO
    conn = ezomero.connect(group=OMERO_GROUP, host=OMERO_SERVER, port=4064, secure=True)

    # load data from csv
    df = pd.read_csv(csv, dtype=DTYPES)

    # Ensure we are not missing any columns in the csv
    for col in DTYPES.keys():
        if col not in df.columns:
            raise ValueError(f'csv missing column {col}')

    # Go over every row of csv and annotate well accordingly
    for _, record in df.iterrows():
        # Parse the row
        record = dict(record)
        plate_name = record.pop('plate_name')
        row_index = record.pop('row') - 1
        col_index = record.pop('column') - 1

        # Find the well to annotate
        plate_id = get_plate_id(conn, plate_name)
        well_id = ezomero.get_well_id(conn, plate_id=plate_id, row=row_index, column=col_index)
        if well_id is None:
            raise ValueError(f'No well found at plate:{plate_id}, row_index:{row_index}, col_index:{col_index}')

        # check if MapAnnotation already exists for this well and act accordingly
        ma_ids = ezomero.get_map_annotation_ids(conn, "Well", well_id, ns=NAMESPACE)
        if len(ma_ids) != 0:
            logging.warning(f'MapAnnotation with namespace:{NAMESPACE} already exists for well:{well_id}')
            # check whether user indicated to force overwrite existing MapAnnotations
            if not force:
                logging.warning(f'Skipping MapAnnotation for well:{well_id}')
            else:
                logging.warning(f'Forcing update of MapAnnotation:{ma_ids[0]} for well:{well_id}')
                ezomero.put_map_annotation(conn, ma_ids[0], kv_dict=record)
        else:
            new_ma_id = ezomero.post_map_annotation(conn, "Well", well_id, kv_dict=record, ns=NAMESPACE)
            print(f"New MapAnnotation:{new_ma_id} posted to Well:{well_id}")
    conn.close()


if __name__ == '__main__':
    description = 'Annotate an OMERO plate from a csv file.'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('csv',
                        type=str,
                        help='Path to csv file containing plate metadata')
    parser.add_argument('--force',
                        action='store_true',
                        help='Overwrite existing MapAnnotations with same namespace')
    args = parser.parse_args()
    main(args.csv, args.force)


    
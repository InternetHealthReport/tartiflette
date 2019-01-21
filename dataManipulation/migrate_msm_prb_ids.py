import psycopg2
import psycopg2.extras
from progress.bar import Bar


def migrate_ids(alarm_id, cursor, table_name):
    ids = {}
    cursor.execute("SELECT msmid, probeid FROM {}_msms WHERE alarm_id=%s"
            .format(table_name), (alarm_id,))
    for row in cursor:
        msmid = row[0]
        prbid = row[1]

        if msmid not in ids:
            ids[msmid]=[]

        ids[msmid].append(prbid)

    cursor.execute("UPDATE {} SET msm_prb_ids = %s WHERE id = %s"
            .format(table_name), (psycopg2.extras.Json(ids), alarm_id))


if __name__ == "__main__":

    table_name = "ihr_delay_alarms"
    conn_string = "dbname='ihr'"

    conn = psycopg2.connect(conn_string)
    cur2 = conn.cursor()
    cur = conn.cursor()

    cur.execute("SELECT id FROM {} WHERE msm_prb_ids is null".format(table_name))

    bar = Bar('Processing', max=cur.rowcount)
    print("Found {} alarms".format(cur.rowcount))

    for row in cur:
        migrate_ids(row[0], cur2, table_name)
        bar.next()

    conn.commit()
    conn.close()
    bar.finish()


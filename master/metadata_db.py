import sqlite3

DB = "master/master.db"


def init_db():

    conn = sqlite3.connect(DB)

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    conn.commit()
    conn.close()

def save_primary(primary):

    conn = sqlite3.connect(DB)

    cur = conn.cursor()

    cur.execute(
        """
        INSERT OR REPLACE INTO metadata
        VALUES (?,?)
        """,
        ("primary", primary)
    )

    conn.commit()
    conn.close()
def load_primary():

    conn = sqlite3.connect(DB)

    cur = conn.cursor()

    cur.execute(
        """
        SELECT value
        FROM metadata
        WHERE key='primary'
        """
    )

    row = cur.fetchone()

    conn.close()

    if row:
        return row[0]

    return "1"

import sqlite3

DB = "master/file_metadata.db"


def init_file_db():

    conn = sqlite3.connect(DB)

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            filename TEXT,
            chunk_id TEXT
        )
    """)

    conn.commit()
    conn.close()


def save_file(filename, chunks):

    conn = sqlite3.connect(DB)

    cur = conn.cursor()

    cur.execute(
        "DELETE FROM files WHERE filename=?",
        (filename,)
    )

    for chunk in chunks:

        cur.execute(
            """
            INSERT INTO files
            VALUES (?,?)
            """,
            (
                filename,
                chunk
            )
        )

    conn.commit()
    conn.close()


def delete_file(filename):

    conn = sqlite3.connect(DB)

    cur = conn.cursor()

    cur.execute(
        """
        DELETE FROM files
        WHERE filename=?
        """,
        (filename,)
    )

    conn.commit()
    conn.close()


def load_files():

    conn = sqlite3.connect(DB)

    cur = conn.cursor()

    cur.execute(
        """
        SELECT filename, chunk_id
        FROM files
        """
    )

    rows = cur.fetchall()

    conn.close()

    file_table = {}

    for filename, chunk in rows:

        if filename not in file_table:

            file_table[filename] = []

        file_table[filename].append(
            chunk
        )

    return file_table
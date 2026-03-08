import bz2
import json
import sqlite3
import logging
from tqdm import tqdm


DUMP_FILE = "latest-all.json.bz2"
DB_FILE = "wikidata.db"
LOG_FILE = "wikidata_build.log"


# -------------------------------------------------
# Logging setup
# -------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()


# -------------------------------------------------
# SQLite setup
# -------------------------------------------------

logger.info("Opening SQLite database")

conn = sqlite3.connect(DB_FILE)

conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA cache_size=-512000")
conn.execute("PRAGMA busy_timeout=60000")


logger.info("Creating tables")

conn.execute("""
CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    label TEXT,
    description TEXT,
    aliases TEXT,
    instance_of TEXT,
    parent_company TEXT,
    subsidiaries TEXT,
    industry TEXT,
    country TEXT,
    sitelink_count INTEGER DEFAULT 0
)
""")


conn.execute("""
CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts
USING fts5(
    id UNINDEXED,
    label,
    content=entities,
    content_rowid=rowid,
    tokenize="unicode61"
)
""")


# -------------------------------------------------
# Helper
# -------------------------------------------------

def get_claim_ids(entity, prop):

    claims = entity.get("claims", {}).get(prop, [])

    ids = []

    for c in claims:
        try:
            val = c["mainsnak"]["datavalue"]["value"]

            if isinstance(val, dict):
                ids.append(val.get("id", ""))
            else:
                ids.append(str(val))

        except (KeyError, TypeError):
            pass

    return ids


# -------------------------------------------------
# Processing dump
# -------------------------------------------------

logger.info("Opening dump file: %s", DUMP_FILE)

batch = []
batch_size = 50000

lines = 0
inserted = 0
errors = 0


with bz2.open(DUMP_FILE, "rt", encoding="utf-8") as f:

    logger.info("Starting Wikidata processing")

    for line in tqdm(f, desc="Building index"):

        lines += 1

        line = line.strip().rstrip(",")

        if not line or line in ("[", "]"):
            continue

        try:
            entity = json.loads(line)
        except json.JSONDecodeError:
            errors += 1
            continue


        eid = entity.get("id", "")

        label = entity.get("labels", {}).get("en", {}).get("value", "")
        desc = entity.get("descriptions", {}).get("en", {}).get("value", "")

        aliases = [a["value"] for a in entity.get("aliases", {}).get("en", [])]

        sitelinks = len(entity.get("sitelinks", {}))

        instance_of = get_claim_ids(entity, "P31")
        parent = get_claim_ids(entity, "P749")
        subsidiaries = get_claim_ids(entity, "P355")
        industry = get_claim_ids(entity, "P452")
        country = get_claim_ids(entity, "P17")


        if label:

            batch.append((
                eid,
                label,
                desc,
                json.dumps(aliases),
                json.dumps(instance_of),
                json.dumps(parent),
                json.dumps(subsidiaries),
                json.dumps(industry),
                json.dumps(country),
                sitelinks
            ))


        # ---------------------------------------------
        # Batch insert
        # ---------------------------------------------

        if len(batch) >= batch_size:

            try:

                conn.executemany(
                    "INSERT OR REPLACE INTO entities VALUES (?,?,?,?,?,?,?,?,?,?)",
                    batch
                )

                conn.commit()

                inserted += len(batch)

                logger.info(
                    "Inserted batch=%s total=%s lines=%s",
                    len(batch),
                    inserted,
                    lines
                )

            except Exception as e:

                logger.error("Batch insert failed: %s", e)

            batch = []


# -------------------------------------------------
# Final batch
# -------------------------------------------------

if batch:

    logger.info("Inserting final batch")

    conn.executemany(
        "INSERT OR REPLACE INTO entities VALUES (?,?,?,?,?,?,?,?,?,?)",
        batch
    )

    conn.commit()

    inserted += len(batch)


# -------------------------------------------------
# Build FTS
# -------------------------------------------------

logger.info("Building FTS index")

conn.execute(
    'INSERT INTO entities_fts(entities_fts) VALUES("rebuild")'
)

conn.commit()


# -------------------------------------------------
# Finalize
# -------------------------------------------------

logger.info("Checkpointing WAL")

conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

conn.close()


# -------------------------------------------------
# Summary
# -------------------------------------------------

logger.info("Finished processing")

logger.info("Lines processed: %s", lines)
logger.info("Entities inserted: %s", inserted)
logger.info("JSON errors skipped: %s", errors)
logger.info("Database ready: %s", DB_FILE)
#!/usr/bin/env python3
import os
import sys
import argparse
import psycopg2
import time
import logging
from typing import List, Optional
from psycopg2 import sql, errors
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('sql_pipeline')


class SQLPipeline:
    def __init__(self):
        self.args = self.parse_arguments()
        self.conn = None

        # ── All SQL files live FLAT inside --sql-dir ──────────────────────────
        self.standard_sql_files = [
            '07-cs.categories.sql',
            '08-cs.payment_methods.sql'
        ]

        # Bulk-load files — kept here so you can add them later if needed.
        # Set to [] if you don't have them yet.
        self.bulk_load_sql_files = [
            '07-INSERT-ADDRESSES.sql',
            '08-INSERT-PATIENTS.sql',
            '09-INSERT-PATIENTS-PHONE.sql',
            '10-INSERT-PATIENTS-ADDRESSES.sql',
            '11-INSERT-PATIENTS-ALLERGIES.sql',
            '12-INSERT-DOCTORS.sql',
            '13-INSERT-DOCTOR_SPECIALTIES.sql',
            '14-INSERT-DOCTOR_ADDRESSES.sql',
            '15-INSERT-ROOMS.sql',
            '16-INSERT-APPOINTMENTS.sql',
            '17-INSERT-MEDICAL_RECORDS.sql',
            '18-INSERT-RECORD-DIAGNOSES.sql',
            '19-INSERT-PRESCRIPTIONS.sql',
            '21-INSERT-ORDERS.sql',
            '22-INSERT-PAYMENTS.sql',
        ]

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='PostgreSQL Database Pipeline')
        parser.add_argument('--host',        default='localhost')
        parser.add_argument('--port',        default=5433, type=int)
        parser.add_argument('--user',        required=True)
        parser.add_argument('--password',    required=True)
        parser.add_argument('--db-name',     default='smarthdb')
        parser.add_argument('--schema-name', default='smart_health')
        parser.add_argument('--sql-dir',     default='.')
        parser.add_argument('--max-retries', type=int,   default=3)
        parser.add_argument('--delay',       type=float, default=1.0)
        return parser.parse_args()

    # ── Connection ────────────────────────────────────────────────────────────

    def connect_postgres(self) -> Optional[psycopg2.extensions.connection]:
        for attempt in range(self.args.max_retries):
            try:
                conn = psycopg2.connect(
                    host=self.args.host,
                    port=self.args.port,
                    user=self.args.user,
                    password=self.args.password,
                    dbname=self.args.db_name,
                    connect_timeout=10,
                )
                logger.info(f"Connected to PostgreSQL (attempt {attempt + 1})")
                return conn
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt == self.args.max_retries - 1:
                    logger.error("Max connection retries reached")
                    return None
                time.sleep(2 ** attempt)

    # ── File helpers ──────────────────────────────────────────────────────────

    def validate_sql_file(self, file_path: str) -> bool:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False
        if not os.access(file_path, os.R_OK):
            logger.error(f"File not readable: {file_path}")
            return False
        if os.path.getsize(file_path) == 0:
            logger.warning(f"Empty file: {file_path}")
            return False
        return True

    def execute_sql_file(self, file_path: str) -> bool:
        if not self.validate_sql_file(file_path):
            return False

        # Read file (try UTF-8 first, fall back to latin-1)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_commands = f.read()
        except UnicodeDecodeError:
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    sql_commands = f.read()
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                return False

        # Split into individual statements
        statements = []
        buffer = ""
        for line in sql_commands.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith('--'):
                continue
            buffer += " " + stripped
            if stripped.endswith(";"):
                statements.append(buffer.strip())
                buffer = ""

        if not statements:
            logger.warning(f"No valid SQL statements found in {file_path}")
            return True

        try:
            with self.conn.cursor() as cur:
                with tqdm(statements,
                          desc=f"  {os.path.basename(file_path)}",
                          leave=False) as pbar:
                    for stmt in pbar:
                        try:
                            cur.execute(stmt)
                            pbar.set_postfix(status="OK")
                        except errors.Error as e:
                            pbar.set_postfix(status="ERROR")
                            logger.error(f"Statement error: {e.pgerror}")
                            self.conn.rollback()
                            return False
            self.conn.commit()
            logger.info(f"✔  {len(statements)} statements — {os.path.basename(file_path)}")
            return True
        except Exception as e:
            logger.error(f"Unexpected error executing {file_path}: {e}")
            self.conn.rollback()
            return False

    # ── Directory resolution ──────────────────────────────────────────────────

    def find_sql_directory(self) -> Optional[str]:
        """
        Returns the resolved --sql-dir path if at least one standard SQL file
        exists directly inside it (flat layout — no Catalogo/ subdirectory).
        """
        candidate = os.path.abspath(self.args.sql_dir)
        if not os.path.isdir(candidate):
            logger.error(f"--sql-dir does not exist: {candidate}")
            return None

        # Check that at least the first standard file is present
        first_file = os.path.join(candidate, self.standard_sql_files[0])
        if not os.path.exists(first_file):
            logger.error(
                f"Expected '{self.standard_sql_files[0]}' inside {candidate} but it was not found.\n"
                f"Make sure --sql-dir points to the folder that contains the .sql files directly."
            )
            return None

        return candidate

    # ── Main flow ─────────────────────────────────────────────────────────────

    def schema_exists(self, schema_name: str) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema_name,))
                exists = cur.fetchone() is not None
            logger.info(f"Schema '{schema_name}' exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Error checking schema: {e}")
            return False

    def run(self):
        logger.info("═══ Starting SQL Pipeline ═══")

        # 1. Locate SQL directory
        sql_dir = self.find_sql_directory()
        if not sql_dir:
            sys.exit(1)
        logger.info(f"SQL directory: {sql_dir}")

        # 2. Connect
        self.conn = self.connect_postgres()
        if not self.conn:
            sys.exit(1)

        try:
            # 3. Verify schema
            if not self.schema_exists(self.args.schema_name):
                logger.error(f"Schema '{self.args.schema_name}' does not exist in the database.")
                sys.exit(1)

            # 4. Build file list — all files are flat inside sql_dir
            all_files = []

            for sql_file in self.standard_sql_files:
                path = os.path.join(sql_dir, sql_file)
                all_files.append((sql_file, path))

            # Only include bulk-load files that actually exist
            for sql_file in self.bulk_load_sql_files:
                path = os.path.join(sql_dir, sql_file)
                if os.path.exists(path):
                    all_files.append((sql_file, path))
                else:
                    logger.debug(f"Skipping missing bulk-load file: {sql_file}")

            logger.info(f"Files to process: {len(all_files)}")

            # 5. Execute
            success_count = 0
            with tqdm(all_files, desc="Overall progress") as pbar:
                for idx, (name, path) in enumerate(pbar):
                    pbar.set_postfix(file=name[:20])
                    if self.execute_sql_file(path):
                        success_count += 1
                    else:
                        logger.error(f"Pipeline stopped at: {name}")
                        break

                    # Delay between files (skip after last)
                    if idx < len(all_files) - 1:
                        time.sleep(self.args.delay)

            logger.info(
                f"═══ Pipeline done: {success_count}/{len(all_files)} files successful ═══"
            )
            return success_count == len(all_files)

        finally:
            if self.conn:
                self.conn.close()
                logger.info("Database connection closed")


if __name__ == "__main__":
    pipeline = SQLPipeline()
    if not pipeline.run():
        sys.exit(1)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import csv
import sys
import time
import os
import re
import logging
import uuid
from datetime import datetime, timedelta

import hydra
from omegaconf import DictConfig, OmegaConf
import psycopg2
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ------------------------- Session initialization -------------------------
def create_session(cfg):
    session = requests.Session()
    session.headers.update({"User-Agent": cfg.api.user_agent})
    return session

# ------------------------- PostgreSQL (with retry) -------------------------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(psycopg2.Error),
    reraise=True
)
def get_db_connection(cfg):
    """Creates a database connection using config parameters."""
    try:
        conn = psycopg2.connect(
            dbname=cfg.db.name,
            user=cfg.db.user,
            password=cfg.db.password,
            host=cfg.db.host,
            port=cfg.db.port
        )
        conn.autocommit = False
        return conn
    except Exception as e:
        logging.getLogger(__name__).exception("Database connection error")
        raise

def init_db(cfg):
    """Creates the vacancies table if it doesn't exist, adds etl_id column."""
    with get_db_connection(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    id VARCHAR(20) PRIMARY KEY,
                    name TEXT,
                    employer TEXT,
                    city TEXT,
                    salary_from INTEGER,
                    salary_to INTEGER,
                    currency VARCHAR(10),
                    requirement TEXT,
                    experience TEXT,
                    schedule TEXT,
                    published DATE,
                    url TEXT,
                    updated_at TIMESTAMP DEFAULT NOW(),
                    etl_id VARCHAR(32)
                );
            """)
            cur.execute("""
                ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS etl_id VARCHAR(32);
            """)
            conn.commit()
    logging.getLogger(__name__).info("Table 'vacancies' checked/created.")

def init_logs_table(cfg):
    """Creates the parsing_logs table if it doesn't exist, adds etl_id column."""
    with get_db_connection(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS parsing_logs (
                    id SERIAL PRIMARY KEY,
                    employer_id VARCHAR(20),
                    employer_name TEXT,
                    date_from DATE,
                    vacancies_found INTEGER,
                    status VARCHAR(20),
                    error_message TEXT,
                    started_at TIMESTAMP DEFAULT NOW(),
                    finished_at TIMESTAMP,
                    etl_id VARCHAR(32)
                );
            """)
            cur.execute("""
                ALTER TABLE parsing_logs ADD COLUMN IF NOT EXISTS etl_id VARCHAR(32);
            """)
            conn.commit()
    logging.getLogger(__name__).info("Table 'parsing_logs' checked/created.")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(psycopg2.Error),
    reraise=True
)
def save_vacancies_to_db(cfg, vacancies, etl_id):
    """Saves a list of vacancies to the database (upsert) with etl_id."""
    logger = logging.getLogger(__name__)
    if not vacancies:
        logger.info("No data to insert.")
        return 0

    query = """
        INSERT INTO vacancies (
            id, name, employer, city, salary_from, salary_to, currency,
            requirement, experience, schedule, published, url, etl_id
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            employer = EXCLUDED.employer,
            city = EXCLUDED.city,
            salary_from = EXCLUDED.salary_from,
            salary_to = EXCLUDED.salary_to,
            currency = EXCLUDED.currency,
            requirement = EXCLUDED.requirement,
            experience = EXCLUDED.experience,
            schedule = EXCLUDED.schedule,
            published = EXCLUDED.published,
            url = EXCLUDED.url,
            etl_id = EXCLUDED.etl_id,
            updated_at = NOW();
    """

    values = [(
        v['id'],
        v['name'],
        v['employer'],
        v['city'],
        v['salary_from'],
        v['salary_to'],
        v['currency'],
        v['requirement'],
        v['experience'],
        v['schedule'],
        v['published'] if v['published'] else None,
        v['url'],
        etl_id
    ) for v in vacancies]

    try:
        with get_db_connection(cfg) as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, values)
                conn.commit()
        logger.info(f"Added/updated {len(vacancies)} vacancies in PostgreSQL.")
    except Exception as e:
        logger.exception("Error saving vacancies to database")
        raise
    return len(vacancies)

def log_parsing_job(cfg, employer_id, employer_name, date_from, vacancies_found, status, error_message, etl_id):
    """Writes a parsing log entry to the parsing_logs table with etl_id."""
    logger = logging.getLogger(__name__)
    try:
        with get_db_connection(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO parsing_logs 
                        (employer_id, employer_name, date_from, vacancies_found, status, error_message, finished_at, etl_id)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
                """, (employer_id, employer_name, date_from, vacancies_found, status, error_message, etl_id))
                conn.commit()
        logger.info("Log entry added to parsing_logs table.")
    except Exception as e:
        logger.exception("Failed to write log to database")

# ------------------------- CSV logging -------------------------
def log_to_csv(cfg, employer_id, employer_name, date_from, vacancies_found, status, error_message, started_at, finished_at, etl_id):
    """Writes job information to a CSV file, including etl_id."""
    logger = logging.getLogger(__name__)
    filename = cfg.csv_log.log_file
    try:
        file_exists = os.path.isfile(filename)
        with open(filename, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            if not file_exists:
                writer.writerow([
                    "employer_id", "employer_name", "date_from", "vacancies_found",
                    "status", "error_message", "started_at", "finished_at", "etl_id"
                ])
            writer.writerow([
                employer_id, employer_name, date_from, vacancies_found,
                status, error_message or "", started_at.strftime("%Y-%m-%d %H:%M:%S"),
                finished_at.strftime("%Y-%m-%d %H:%M:%S"), etl_id
            ])
        logger.info(f"Log entry added to CSV file: {filename}")
    except Exception as e:
        logger.exception("Failed to write log to CSV")

# ------------------------- HH.ru API (with retry) -------------------------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def find_company_id_by_name(session, cfg, company_name):
    """Finds company ID by name."""
    logger = logging.getLogger(__name__)
    params = {"text": company_name, "per_page": 1}
    logger.debug("Request to /employers with params: %s", params)
    try:
        resp = session.get(cfg.api.hh_url + "employers", params=params)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.exception("Error searching for company")
        raise
    items = resp.json().get("items", [])
    if not items:
        logger.error(f"Company '{company_name}' not found.")
        sys.exit(1)
    emp = items[0]
    logger.info(f"Found company: {emp['name']} (ID: {emp['id']})")
    return emp["id"], emp["name"]

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def fetch_vacancies_page(session, cfg, employer_id, page=0, date_from=None):
    """Loads one page of vacancies."""
    logger = logging.getLogger(__name__)
    params = {
        "employer_id": str(employer_id).strip(),
        "page": page,
        "per_page": cfg.api.per_page
    }
    if date_from:
        if re.match(r"\d{4}-\d{2}-\d{2}", date_from):
            params["date_from"] = date_from
        else:
            raise ValueError("Invalid date_from format. Use YYYY-MM-DD")
    logger.debug("Request to /vacancies, params: %s", params)
    try:
        resp = session.get(cfg.api.hh_url + "vacancies", params=params)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.exception("Error fetching vacancies")
        raise
    return resp.json()

def parse_vacancy(item):
    """Converts raw vacancy JSON into a structured dictionary."""
    salary = item.get("salary") or {}
    snippet = item.get("snippet") or {}
    area = item.get("area") or {}
    experience = item.get("experience") or {}
    schedule = item.get("schedule") or {}

    requirement = snippet.get("requirement")
    if requirement:
        requirement = requirement.replace("\n", " ").replace(";", ",")
    else:
        requirement = ""

    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "employer": item.get("employer", {}).get("name"),
        "city": area.get("name"),
        "salary_from": salary.get("from"),
        "salary_to": salary.get("to"),
        "currency": salary.get("currency"),
        "requirement": requirement,
        "experience": experience.get("name"),
        "schedule": schedule.get("name"),
        "published": (item.get("published_at") or "")[:10],
        "url": item.get("alternate_url")
    }

# ------------------------- CSV for vacancies (fallback) -------------------------
def save_vacancies_to_csv(cfg, vacancies, filename):
    """Saves vacancies to a CSV file."""
    logger = logging.getLogger(__name__)
    if not vacancies:
        logger.info("No data to save.")
        return
    try:
        with open(filename, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=vacancies[0].keys(), delimiter=";")
            writer.writeheader()
            writer.writerows(vacancies)
        logger.info(f"Saved {len(vacancies)} vacancies to file: {filename}")
    except Exception as e:
        logger.exception("Error saving CSV with vacancies")
        raise

# ------------------------- Main (Hydra) -------------------------
@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    # Configure logging from config
    log_handlers = [logging.StreamHandler()]

    
    logger = logging.getLogger(__name__)

    start_time = datetime.now()
    etl_id = uuid.uuid4().hex
    employer_id = None
    employer_name = None
    date_filter = None
    vacancies_count = 0
    status = "error"
    error_message = None

    try:
        # Create session
        session = create_session(cfg)

        # --- Determine company ---
        if cfg.parser.company_id:
            employer_id = cfg.parser.company_id
            try:
                resp = session.get(cfg.api.hh_url + f"employers/{employer_id}")
                resp.raise_for_status()
                employer_name = resp.json().get("name", employer_id)
                logger.info(f"Company ID: {employer_id}, name: {employer_name}")
            except Exception as e:
                logger.exception("Failed to get company name by ID")
                employer_name = employer_id
        elif cfg.parser.company_name:
            employer_id, employer_name = find_company_id_by_name(session, cfg, cfg.parser.company_name)
        else:
            logger.error("Neither company_id nor company_name provided in config or command line")
            sys.exit(1)

        logger.info(f"Loading vacancies for: {employer_name} (ID: {employer_id})")

        # --- Date filter ---
        if cfg.parser.days_back is not None:
            date_filter = (datetime.now() - timedelta(days=cfg.parser.days_back)).strftime("%Y-%m-%d")
            logger.info(f"days_back={cfg.parser.days_back}, loading from {date_filter}")
        elif cfg.parser.date_from:
            date_filter = cfg.parser.date_from
            datetime.strptime(date_filter, "%Y-%m-%d")  # validate format
            logger.info(f"date_from={date_filter}")
        else:
            date_filter = None
            logger.info("No date filter applied, loading all vacancies (no date_from)")

        # --- Load pages ---
        all_vacancies = []
        page = 0
        while True:
            logger.debug(f"Loading page {page+1}...")
            data = fetch_vacancies_page(session, cfg, employer_id, page, date_filter)
            items = data.get("items", [])
            logger.info(f"Page {page+1}: received {len(items)} vacancies")
            for item in items:
                all_vacancies.append(parse_vacancy(item))
            if page >= data.get("pages", 0) - 1:
                break
            page += 1
            time.sleep(0.3)

        vacancies_count = len(all_vacancies)
        logger.info(f"Total vacancies fetched: {vacancies_count}")

        # --- Save ---
        if cfg.parser.db_enabled:
            init_db(cfg)
            init_logs_table(cfg)
            saved = save_vacancies_to_db(cfg, all_vacancies, etl_id)
        else:
            if cfg.parser.no_csv:
                logger.info("First 5 vacancies:")
                for v in all_vacancies[:5]:
                    logger.info(f"• {v['name']} — {v['city']} — {v['url']}")
            else:
                save_vacancies_to_csv(cfg, all_vacancies, cfg.parser.output_file)

        status = "success"

    except Exception as e:
        error_message = str(e)[:500]
        logger.exception("Critical error during script execution")
        status = "error"

    finally:
        finish_time = datetime.now()

        # --- Log to database (if DB enabled) ---
        if cfg.parser.db_enabled and employer_id and employer_name:
            log_parsing_job(cfg, employer_id, employer_name, date_filter, vacancies_count, status, error_message, etl_id)

        # --- Log to CSV (if not disabled) ---
        if not cfg.csv_log.no_log and employer_id and employer_name:
            log_to_csv(cfg, employer_id, employer_name, date_filter, vacancies_count, status, error_message, start_time, finish_time, etl_id)

if __name__ == "__main__":
    main()
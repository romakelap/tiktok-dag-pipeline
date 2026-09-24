"""
Database Hook untuk Ingestion DAG.

Wrapper SQLAlchemy untuk koneksi MySQL.
Connection string dari Airflow Variable: DB_CONNECTION_STRING
Format: mysql+pymysql://user:password@host:port/database
"""
import logging
import pandas as pd
from typing import List, Dict, Optional, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from contextlib import contextmanager


class EchotikDBHook:
    """
    Hook untuk MySQL database (tiktok_oltp).
    """
    
    def __init__(self, connection_string: str, echo: bool = False):
        """
        Args:
            connection_string: SQLAlchemy connection string
            echo: Print SQL queries (untuk debug)
        """
        self.connection_string = connection_string
        
        # SSL configuration for PyMySQL (required for Aiven)
        connect_args = {}
        import re
        if "ssl-mode=" in connection_string or "ssl_mode=" in connection_string or "aivencloud.com" in connection_string:
            connect_args["ssl"] = {"check_hostname": False}
            # Clean up ssl-mode from connection string to avoid driver errors
            connection_string = re.sub(r'[?&]ssl[-_]mode=[^&]+', '', connection_string)
            if connection_string.endswith('?'):
                connection_string = connection_string[:-1]
                
        self.engine: Engine = create_engine(
            connection_string,
            echo=echo,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args=connect_args if connect_args else None
        )
        logging.info("DB engine created with SSL settings")
    
    def test_connection(self) -> bool:
        """Test koneksi DB"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
                return True
        except Exception as e:
            logging.error(f"DB connection test failed: {e}")
            raise e
    
    def check_tables_exist(self, table_names: List[str]) -> Dict[str, bool]:
        """Cek apakah tabel-tabel ini ada di DB"""
        result = {}
        with self.engine.connect() as conn:
            for table in table_names:
                check = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE() 
                      AND table_name = :tbl
                """), {"tbl": table})
                exists = check.scalar() > 0
                result[table] = exists
        return result
    
    @contextmanager
    def transaction(self):
        """Context manager untuk transaction (atomic operation)"""
        conn = self.engine.connect()
        trans = conn.begin()
        try:
            yield conn
            trans.commit()
            logging.info("Transaction committed")
        except Exception as e:
            trans.rollback()
            logging.error(f"Transaction rolled back: {e}")
            raise
        finally:
            conn.close()
    
    def execute(self, sql: str, params: Optional[Dict] = None) -> Any:
        """Execute single SQL (DDL/DML)"""
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                result = conn.execute(text(sql), params or {})
                trans.commit()
                return result
            except Exception as e:
                trans.rollback()
                raise
    
    def execute_many(self, sql: str, params_list: List[Dict]) -> int:
        """
        Execute SQL multiple times dengan param list (batch).
        Return: row count yang ter-affect.
        """
        if not params_list:
            return 0
        
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                result = conn.execute(text(sql), params_list)
                trans.commit()
                return result.rowcount
            except Exception as e:
                trans.rollback()
                raise
    
    def truncate_table(self, table_name: str) -> None:
        """TRUNCATE table (clear semua data)"""
        self.execute(f"TRUNCATE TABLE {table_name}")
        logging.info(f"Truncated: {table_name}")
    
    def bulk_insert(self, table_name: str, records: List[Dict], 
                    batch_size: int = 100) -> int:
        """
        Bulk INSERT records ke table.
        Pakai pandas to_sql untuk efficiency.
        
        Returns: jumlah rows inserted.
        """
        if not records:
            return 0
        
        df = pd.DataFrame(records)
        
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False,
                chunksize=batch_size,
                method='multi'
            )
            logging.info(f"Bulk inserted {len(records)} rows to {table_name}")
            return len(records)
        except Exception as e:
            logging.error(f"Bulk insert failed: {e}")
            raise
    
    def query_to_df(self, sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Run SELECT query, return DataFrame"""
        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})
    
    def query_scalar(self, sql: str, params: Optional[Dict] = None) -> Any:
        """Run SELECT query, return single scalar value"""
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            row = result.fetchone()
            return row[0] if row else None
    
    def count_rows(self, table_name: str, where: str = "") -> int:
        """Hitung rows di table"""
        sql = f"SELECT COUNT(*) FROM {table_name}"
        if where:
            sql += f" WHERE {where}"
        return self.query_scalar(sql)
    
    def close(self):
        """Close engine"""
        self.engine.dispose()
        logging.info("DB engine disposed")


def get_db_hook() -> EchotikDBHook:
    """
    Helper untuk get DB hook dengan connection string dari Airflow Variable.
    """
    from airflow.models import Variable
    
    try:
        conn_str = Variable.get('DB_CONNECTION_STRING')
    except Exception as e:
        raise ValueError(
            f"DB_CONNECTION_STRING tidak set di Airflow Variables. "
            f"Format: mysql+pymysql://user:pass@host:3306/tiktok_oltp"
        )
    
    return EchotikDBHook(connection_string=conn_str)

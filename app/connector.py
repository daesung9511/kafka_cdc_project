import abc
from contextlib import contextmanager
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class BaseConnector(abc.ABC):
    def __init__(self, hostname: str, port: int, target_url: Optional[str]) -> None:
        self.target_url = target_url
        self.hostname = hostname
        self.port = port
        self.sql_engine = None


    def get_sql_engine(self):
        if self.sql_engine is None:
            self.sql_engine = create_engine(self.target_url)
        return self.sql_engine
    def get_session_local(self):
        return sessionmaker(autocommit=False, autoflush=False, bind=self.get_sql_engine())()

    @contextmanager
    def get_session(self):
        session = self.get_session_local()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


class PostgresConnector(BaseConnector):
    def __init__(self, hostname: str = "localhost", port: int = 5342, target_url: Optional[str] = None, *args, **kwargs) -> None:
        super().__init__(hostname, port, target_url)
        if not target_url:
            self.target_url = f"postgresql+psycopg2://{self.hostname}:{self.port}"
        print(f"Connecting to PostgreSQL on {target_url}")

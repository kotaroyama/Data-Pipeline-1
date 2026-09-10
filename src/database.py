from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
from sqlmodel import create_engine, Field, select, Session, SQLModel, text


class EventLog(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    event_time: datetime = Field(
        index=True
    )
    event_type: str
    product_id: int
    price: float
    user_id: int
    user_session: str

class SessionMetric(EventLog):
    __tablename__ = "session_metrics"
    pass

class ProductMetric(SQLModel, table=True):
    __tablename__ = "product_metrics"

    product_id: int = Field(primary_key=True)
    avg_conversion_time: float
    total_purchases: int

class DatabaseManager:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)

    def init_db(self):
        """Creates tables if they don't exist."""
        SQLModel.metadata.create_all(self.engine)

    def bulk_insert_events(self, chunk: pd.DataFrame):
        """Bulk Inserts Events"""
        if chunk.empty:
            return

        records: List[Dict[str, Any]] = chunk.to_dict(orient="records")
        event_objects = [EventLog(**record) for record in records]

        with Session(self.engine) as session:
            session.add_all(event_objects)
            session.commit()

    def save_metrics(self, df: pd.DataFrame, table_name: str):
        """Inserts Session/Product Metrics"""
        if df.empty:
            return

        df.to_sql(
            name=table_name,
            con=self.engine,
            if_exists="append",
            index=False,
            method="multi",
        )

    def run_audit_queries(self):
        """Executes SQL audit query to return overall conversion rate"""
        query = text("""
            SELECT 
                COUNT(DISTINCT user_session) AS total_sessions,
                COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_session END) AS purchasing_sessions,
                ROUND(
                    (COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_session END) * 100.0) /
                    NULLIF(COUNT(DISTINCT user_session), 0),
                    2
                ) AS overall_conversion_rate
            FROM eventlog;
        """)

        with Session(self.engine) as session:
            results = session.exec(query).first()

        return {
            "total_sessions": results[0],
            "purchasing_sessions": results[1],
            "overall_conversion_rate": results[2],
        }
    
    def get_top_performing_products(self, limit: int = 5) -> List[ProductMetric]:
        """Runs a SQL query to fetch diagnostic results."""
        with Session(self.engine) as session:
            statement = (
                select(ProductMetric)
                .order_by(ProductMetric.total_purchases.desc())
                .limit(limit)
            )
            return session.exec(statement).all()
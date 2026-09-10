from typing import Generator, List, Optional

import pandas as pd

class DataCleaner:
    def __init__(self, use_cols=None):
        self.use_cols = use_cols or ["event_time", "event_type", "product_id", "price", "user_id", "user_session"]
    
    def clean_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df = df.drop_duplicates()
        df = df.dropna(subset=["user_session"]) # Drop columns whose "user_session" is NaN
        df["event_time"] = pd.to_datetime(
            df["event_time"].str[:-3].str.strip(),
            format="%Y-%m-%d %H:%M:%S"
        )
        return df

    def load_and_clean_chunks(
        self,
        data_path: str,
        chunksize: int,
        limit: Optional[int] = None,
    ) -> Generator[pd.DataFrame, None, None]:
        rows_processed = 0

        # Read CSV in streaming chunks
        for chunk in pd.read_csv(
            data_path, usecols=self.use_cols, chunksize=chunksize
        ):
            cleaned_chunk = self.clean_chunk(chunk)
            if cleaned_chunk.empty:
                continue

            chunk_len = len(cleaned_chunk)

            if limit and (rows_processed + chunk_len) >= limit:
                overflow = limit - rows_processed
                if overflow > 0:
                    yield cleaned_chunk.iloc[:-overflow]
                break
            rows_processed += chunk_len
            yield cleaned_chunk
            
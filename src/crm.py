import pandas as pd

class CRMClient:
    def fetch(self) -> pd.DataFrame: raise NotImplementedError
    def writeback(self, df: pd.DataFrame) -> None: raise NotImplementedError

class CsvCRM(CRMClient):
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path; self.output_path = output_path
    def fetch(self) -> pd.DataFrame: return pd.read_csv(self.input_path).fillna("")
    def writeback(self, df: pd.DataFrame) -> None: df.to_csv(self.output_path, index=False)

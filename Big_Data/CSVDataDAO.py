import pandas as pd


class CSVDataDAO:

    def __init__(self, filename: str, zadanie: int = 1):
        self.filename = filename
        self.counter = 0
        self.zadanie = zadanie

    def save(self, data: pd.DataFrame, sep: str = ',', encoding='utf-8', filename: str = "") -> None:
        if filename == "":
            fullFilename = f"{self.filename}_{self.counter}.csv"
            self.counter += 1
        else:
            fullFilename = f"{filename}.csv"
        data.to_csv(f"zadanie{self.zadanie}/{fullFilename}", sep, encoding)

    def load(self, filename: str, zadanie: int = -1, dtypes: dict = None) -> pd.DataFrame:
        zadanie = self.zadanie if zadanie == -1 else zadanie
        if not dict:
            return pd.read_csv(f"zadanie{zadanie}/{filename}.csv", dtype=dtypes)
        return pd.read_csv(f"zadanie{zadanie}/{filename}.csv")


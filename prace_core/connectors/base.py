import pandas as pd

class Connector:
    def load(self, **kwargs) -> pd.DataFrame:
        """Return DataFrame with at least ['id', 'input', 'ground_truth'] columns
        ground_truth may be NaN for vibe-only tasks."""
        pass 
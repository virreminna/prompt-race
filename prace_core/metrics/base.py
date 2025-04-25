class Metric:
    name: str

    def score(self, prediction: str, reference: str | None) -> float:
        ... 
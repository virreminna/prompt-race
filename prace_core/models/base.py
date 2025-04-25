class Model:
    name: str

    def run(self, prompt: str, image: str | None = None) -> str:
        ... 
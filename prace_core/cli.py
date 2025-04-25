import click

@click.group()
def prace():
    ...

@prace.command()
def hello():
    """Sanity check."""
    click.echo("🏁 PromptRace ready!")

@prace.command()
@click.argument("config")
def run(config):
    """Run evaluation on YAML config."""
    click.echo(f"Running config {config}")
    # TODO: parse YAML, loop over prompts/models/metrics

if __name__ == "__main__":
    prace() 
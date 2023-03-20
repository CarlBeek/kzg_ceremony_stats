import click

import process_data
import stats

@click.group()
def cli():
    pass

cli.add_command(process_data.load_new_data)
cli.add_command(stats.calculate_stats)
cli()

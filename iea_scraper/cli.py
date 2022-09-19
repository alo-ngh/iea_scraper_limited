from pathlib import Path
import click
from iea_scraper.settings import ROOT_PATH
from subprocess import call



def get_opts(ctx, args, incomplete):
    """ auto complete for option "opt"

    :param ctx: The current click context.
    :param args: The list of arguments passed in.
    :param incomplete: The partial word that is being completed, as a
        string. May be an empty string '' if no characters have
        been entered yet.
    :return: list of possible choices
    """
    iea_scripts_dir = Path(ROOT_PATH) / 'iea_scraper' / 'scripts'
    files = [x.name[:-3] for x in iea_scripts_dir.glob('**/*') if x.is_file()]
    return [arg for arg in files if arg.startswith(incomplete)]


@click.command()
@click.argument('script')
def cli(script):
    iea_scripts_dir = Path(ROOT_PATH) / 'iea_scraper' / 'scripts'
    files = [x.name[:-3] for x in iea_scripts_dir.glob('**/*') if x.is_file()]
    if script is None:
        print('Please add argument --script')
    elif script not in files:
        print(f'Script argument must be in {files}')
    else:
        call(["python", ROOT_PATH / 'iea_scraper' / 'scripts' / (script +'.py') ])

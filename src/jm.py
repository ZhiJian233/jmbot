import jmcomic
import os
import pathlib

def jmcomic_create_option_by_file():
    jmbot_path = pathlib.Path(__file__).parent.parent.absolute()
    print(jmbot_path)
    os.environ['JMBOT_PATH'] = f'{jmbot_path}'
    return jmcomic.create_option_by_file(f'{jmbot_path}/options/jmcomic_option.yml')
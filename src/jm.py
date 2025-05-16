import jmcomic
from jmcomic import *

# 创建配置对象
option = jmcomic.create_option_by_file('./options/jmcomic_option.yml')
# 使用option对象来下载本子

option.download_album(422866)

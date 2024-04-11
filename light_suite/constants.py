from toolkit.fileutils import Fileutils
from toolkit.logger import Logger

logging = Logger(10)

DIRP = "../../"
DATA = "../data/"
FUTL = Fileutils()
CRED = FUTL.get_lst_fm_yml(DIRP + "suite.yml")
print(CRED)

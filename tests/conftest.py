"""pytest 公共装置：把插件目录塞进 sys.path。

插件模块用扁平名（blg_proto / blg_events）导入，与 plugin.py 内的
sys.path.insert 保持一致，避免与其它插件同名模块互相覆盖。
"""

import sys
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

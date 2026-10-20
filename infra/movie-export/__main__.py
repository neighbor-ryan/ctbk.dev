from pathlib import Path
import sys

import pulumi

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from movies import provision

provision(pulumi.Config().require('cloudflare_account_id'))

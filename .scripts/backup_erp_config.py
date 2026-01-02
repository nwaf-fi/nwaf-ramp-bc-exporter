from pathlib import Path
from datetime import datetime
import shutil

src = Path('ERP_Config')
if not src.exists():
    print('ERP_Config not found')
    raise SystemExit(1)
out = Path('exports')
out.mkdir(exist_ok=True)
ts = datetime.now().strftime('%Y%m%dT%H%M%S')
backup_dir = out / f'ERP_Config_backup_{ts}'
if backup_dir.exists():
    shutil.rmtree(backup_dir)
shutil.copytree(src, backup_dir)
print('Copied ERP_Config to', backup_dir)

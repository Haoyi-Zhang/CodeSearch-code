#!/usr/bin/env python3
"""Run the complete bounded scientific reproduction offline.

This driver never silently skips a missing scientific stage. Paper compilation
is intentionally separate so the artifact also works without a paper directory.
"""
from __future__ import annotations
import json,os,subprocess,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
ENV={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','PYTHONHASHSEED':'0'}
STAGES=[('continuation','run_continuation_suite.py'),('published-history','run_real_history_suite.py'),
        ('cold-query','run_suite.py'),('resource-ledger','refresh_resource_ledger.py'),
        ('release','validate_release.py')]

def main():
    start=time.monotonic();records=[];status='PASS';error=None
    for label,script in STAGES:
        then=time.monotonic();log=ROOT/'results'/('aggregate-'+label+'.log')
        try:
            result=subprocess.run([sys.executable,script],cwd=ROOT,env=ENV,text=True,
                   stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=240)
            log.write_text(result.stdout.replace(str(ROOT),'$ARTIFACT'),encoding='utf-8')
            records.append({'stage':label,'command':['python',script],'exit_code':result.returncode,
               'wall_seconds':time.monotonic()-then,'log':str(log.relative_to(ROOT))})
            if result.returncode:raise RuntimeError(f'{label} failed; see {log.name}')
        except Exception as exc:
            status='FAIL';error=str(exc);break
    report={'status':status,'commands':records,'wall_seconds':time.monotonic()-start,'error':error,
       'scope':'bounded offline experiment and evidence validation; no paper build or acceptance claim'}
    (ROOT/'results/all-clean-reproduction.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
    raise SystemExit(status!='PASS')
if __name__=='__main__':main()

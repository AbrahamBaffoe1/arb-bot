"""Generate user launchd jobs for public collection and isolated offline research."""
import plistlib
import sys
from pathlib import Path
from .configuration import ROOT


def service_manifests(config,study_directory,output):
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    jobs={
        'local.arb.collector':[sys.executable,str(ROOT/'paper_service.py'),'--config',str(Path(config).resolve())],
        'local.arb.study':[sys.executable,str(ROOT/'study.py'),'worker','--config',str(Path(config).resolve()),'--directory',str(Path(study_directory).resolve())],
    }
    paths=[]
    for label,args in jobs.items():
        payload=dict(Label=label,ProgramArguments=args,WorkingDirectory=str(ROOT),RunAtLoad=True,
            KeepAlive=True,ThrottleInterval=30,ExitTimeOut=30,ProcessType='Background',
            StandardOutPath=str(ROOT/'data'/f'{label}.log'),StandardErrorPath=str(ROOT/'data'/f'{label}.log'),
            EnvironmentVariables={'PYTHONUNBUFFERED':'1'})
        path=output/(label+'.plist')
        with path.open('wb') as f:plistlib.dump(payload,f)
        paths.append(path)
    return paths

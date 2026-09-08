import json
import plistlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from src.configuration import load_config,fingerprint
from src.operations import restore_drill,database_inventory
from src.recorder import Recorder
from src.service import service_manifests
from src.study import initialize_study,evaluate_study,phase,period_replay


class StudyOperationsTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.cfg=load_config();self.cfg['recording']['db_path']=str(self.root/'tape.db')

    def test_study_waits_for_wall_clock_and_recorded_data(self):
        p=initialize_study(self.cfg,self.root/'study',now=1000,training_hours=1,validation_hours=2)
        self.assertEqual(phase(p,20000,1100),'collecting_training')
        self.assertEqual(phase(p,1200,20000),'collecting_training')
        self.assertEqual(phase(p,5000,5000),'collecting_validation')
        self.assertEqual(phase(p,12000,12000),'ready_to_evaluate')

    def test_changed_code_blocks_frozen_study(self):
        directory=self.root/'study'
        initialize_study(self.cfg,directory)
        with patch('src.study.code_identity',return_value='changed'):
            result=evaluate_study(self.cfg,directory)
        self.assertEqual(result['phase'],'blocked')
        self.assertFalse(result['complete'])
        self.assertFalse(result['live_ready'])

    def test_protocol_cannot_be_overwritten(self):
        directory=self.root/'study'
        initialize_study(self.cfg,directory)
        with self.assertRaises(FileExistsError):initialize_study(self.cfg,directory)

    def test_period_never_reads_future_book_or_mutates_tape(self):
        recorder=Recorder(self.cfg['recording']['db_path'],self.cfg,fingerprint(self.cfg))
        recorder.emit('scan',body={'candidates':1,'fresh_inventory':True},received=1000)
        recorder.emit('scan',body={'candidates':1,'fresh_inventory':True},received=1001)
        recorder.emit('recording_gap',body={'reason':'outside window'},received=1010)
        recorder.close()
        before=database_inventory(recorder.path)
        p=initialize_study(self.cfg,self.root/'study',now=1000)
        result,_=period_replay(recorder.path,p,1000,1002,100000,p['parameters'])
        self.assertTrue(result['data_integrity_ok'])
        self.assertEqual(result['healthy_seconds'],1)
        self.assertEqual(result['healthy_fraction'],.5)
        self.assertEqual(database_inventory(recorder.path),before)

    def test_study_rejects_mixed_economics(self):
        other=load_config();other['venues']['coinbase']['maker_fee']=.007
        recorder=Recorder(self.cfg['recording']['db_path'],other,fingerprint(other))
        recorder.emit('scan',received=1000);recorder.close()
        p=initialize_study(self.cfg,self.root/'study',now=1000)
        with self.assertRaisesRegex(ValueError,'mixed economic'):
            period_replay(recorder.path,p,1000,1002,100000,p['parameters'])

    def test_restore_drill_checks_actual_content_and_refuses_overwrite(self):
        backup=self.root/'backup.db';restored=self.root/'restored.db'
        with sqlite3.connect(backup) as db:
            db.execute('CREATE TABLE intent(id TEXT,body BLOB)')
            db.execute('INSERT INTO intent VALUES(?,?)',('unsettled',b'abc'))
        result=restore_drill(backup,restored)
        self.assertTrue(result['content_verified'])
        self.assertEqual(result['tables']['intent']['rows'],1)
        self.assertFalse(result['production_modified'])
        with self.assertRaises(ValueError):restore_drill(backup,restored)

    def test_unattended_manifests_cannot_enable_live_orders(self):
        paths=service_manifests(self.root/'config.yaml',self.root/'study',self.root/'deploy')
        for path in paths:
            with path.open('rb') as f:manifest=plistlib.load(f)
            self.assertNotIn('--enable-live',manifest['ProgramArguments'])
            self.assertTrue(manifest['KeepAlive'])
        with paths[0].open('rb') as f:collector=plistlib.load(f)
        self.assertTrue(collector['ProgramArguments'][1].endswith('paper_service.py'))

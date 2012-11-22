#!/usr/bin/env python
"""Store usually used paths."""
import os

class Path:
    def __init__(self):
        pwd = os.getcwd()
        os.chdir(os.path.dirname(os.path.realpath(__file__)))
        self.TestConfigXml      = os.path.abspath('../../testspec/metadata/testconfig.xml')
        self.AnalyticsToolXml   = os.path.abspath('../../testspec/metadata/analyticstool.xml')
        self.ScheduleDir     = os.path.abspath('../../schedule/') + '/'
        self.BootstrapDir = os.path.abspath("../../bootstrap/") + '/'
        self.ReportDir       = os.path.abspath('../../report/') + '/'
        self.TestCaseDir     = os.path.abspath('../../testcase/') + '/'

        self.RootPath        = os.path.abspath("../../") + '/'
        self.algorithmsSpecXml = "algorithmspec.xml"
        self.CfgSpecPath   = os.path.abspath("../../testspec/metadata/") + '/'
        self.testconfigXml = "testconfig.xml"
        self.datasetXml = "dataset.xml"
        self.analyticstoolXml = "analyticstool.xml"
        self.templateExecutorPath = os.path.abspath('../executor/template_executor.py')
        self.caseSpecPath = os.path.abspath("../../testspec/casespec/") + '/'
        os.chdir(pwd)

import sys

SrcDir = '../src/'
sys.path.append(SrcDir)
from loader.loadingManager import loadingManager
from  utility import analytics_tool, file_path

Path = file_path.Path()

def main():
    analyticsTools = analytics_tool.AnalyticsTools(Path.AnalyticsToolXml)
    analyticsTools.parseTools()
    loading_manager = loadingManager(Path.RootPath, 'madlibtestdata', analyticsTools)
    loading_manager.do(None, False, True, True)


if __name__ == '__main__':
    main()

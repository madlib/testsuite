import sys

SrcDir = '../src/'
sys.path.append(SrcDir)
from loader.loadingManager import loadingManager

def main():
    loading_manager = loadingManager('..', 'madlibtestdata')
    loading_manager.do(None, False, True, True)


if __name__ == '__main__':
    main()

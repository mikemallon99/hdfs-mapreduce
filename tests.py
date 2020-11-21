import argparse
import logging


def test_split():
    return "DONE"


def parse_args():
    """
    parse the CLI arguments
    """
    description = '''
                Unit tests for maplejuice
                '''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("m_split", default=False, action="store_true")

    p_args = parser.parse_args()

    ret = None
    if p_args.m_split:
        ret = test_split()
    else:
        ret = "No test ran"
    print(ret)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parse_args()
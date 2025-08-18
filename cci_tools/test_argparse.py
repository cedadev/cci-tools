import argparse

def main():

    parser = argparse.ArgumentParser('h1')

    # Usual arguments which are applicable for the whole script / top-level args
    parser.add_argument('--verbose', help='Common top-level parameter',
                        action='store_true', required=False)

    # Same subparsers as usual
    subparsers = parser.add_subparsers(help='Desired action to perform', dest='action')

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-p', help='add db parameter', required=False)

    # Subparsers based on parent

    parser_create = subparsers.add_parser("create", parents=[parent_parser],
                                        help='Create something')
    # Add some arguments exclusively for parser_create

    parser_update = subparsers.add_parser("update", parents=[parent_parser],
                                        help='Update something')
    
    parser_create.parse_args()
    parser_update.parse_args()


if __name__ == '__main__':
    main()
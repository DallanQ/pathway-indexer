from pathway_indexer.get_indexes import get_indexes
from pathway_indexer.crawler import crawl_data
from pathway_indexer.parser import parse_files_to_md

def main():
    print("Getting indexes...")
    get_indexes()
    print("Crawler Started...")
    crawl_data()
    print("Parser Started...")
    parse_files_to_md()

if __name__ == '__main__':
    main()

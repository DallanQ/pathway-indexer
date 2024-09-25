# Steps to crawl the sites

1. Parse indexes running `parse-indexes.ipynb`. This will create the `acm_site.csv` and `missionary_site.csv`.

2. Run the `crawl_stdhndbk.ipynb` notebook to crawl the urls from the handbook and save as `stdhbk.csv`.

3. Run `crawl_url.ipynb` to crawl the urls in `acm_site.csv`, `missionary_site.csv`, and `stdhbk.csv`. This will output the html and pdf files in the `data` directory (you need to specify the folder).

4. Run `general_parse_html.ipynb` to parse the html files to markdown files.

5. Run `pdf_to_md.ipynb` to convert the pdf files to markdown files.

6. Run `one_column_quotes_to_train.ipynb`, it will create 2 files, one for validate 
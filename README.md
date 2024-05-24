# nwc2010-to-warc
Preprocessing script to convert NWC2010 HTML archive to synthesized WARC gzip.

[日本語ウェブコーパス2010](https://www.s-yata.jp/corpus/nwc2010/)のHTMLアーカイブデータを
Web Archiveフォーマット(WARC gzip)に変換するスクリプトです。

# Caveats

- Since the original data sometimes lacks appropriate HTTP headers and/or payloads,
  this script tries to restore that information from the original data,
  or may insert some relevant information.
  E.g., the script may set 2010-09-30T23:59:59Z as the retrieval timestamp
  (the estimated "last date" of crawling) if it doesn't exist.
  If you don't like these workarounds, please modify the corresponding part of the code by yourself.
- `html_to_warc.py` requires that the input be raw texts or gzipped stream.
  Since the original archive is distributed with xz (LZMA) compression,
  you may be required to decompress/re-compress the original stream.
  If your Python is installed with lzma extension,
  you could simply replace the `open` function in `html_to_warc.py` with `lzma.open`.

# Usage

```shell
# Process one archive file
python html_to_warc.py in_txt_gz out_warc_gz

# Process all archive files simultaneously
python run_parallel_jobs.py in_dir out_dir num_processes
```
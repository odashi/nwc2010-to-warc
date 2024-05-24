import argparse
import dataclasses
import datetime
import email.utils
import gzip
import http
import io
import logging
import pathlib
from typing import Iterator
import uuid

logging.basicConfig(level=logging.INFO)


def iter_bytes(p: pathlib.Path) -> Iterator[int]:
    """Iterate over all characters in the given file."""
    open_fn = gzip.open if p.suffix == ".gz" else open
    with open_fn(p, "rb") as fp:
        while buffer := fp.read(1_000_000):
            yield from buffer


def read_chunk(it: Iterator[int], min_bytes: int | None = None) -> bytes:
    """Retrieve next chunk of lines."""
    out_buffer: list[int] = []
    nl_char = ord("\n")

    if min_bytes is None:
        out_buffer.append(next(it))
        while out_buffer[-1] != nl_char:
            out_buffer.append(next(it))
    else:
        for _ in range(min_bytes):
            out_buffer.append(next(it))
    
    return bytes(out_buffer)


def find_header_key(header: dict[str, str], key: str) -> str:
    """Search for corresponding header key."""
    lowered = key.lower()
    for k in header:
        if k.lower() == lowered:
            return k
    return key  # use specified key


def convert_date(dt_str: str) -> datetime.datetime | None:
    """Convert date format from HTTP to W3CDTF."""
    try:
        dt_tuple = email.utils.parsedate(dt_str)
        return datetime.datetime(*dt_tuple[:6])
    except Exception as ex:
        logging.warning(f"Error on datetime conversion: {ex}")
        return None


# Dummy timestamp, defined as the estimated last date of crawling.
FINAL_DATE_HTTP = "Thu, 30 Sep 2010 23:59:59 GMT"
FINAL_DATE: datetime.datetime = convert_date(FINAL_DATE_HTTP)


@dataclasses.dataclass(frozen=True)
class Document:
    id: uuid.UUID
    url: str
    date: datetime.datetime
    status: int
    header: dict[str, str]
    body: bytes


def parse_header(raw: bytes) -> dict[str, str]:
    """Converts raw header bytes into dictionary."""
    decoded = raw.decode("latin_1").rstrip()
    lines = [x.rstrip() for x in decoded.split("\n")]
    kv = [x.split(":", 1) for x in lines]

    kv_processed: list[tuple[str, str]] = []
    for x in kv:
        if len(x) < 2:
            # Maybe continuation of the previous entry
            if not kv_processed:
                logging.warning(f"Invalid header sequence found, skipped: {x[0]}")
            else:
                logging.warning(f"Invalid header sequence found, concatenated to the previous header: {x[0]}")
                kv_processed[-1][1] += x[0]
        else:
            kv_processed.append(x)
    
    return {k.strip(): v.strip() for k, v in kv_processed}
                

def iter_documents(p: pathlib.Path) -> Iterator[Document]:
    """Iterate over all documents."""
    it = iter_bytes(p)

    try:
        while True:
            url = read_chunk(it).decode("ascii").rstrip("\n")
            status = int(read_chunk(it))
            header_length = int(read_chunk(it))
            header_raw = read_chunk(it, header_length)
            body_length = int(read_chunk(it))
            body = read_chunk(it, body_length)

            if header_length == 0 or body_length == 0:
                continue

            if not url.startswith("http"):
                logging.warning(f"{url}: URL doesn't start with 'http'")
            
            header = parse_header(header_raw)

            content_length_key = find_header_key(header, "Content-Length")
            content_type_key = find_header_key(header, "Content-Type")
            date_key = find_header_key(header, "Date")

            if content_length_key not in header:
                # NOTE(odashi): No logging: most entries have no Content-Length header.
                header[content_length_key] = str(body_length)
            elif (body_length_in_header := int(header[content_length_key])) != body_length:
                logging.warning(f"{url}: Content-Length mismatched: {body_length_in_header} != {body_length}")
                header[content_length_key] = str(body_length)

            if content_type_key not in header:
                logging.warning(f"{url}: No header: Content-Type")
                header[content_type_key] = "text/html"

            if date_key not in header:
                logging.warning(f"{url}: No header: Date")
                header[date_key] = FINAL_DATE_HTTP
            
            if (date_converted := convert_date(header[date_key])) is not None:
                date = date_converted
            else:
                logging.warning(f"{url}: Invalid datetime format: {header[date_key]}")
                header[date_key] = FINAL_DATE_HTTP
                date = FINAL_DATE

            yield Document(uuid.uuid4(), url, date, status, header, body)
    except StopIteration:
        pass


def make_http_response(doc: Document) -> bytes:
    """Generates simulated HTTP response from the given document."""
    b = io.BytesIO()

    try:
        status_message = f"{doc.status} {http.HTTPStatus(doc.status).phrase}"
    except Exception as ex:
        logging.warning(f"Error occurred during parsing HTTP status: {ex}")
        status_message = str(doc.status)

    b.write(f"HTTP/1.1 {status_message}\r\n".encode("ascii"))
    for k, v in doc.header.items():
        b.write(k.encode("latin_1") + b": " + v.encode("latin_1") + b"\r\n")
    b.write(b"\r\n")

    return b.getvalue() + doc.body


def make_warc_record(doc: Document) -> bytes:
    """Generates gzip-ed WARC byte stream of the given document."""
    payload = make_http_response(doc)

    b = io.BytesIO()

    b.write(b"WARC/1.0\r\n")
    b.write(b"WARC-Type: response\r\n")
    b.write(f"WARC-Record-ID: <urn:uuid:{doc.id}>\r\n".encode("ascii"))
    b.write(f"WARC-Date: {doc.date.strftime("%Y-%m-%dT%H:%M:%SZ")}\r\n".encode("ascii"))
    b.write(f"WARC-Target-URI: {doc.url}\r\n".encode("ascii"))
    b.write(f"Content-Length: {len(payload)}\r\n".encode("ascii"))
    b.write(b"Content-Type: application/http; msgtype=response\r\n")
    b.write(b"\r\n")
    
    return b.getvalue() + payload


@dataclasses.dataclass(frozen=True)
class Args:
    in_file: pathlib.Path
    out_file: pathlib.Path


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("in_file", type=pathlib.Path, help="Input NWC2010 HTML file")
    p.add_argument("out_file", type=pathlib.Path, help="Output WARC file")
    args = p.parse_args()
    return Args(args.in_file, args.out_file)


def main():
    args = parse_args()

    args.out_file.parent.mkdir(mode=0o755, parents=True, exist_ok=True)

    with args.out_file.open("wb") as out_file:
        for doc in iter_documents(args.in_file):
            warc_record = make_warc_record(doc)
            out_file.write(gzip.compress(warc_record))


if __name__ == "__main__":
    main()
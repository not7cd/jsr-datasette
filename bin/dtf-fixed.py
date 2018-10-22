"""
by @not7cd

Used to read DTF files provided by JSR database

DTF files are used by chandra x-ray observatory and they provide tooling.
Unfortunetly I had no luck with JSR db.

From limited research, they resemble structure simillar to FITS files.
Main diffrence, DTF file can have it's file in ASCII form.
"""


def dd_gen(rows):
    for row in rows:
        try:
            key, value = row.split("=")
        except Exception:
            pass
        else:
            value, *comment = value.split("/")
            key = key.strip()
            value = value.replace("\'", "").strip()
            yield key, value


def fixed_headers_gen(header_str):
    header = {key: value for key, value in dd_gen(header_str.split("\n"))}
    print(header)

    for f in range(int(header["TFIELDS"])):

        name = str(header.get("TTYPE" + str(f), ""))
        col = int(header["TBCOL" + str(f + 1)]) - 1
        yield name, col


def split_row(row, start_bytes):
    if len(start_bytes) <= 1:
        return [row]

    for byte in start_bytes[1:]:
        return [row[:byte].strip()] + split_row(row[byte:], list(map(lambda b: b - byte, start_bytes[1:])))  


if __name__ == '__main__':
    HEADER_FILE = "sdb/sdb/Sites_header"

    with open(HEADER_FILE) as f:
        raw = f.read()
        print(raw)

    # raw, raw_data = raw.split("END")    

    headers = [(k, v) for k, v in fixed_headers_gen(raw)]
    # headers = {k: headers[k] for k in sorted(headers.keys())}

    # print(headers)

    FILE = "sdb/sdb/Sites_data"

    with open(FILE) as f:
        raw = f.read()
        print(raw)

    data_rows = raw.lstrip().split("\n")

    start_bytes = [num for name, num in headers]
    print(start_bytes)

    print(split_row(data_rows[0], start_bytes))

    import csv
    with open('sites.csv', 'w') as csvfile:
        spamwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow([name for name, num in headers])
        for row in data_rows:
            spamwriter.writerow(split_row(row, start_bytes))

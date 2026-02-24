import os
import re
import pandas as pd


def arff_to_dataframe(arff_path):
    # Parse attributes and then read data with tolerant splitting/stripping
    attributes = []
    numeric_cols = set()
    in_data = False
    data_rows = []

    attr_re = re.compile(r"@attribute\s+(?:'(?P<qname>[^']+)'|(?P<name>[^\s]+))\s+(?P<type>.+)", re.IGNORECASE)

    with open(arff_path, "r", errors='replace') as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('%'):
                continue
            if not in_data:
                if line.lower().startswith('@attribute'):
                    m = attr_re.match(line)
                    if m:
                        name = m.group('qname') or m.group('name')
                        typ = m.group('type').strip()
                        # normalize name
                        name = name.strip().strip('"').strip("'")
                        attributes.append(name)
                        if typ.lower().startswith('numeric') or typ.lower().startswith('real') or typ.lower().startswith('integer'):
                            numeric_cols.add(name)
                elif line.lower().startswith('@data'):
                    in_data = True
                else:
                    continue
            else:
                # tolerant cleaning: remove trailing commas, strip whitespace
                # keep commas inside braces/quotes? dataset is simple CSV-like so split on commas is fine
                clean = line.rstrip(',')
                # skip comments or empty lines
                if not clean or clean.startswith('%'):
                    continue
                parts = [p.strip() for p in clean.split(',')]
                # remove empty trailing tokens
                if parts and parts[-1] == '':
                    parts = parts[:-1]
                # pad or trim to match attributes
                if len(parts) > len(attributes):
                    parts = parts[:len(attributes)]
                elif len(parts) < len(attributes):
                    parts = parts + [None] * (len(attributes) - len(parts))
                # normalize missing value markers
                parts = [None if (p is None or p == '?' or p == '') else p for p in parts]
                data_rows.append(parts)

    df = pd.DataFrame(data_rows, columns=attributes)

    # convert numeric columns
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # strip spaces in object columns
    for c in df.select_dtypes([object]).columns:
        df[c] = df[c].apply(lambda x: x.strip() if isinstance(x, str) else x)

    return df


def main():
    root = os.path.dirname(__file__)
    arff_path = os.path.join(root, "data", "chronic_kidney_disease.arff")
    csv_path = os.path.join(root, "data", "chronic_kidney_disease.csv")

    df = arff_to_dataframe(arff_path)
    df.to_csv(csv_path, index=False)
    print(f"Wrote CSV: {csv_path} (shape: {df.shape})")


if __name__ == '__main__':
    main()

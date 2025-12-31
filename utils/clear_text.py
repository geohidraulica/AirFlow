def clean_text(value):
    if value is None:
        return value

    return (
        str(value)
        .replace('|', ' ')
        .replace(',', ' ')
        .replace('\\', ' ')
        .replace('\n', ' ')
        .replace('\r', ' ')
        .replace('\t', ' ')
        .replace('"', ' ')
        .replace('\x00', ' ')
        .replace('\x1a', ' ')
    )

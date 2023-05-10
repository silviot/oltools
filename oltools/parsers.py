def extract_json(textline):
    """We need to get the JSON content of this line.
    We know the line has a prefix. We assume the character `{` does not appear in the prefix.
    """
    # We first find the index of the first `{` character.
    index = textline.find("{")
    return textline[index:]

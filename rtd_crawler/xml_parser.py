def xml_to_json(xml):
    """
    A recursive function to convert xml to list dict mix.

    Parameters
    ----------
    xml : lxml.etree
        The xml to convert

    Returns
    -------
    dict
        A dict list mix of the xml
    """
    parsed = dict(xml.attrib)
    for xml_child in list(xml):
        if xml_child.tag in parsed:
            parsed[xml_child.tag].append(xml_to_json(xml_child))
        else:
            parsed[xml_child.tag] = [xml_to_json(xml_child)]
    return parsed

import sqlite3
from datetime import datetime
from os.path import basename
from os.path import join
from time import perf_counter

from copy import copy

from os.path import exists, dirname

from os import makedirs
from xml.dom import minidom
from xml.etree import ElementTree as et

import logging


def transform_non_html_nodes(xml_document):
    msg_node = xml_document.childNodes[0]
    for node in msg_node.childNodes:
        if node.nodeType == xml_document.ELEMENT_NODE:
            escaped = escape_node(node)
            if isinstance(escaped, list):
                # list of files
                for new_node in escaped:
                    msg_node.insertBefore(new_node, node)
                msg_node.removeChild(node)
            else:
                msg_node.replaceChild(
                    escape_node(node),
                    node)


def get_media_tag(node):
    # TODO: improve this
    logging.warning("Converting media tag '{n}', currently unsupported".format(n=node.tagName))
    logging.debug(node.toxml())
    if node.tagName.lower() == "uriobject":
        return node.ownerDocument.createTextNode("[IMAGE]")
    elif node.tagName.lower() == "file":
        return node.ownerDocument.createTextNode("[FILE {f}]".format(f=node.firstChild.data))

    return node.ownerDocument.createTextNode("")

def escape_node(minidom_node):
    tag = minidom_node.tagName.lower()
    if tag == "ss":
        return minidom_node.ownerDocument.createTextNode(minidom_node.firstChild.data)
    if tag == "files":
        result = ""
        for file_tag in minidom_node.childNodes:
            result = []
            # goddamn blank space, be careful
            if file_tag.nodeType == minidom_node.ELEMENT_NODE:
                result.append(get_media_tag(file_tag))
        return result
    if tag == "uriobject":
        return get_media_tag(minidom_node)
    logging.debug("Escaped weird node: " + tag)
    return minidom_node

class RecordSerializer:

    HTML_ROW_TEMPLATE = """<tr class="{row_class}">
    <td class="timestamp">{timestamp}</td>
    <td class="td_user"><span class="username" alt="{user_id}">{user_name}</span></td>
    <td class="td_msg"><span class="msg" id="{msg_id}">{msg}</span></td>
</tr>"""




    def to_html(self, columns, record_tuple, *args, **kwargs):
        record = {"msg_id": record_tuple[columns["id"]],
                  "msg": "",
                  "row_class": "row_odd",
                  "timestamp": "",
                  "user_id": "",
                  "user_name": "",
                  }

        if columns.get("timestamp", None):
            ts = record_tuple[columns.get("timestamp")] or 0
            record["timestamp"] = datetime.fromtimestamp(ts).isoformat(" ") or ""
        if columns.get("author", None):
            record["user_id"] = record_tuple[columns["author"]] or ""
        if columns.get("from_dispname", None):
            record["user_name"] = record_tuple[columns["from_dispname"]] or ""
        if columns.get("body_xml", None):
            content = record_tuple[columns["body_xml"]] or ""
            xml_element = minidom.parseString("<msg>"+ content +"</msg>")
            transform_non_html_nodes(xml_element)
            record["msg"] = xml_element.toxml()

        if kwargs.get("row_index", 1) % 2 == 0:
            record["row_class"] = "row_even"

        return self.HTML_ROW_TEMPLATE.format(**record)


class ConversationSerializer:

    HTML_HEADER_TEMPLATE="""<!DOCTYPE html><html><head>
    <meta charset="utf-8"/>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
<title>{title}</title></head>
<style>
body {{ font-family: Helvetica, Verdana, Arial, sans-serif; font-size: small;}}
table {{ width: 100%; }}
#footer {{font-size: xx-small; }}
td.timestamp {{ font-size: x-small; text-align: center;}}
td.td_user {{ text-align: center; padding: 2px;}}
td.td_msg {{ padding: 4px; }}
span.username {{ color: #536073; font-size: xx-small; font-weight: bold;}}
.row_even {{ background-color: #D4F1FC; }}


</style>
    <body><table>
    {cols}
    <thead>
    {headers}
    </thead><tbody>
    """
    HTML_FOOTER_TEMPLATE="""</tbody></table><div id="footer">Exported by {name} in {secs} seconds.</div></body></html>"""

    TABLE_HEADERS = ["Timestamp", "User", "Message"]

    def _make_headers(self, *args, **kwargs):

        # currently a hack
        cols ="""<col style="width:10%">
        <col style="width:10%">
        <col style="width:80%">"""

        headers = "<tr><th>" + "</th><th>".join(self.TABLE_HEADERS) + "</th></tr>"
        header = self.HTML_HEADER_TEMPLATE.format(cols=cols,
                                                  headers=headers,
                                                  title=kwargs.get("title", ""))
        return header

    def _make_footer(self, *args, **kwargs):
        details = {
            "name": "skypescape",
            "secs": kwargs.get("seconds", 0)
        }
        return self.HTML_FOOTER_TEMPLATE.format(**details)

    def to_html(self, columns, records, fileobj=None, *args, **kwargs):
        row_serializer = RecordSerializer()
        if fileobj is not None:

            fileobj.write(self._make_headers())
            start = perf_counter()
            for index, record in enumerate(records):
                fileobj.write(row_serializer.to_html(columns, record, row_index=index))
            end = perf_counter()

            fileobj.write(self._make_footer(seconds=(end-start)))
            return fileobj

        else:
            content = self._make_headers()
            start = perf_counter()
            for index, record in enumerate(records):
                content += row_serializer.to_html(columns, record, row_index=index)
            end = perf_counter()
            content += self._make_footer(seconds=(end-start))

            return content


class ConversationExtractor:
    def __init__(self, db_connection, convo_ids=None):
        self.connection = db_connection
        self.convo_ids = []
        if isinstance(convo_ids, list):
            self.convo_ids = copy(convo_ids)
        elif isinstance(convo_ids, int):
            self.convo_ids.append(convo_ids)

    def dump(self, file_name=None, overwrite=False, max_years=10):
        current_year = datetime.now().year
        thresholds = {}
        for i in range(current_year+1, current_year+1-max_years, -1):
            thresholds[i] = int(datetime(year=i, month=1, day=1).timestamp())
        year = current_year
        while year in thresholds.keys():
            query = "select m.id, m.from_dispname, m.body_xml, m.author, m.timestamp, " + \
                "m.chatmsg_type, m.type "+ \
                "from messages m, conversations c "+ \
                "where m.convo_id = c.id and m.convo_id IN( {id_list} ) "+ \
                "and m.timestamp >= {low_threshold} and m.timestamp < {high_threshold} "+ \
                "order by timestamp__ms asc"
            query = query.format(
                    id_list=",".join([str(i) for i in self.convo_ids]),
                    low_threshold=thresholds[year],
                    high_threshold=thresholds[year+1]
                )
            queryset = self.connection.execute(query)
            col_names = {description[0]:index for index, description in enumerate(queryset.description)}
            serializer = ConversationSerializer()
            if file_name is not None:

                year_fname = join(dirname(file_name), str(year)+"_"+basename(file_name))

                if exists(year_fname) and not overwrite:
                    raise Exception("Target file already exists: {f}".format(f=year_fname))
                if not exists(dirname(year_fname)):
                    makedirs(dirname(year_fname))
                with(open(year_fname, "w", encoding="utf-8")) as f_out:
                    serializer.to_html(col_names, queryset, f_out)
                year -= 1
            else:
                return serializer.to_html(col_names, queryset)

if __name__ == "__main__":
    conn = sqlite3.connect("test_data/giacomolacava/main.db")
    extractor = ConversationExtractor(conn, [101405,182685,59214,185538])
    extractor.dump("test_data/dailygrind.html", overwrite=True)
    logging.info("Completed!")
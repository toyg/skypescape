import base64
import logging
import sqlite3
import sys
from datetime import datetime
from os import makedirs
from os.path import dirname
from os.path import exists
from time import perf_counter


class ConversationDescriptionSerializer:
    HTML_ROW_TEMPLATE = """<tr class="{row_class}">
    <td class="td_name">{name}</td>
    <td class="td_id">{conv_id}</td>
    <td class="td_img">{image}</td>
    <td class="td_numpeople">{num_participants}</td>
    <td class="td_people">{participants}</td>
    <td class="timestamp">{last_activity_ts}</td>
    <td class="timestamp">{created_ts}</td>
    </tr>"""

    COLOR_THRESHOLDS = [
        (500, "green"),
        (1000, "yellow"),
        (5000, "orange"),
        (10000, "red")
    ]

    def to_html(self, columns, row, *args, **kwargs):
        record = {"name": "",
                  "image": "",
                  "conv_id": row[columns["convo_id"]],
                  "num_participants": "",
                  "participants": "",
                  "last_activity_ts": "",
                  "created_ts": "",
                  "row_class": "row_odd"
                  }
        if row[columns["picture"]] is not None:
            if row[columns["picture"]] != "0":
                url = row[columns["picture"]].split(' ')[1]
                logging.debug(url)
                record["image"] = '<a href="{url}"><small>Download</small></a>'.format(url=url)
        elif row[columns["meta_picture"]] is not None:
            img_b64 = base64.b64encode(row[columns["meta_picture"]]).decode("utf-8").replace("\n", '')
            record["image"] = """<img src="data:image/png;base64,{b64}" width="64"/>""".format(b64=img_b64)
        logging.debug(row[columns["displayname"]])
        record["name"] = row[columns["displayname"]] or ""
        if columns.get("people", None):
            record["participants"] = row[columns["people"]] or ""
        if columns.get("numpeople", None):
            record["num_participants"] = row[columns["numpeople"]] or ""
        if columns.get("creation_timestamp", None):
            ts = row[columns.get("creation_timestamp")] or 0
            record["created_ts"] = datetime.fromtimestamp(ts).isoformat(" ") or ""
        if columns.get("last_activity_timestamp", None):
            ts = row[columns.get("last_activity_timestamp")] or 0
            record["last_activity_ts"] = datetime.fromtimestamp(ts).isoformat(" ") or ""

        if kwargs.get("row_index", 1) % 2 == 0:
            record["row_class"] = "row_even"
        stats = kwargs.get("stats")
        if stats:
            num_messages = stats.get(row[columns["convo_id"]])
            if num_messages:
                chosen_color = ""
                for threshold, class_name in self.COLOR_THRESHOLDS:
                    if num_messages > threshold:
                        chosen_color = class_name

                record["row_class"] += " " + chosen_color

        return self.HTML_ROW_TEMPLATE.format(**record)


class ConversationListSerializer:
    HTML_HEADER_TEMPLATE = """<!DOCTYPE html><html><head>
    <meta charset="utf-8"/>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
<title>Skype Conversations</title></head>
<style>
body {{ font-family: Helvetica, Verdana, Arial, sans-serif; font-size: small;}}
table {{ width: 100%; }}
#footer {{font-size: xx-small; }}
td.timestamp {{ font-size: x-small; text-align: center;}}
td.td_numpeople {{ text-align: center; }}
td.td_people, td.td_id, td.td_img {{ text-align: center; }}
td.td_name{{ font-weight: bold; }}

.row_even {{ background-color: #D4F1FC; }}
.red {{ background-color: rgba(255,0,1,0.69); }}
.orange {{ background-color: rgba(219,150,3,0.77); }}
.yellow {{ background-color: rgba(255,244,4,0.44); }}
.green {{ background-color: rgba(30,255,0,0.35); }}

</style>
    <body><table>
    {cols}
    <thead>
    {headers}
    </thead><tbody>
    """
    HTML_FOOTER_TEMPLATE = """</tbody></table><div id="footer">Exported by {name} in {secs} seconds.</div></body></html>"""

    TABLE_HEADERS = ["Chat Name", "Chat ID", "Image", "Num. People", "People", "Last Activity", "Created"]

    def _make_headers(self, *args, **kwargs):
        # currently a hack
        cols = """<col style="width:15%">
                <col style="width:5%">
                <col style="width:5%">
                <col style="width:5%">
                <col style="width:40%">
                <col style="width:10%">
                <col style="width:10%">
                <col style="width:10%">"""

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

    def to_html(self, columns, queryset, fileobj=None, *args, **kwargs):
        row_serializer = ConversationDescriptionSerializer()
        if fileobj is not None:

            fileobj.write(self._make_headers())
            start = perf_counter()
            for index, record in enumerate(queryset):
                logging.debug(index)
                fileobj.write(row_serializer.to_html(columns, record, row_index=index, stats=kwargs.get("stats")))
            end = perf_counter()

            fileobj.write(self._make_footer(seconds=(end - start)))
            return fileobj

        else:
            content = self._make_headers()
            start = perf_counter()
            for index, record in enumerate(queryset):
                content += row_serializer.to_html(columns, record, row_index=index, stats=kwargs.get("stats"))
            end = perf_counter()
            content += self._make_footer(seconds=(end - start))

            return content


class ConversationListExtractor:
    def __init__(self, db_connection):
        self.connection = db_connection

    def _get_queryset(self):
        query = """select
c.displayname,
c.picture,
c.meta_picture,
c.id as convo_id,
count(c.id) as numpeople ,
group_concat(ct.displayname, ", ") as people,
c.creation_timestamp,
c.last_activity_timestamp
from conversations c, participants p, contacts ct
where c.id = p.convo_id
and p.identity = ct.skypename
group by c.id
order by numpeople desc"""
        queryset = self.connection.execute(query)
        return queryset

    def _get_messages_stats(self):
        query = """select convo_id, count(convo_id) as num
from messages group by convo_id
order by num desc"""
        recs = self.connection.execute(query).fetchall()
        result = {cid: num for cid, num in recs}
        logging.debug(result)
        return result

    def extract_list(self, file_name=None, overwrite=False):
        qs = self._get_queryset()
        col_names = {description[0]: index for index, description in enumerate(qs.description)}

        serializer = ConversationListSerializer()
        if file_name is not None:

            if exists(file_name) and not overwrite:
                raise Exception("Target file already exists: {f}".format(f=file_name))
            if not exists(dirname(file_name)):
                makedirs(dirname(file_name))
            with(open(file_name, "w", encoding="utf-8")) as f_out:
                serializer.to_html(col_names, qs, f_out, stats=self._get_messages_stats())
            return True
        else:
            return serializer.to_html(col_names, qs, stats=self._get_messages_stats())


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Specify path to main.db (and optionally, path to HTML file to output).")
        sys.exit(1)

    out_file_name = "conversations.html"
    if len(sys.argv) > 2:
        out_file_name = sys.argv[2]

    logging.getLogger().setLevel(logging.ERROR)

    conn = sqlite3.connect(sys.argv[1])
    extractor = ConversationListExtractor(conn)
    extractor.extract_list(out_file_name, overwrite=False)
    logging.info("Completed export!")

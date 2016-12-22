Skypescape
-----

These are a couple of Python scripts to dump some stuff out of Skype. 
To use, you'll need to find the `main.db` file in your Skype folder (NOTE: 
I would **strongly** recommend copying it to a safe place before doing
anything with it.)

Usage:

- `python list_conversations.py /path/to/main.db [/path/to/output.html]`
- `python dump_chat.py /path/to/main.db id1,id2,... [/path/to/output.html]`

Output path is optional in both cases. 
It will default to `conversations.html` and `chat_transcript.html` respectively.

`dump_chat.py` needs a second parameter which is a comma-separated list of numeric chat IDs.
You can get them by first running `list_conversations.py`.

Scripts were tested with Python 3.5.2 only.

Motivation
--------
There are a number of tools out there  for this sort of export, but most of them 
are Windows-only or they just suck. I wanted something simple, quick and 
cross-platform to export a few chats, so here I am.

License
-------

(c) 2016 Giacomo Lacava. 
Released under terms of the MIT license (see LICENSE file).

#!/usr/bin/env python
# encoding: utf8

import sys
import os
import os.path
import re
from datetime import datetime
from collections import namedtuple

import pandas as pd

import seaborn as sns
sns.set_style("whitegrid")

from matplotlib import pyplot as plt
from matplotlib import rc
# For cyrillic labels
rc('font', family='Verdana', weight='normal')


AFISHA_DIR = 'data'
STAR = '*'
NL = '\n'
CANCELED = u'Отменен'
MOVED = u'Перенесен'
NEW = u'Новое'


def list_afishas(dir=AFISHA_DIR):
    for filename in sorted(os.listdir(dir)):
        yield os.path.join(dir, filename)


def read_raw_afisha(path):
    with open(path) as file:
        for line in file:
            yield line


def tokenize_raw_afisha(lines):
    for line in lines:
        line = line.rstrip('\n').decode('utf8')
        for token in line.split('\t'):
            if token:
                yield token
        yield NL


def skip_header(tokens):
    skipped = False
    for token in tokens:
        if token == u'Авиамодельный спорт':
            skipped = True
        if skipped:
            yield token


def tokenize_afisha(path):
    return skip_header(
        tokenize_raw_afisha(
            read_raw_afisha(path)
        )
    )
            

def parse_id(token):
    return int(token)


def is_id(token):
    return token.isdigit()


def parse_date(token, format='%d.%m.%Y'):
    return datetime.strptime(token, format)


def is_date(token):
    return bool(re.match(token, '\d\d.\d\d.\d\d\d\d'))


def parse_title(token):
    return token.capitalize()


def parse_address(token):
    return token.strip()


def parse_participants(token):
    if token.isdigit():
        return int(token)


def parse_participants_description(token):
    return token.strip()


def parse_event_description(token):
    return token.strip()


Participants = namedtuple('Participants', 'description, number')
Event = namedtuple('Event', ['section', 'subsection', 'title',
                             'start', 'stop',
                             'description', 'address', 'participants'])


def make_next_token(tokens):
    tokens = iter(tokens)
    def next_token(tokens=tokens):
        token = next(tokens)
        # print '"' + token + '"'
        return token
    return next_token


def is_epk(token):
    return bool(re.search(ur'ЕКП \d\d\d\d', token))


def is_page_number(token):
    return bool(re.match(ur'Стр. \d+ из \d+', token))


def parse_subsection(token):
    subsection = token.strip()
    if subsection:
        return subsection


def join_address(*parts):
    return ', '.join(_ for _ in parts if _)


def is_canceled(token):
    return token in (CANCELED, MOVED)


def is_new(token):
    return token.strip() == NEW


def parse_tokens(tokens):
    next_token = make_next_token(tokens)
    section = []
    while True:
        token = next_token()
        previous = section
        section = []
        while not is_id(token):
            if token != NL and not is_epk(token) and not is_page_number(token):
                subsection = parse_subsection(token)
                if subsection:
                    section.append(subsection)
            token = next_token()
        size = len(section)
        assert size < 4
        if size == 0:
            section = previous
        elif size == 1:
            section = (previous[0], section[0])
        elif size == 3:
            print >>sys.stderr, '3 subsection section', ', '.join(section)
            section = (section[1], section[2])
        id = parse_id(token)
        token = next_token()
        if token == STAR:
            token = next_token()
        title = parse_title(token)
        start = parse_date(next_token())
        address1 = parse_address(next_token())
        token = next_token()
        if token == NL:
            participants = None
        else:
            participants = parse_participants(token)
            assert next_token() == NL
        participants_description = parse_participants_description(next_token())
        stop = parse_date(next_token())
        address2 = parse_address(next_token())
        assert next_token() == NL
        token = next_token()
        canceled = False
        if is_new(token):
            token = next_token()
        elif is_canceled(token):
            canceled = True
            token = next_token()
        event_description = parse_event_description(token)
        token = next_token()
        if token != NL:
            address3 = parse_address(token)
            assert next_token() == NL
        else:
            address3 = None
        address = join_address(address1, address2, address3)
        main, subsection = section
        yield Event(
            main, subsection, title,
            start, stop,
            event_description,
            address,
            Participants(participants_description, participants)
        )


def parse_afisha(path):
    return parse_tokens(tokenize_afisha(path))


def parse_afishas(dir=AFISHA_DIR):
    for path in list_afishas(dir=dir):
        for event in parse_afisha(path):
            yield event


def make_table(events):
    data = [
        (_.section, _.subsection,
         _.title, _.description,
         _.start, _.stop,
         _.address, _.participants.number)
        for _ in events
    ]
    return pd.DataFrame(
        data,
        columns=['section', 'subsection',
                 'title', 'description',
                 'start', 'stop',
                 'address', 'participants']
    )


import sys, re

from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, \
    Optional, Combine, Group, alphas, nums, alphanums, ParseException, Forward, \
    oneOf, quotedString, ZeroOrMore, restOfLine, Keyword, QuotedString, \
    restOfLine, OnlyOnce, Or, removeQuotes, Regex, Suppress, OneOrMore

sql_select = Forward()

tok_sql_open_paren = Literal("(")
tok_sql_close_paren = Literal(")")
tok_sql_equals = Literal("=")
tok_sql_plus = Literal("+")
tok_sql_comment = Literal("#")
tok_sql_op = oneOf("= + * / % < > in", caseless=True)

tok_sql_literal_insert = Keyword("insert", caseless=True)
tok_sql_literal_select = Keyword("select", caseless=True)
tok_sql_literal_update = Keyword("update", caseless=True)
tok_sql_literal_delete = Keyword("delete", caseless=True)
tok_sql_literal_begin = Keyword("begin", caseless=True)
tok_sql_literal_use = Keyword("use", caseless=True)
tok_sql_literal_as = Keyword("as", caseless=True)
tok_sql_literal_set = Keyword("set", caseless=True)
tok_sql_literal_from = Keyword("from", caseless=True)
tok_sql_literal_commit = Keyword("commit", caseless=True)
tok_sql_literal_rollback = Keyword("rollback", caseless=True)
tok_sql_literal_into = Keyword("into", caseless=True)
tok_sql_literal_order = Keyword("order", caseless=True)
tok_sql_literal_group = Keyword("group", caseless=True)
tok_sql_literal_having = Keyword("having", caseless=True)
tok_sql_literal_by = Keyword("by", caseless=True)
tok_sql_literal_limit = Keyword("limit", caseless=True)
tok_sql_literal_where = Keyword("where", caseless=True)
tok_sql_literal_set = Keyword("set", caseless=True)
tok_sql_literal_ignore = Keyword("ignore", caseless=True)
tok_sql_literal_values = Keyword("values", caseless=True)
tok_sql_literal_on = Keyword("on", caseless=True)
tok_sql_literal_duplicate = Keyword("duplicate", caseless=True)
tok_sql_literal_key = Keyword("key", caseless=True)
tok_sql_literal_update = Keyword("update", caseless=True)
tok_sql_literal_ignore = Keyword("ignore", caseless=True)
tok_sql_literal_semicol = Word(";")

tok_sql_identifier = Word(alphanums + "_.-")
tok_sql_inline_comment = Regex("/\*.*?\*/")
tok_sql_quoted_value = \
    (QuotedString("'", "\\", r"\'", True, False) ^ 
     QuotedString('"', "\\", r'\"', True, False))
tok_sql_table_alias = \
    ((tok_sql_identifier + Suppress(Optional(tok_sql_literal_as)) + tok_sql_identifier |
      tok_sql_identifier) | \
         (Suppress(tok_sql_open_paren) + Suppress(sql_select) + Suppress(tok_sql_close_paren) +
          Suppress(tok_sql_literal_as) + tok_sql_identifier))
tok_sql_computed_value = tok_sql_identifier + tok_sql_plus + \
    (tok_sql_quoted_value | tok_sql_identifier)
tok_sql_func_call = Forward()
tok_sql_func_call << tok_sql_identifier + Suppress("(") + \
    ZeroOrMore(Group(tok_sql_func_call) | tok_sql_identifier | tok_sql_quoted_value | 
               tok_sql_op | Suppress(",")) + Suppress(")")
tok_sql_loose_val = \
    ((Suppress("(") + sql_select + Suppress(")") + \
         tok_sql_literal_as + tok_sql_identifier) | tok_sql_func_call | \
         tok_sql_identifier | tok_sql_quoted_value | \
         tok_sql_computed_value)

tok_sql_kvp = tok_sql_identifier + tok_sql_op + tok_sql_loose_val
tok_sql_cols = delimitedList(tok_sql_identifier) 
tok_sql_table_list = delimitedList(tok_sql_table_alias) 
tok_sql_vals = delimitedList(tok_sql_loose_val)
tok_sql_kvp_list = delimitedList(tok_sql_kvp)
tok_sql_where_clause = \
    (tok_sql_kvp)

sql_select << (tok_sql_literal_select.setResultsName("op") + \
                   Optional(tok_sql_inline_comment) + \
                   (Literal("*") | delimitedList(tok_sql_loose_val)).\
                        setResultsName("column_list") + \
                   tok_sql_literal_from + \
                   tok_sql_table_list.setResultsName("table_list") + \
                   Optional(tok_sql_literal_where + \
                                tok_sql_where_clause.\
                                setResultsName("where_clause")) + \
                   Optional(tok_sql_literal_group + \
                                Suppress(tok_sql_literal_by) + \
                                tok_sql_cols.setResultsName("groupby_clause") + \
                                Optional(tok_sql_literal_having +
                                         tok_sql_kvp_list.\
                                             setResultsName("having_clause"))))

sql_insert = (tok_sql_literal_insert.setResultsName("op") + \
                  Optional(tok_sql_literal_ignore) + \
                  tok_sql_literal_into + \
                  tok_sql_identifier.setResultsName("table") + \
                  Optional(tok_sql_open_paren + \
                               tok_sql_cols.setResultsName("cols") + \
                               tok_sql_close_paren 
                           ) + \
                  Optional(tok_sql_literal_values) + \
                  Optional(tok_sql_open_paren) + \
                  tok_sql_vals.setResultsName("vals") + \
                  Optional(tok_sql_close_paren) + \
                  Optional(tok_sql_literal_on + \
                               tok_sql_literal_duplicate + \
                               tok_sql_literal_key + \
                               tok_sql_literal_update + \
                               tok_sql_kvp_list.setResultsName("dup_list") 
                           ) + \
                  Optional(tok_sql_literal_semicol))

sql_update = (tok_sql_literal_update.setResultsName("op") + \
                  Optional(tok_sql_literal_ignore) + \
                  tok_sql_identifier.setResultsName("table") + \
                  tok_sql_literal_set + \
                  tok_sql_kvp_list.setResultsName("upd_list") + \
                  Optional(tok_sql_literal_where + \
                               tok_sql_kvp_list.setResultsName("where_list")
                           ) + \
                  Optional(tok_sql_literal_order + \
                               tok_sql_literal_by + \
                               tok_sql_cols.setResultsName("order_list")
                           ) + \
                  Optional(tok_sql_literal_limit + \
                               tok_sql_identifier) + \
                  Optional(tok_sql_literal_semicol))


sql_delete = tok_sql_literal_delete.setResultsName("op") + restOfLine
sql_update = tok_sql_literal_update.setResultsName("op") + restOfLine
sql_begin = tok_sql_literal_begin.setResultsName("op") + restOfLine
sql_use = tok_sql_literal_use.setResultsName("op") + restOfLine
sql_set = tok_sql_literal_set.setResultsName("op") + restOfLine
sql_commit = tok_sql_literal_commit.setResultsName("op") + restOfLine
sql_rollback = tok_sql_literal_rollback.setResultsName("op") + restOfLine
sql_comment = tok_sql_comment.setResultsName("op") + restOfLine

statements = [sql_insert, sql_update, sql_delete, sql_begin, sql_use,
              sql_commit, sql_rollback, sql_comment, sql_set, sql_select]
sql_statement = Or(statements)

def parse(sql):
    try:
        return sql_statement.parseString(sql)
    except ParseException:
        raise ValueError

# test function adapted from simpleSQL.py : Copyright (c) 2003, Paul McGuire
def test( str, op=sql_insert ):
    print str,"->"
    try:
        tokens = op.parseString( str )
        print "parsed.tokens = ", tokens
        for k,v in tokens.items():
            print "parsed.%s = %s" % (k, v)
    except ParseException, err:
        print " "*err.loc + "^\n" + err.msg
        print err
        print
        sys.exit(0)

def testcases():
    test("INSERT INTO rsvp (rsvp_id,member_id,event_id,chapter_id,topic_key,guests,response,comments,mtime,ctime,reminder,reminder_when) VALUES (null, 1755737,11610144, 1454408, '', 0, 3, '', '2009-11-06 14:12:08', '2009-11-06 14:12:08', 1, 0)")
    test("""INSERT INTO email_track (email_code,sent_count,open_count,email_date,ctime) VALUES ("!rn1Im",1,0,'2009-11-06','2009-11-06 14:12:09') ON DUPLICATE KEY UPDATE sent_count = sent_count + 1, mtime = mtime;""")
    test('insert into ne_api_log_old values ( null, null, 1, 1, null, null, "" )')
    test(r"INSERT INTO shout (shout_id,shouter_member_id,shoutee_member_id,chapter_id,status,comments,mtime,ctime) VALUES (null, 9487679, 11058103, 1437529, 1, 'jksfhksdafgsdjahfg', '2009-12-22 10:42:03', '2009-12-22 10:42:03');")
    test("INSERT INTO list_email (list_email_id,list_email_body_id,chapter_id,member_id,status,encoding,subject,announce,had_attachment,is_archived,recip_count,mtime,ctime) VALUES (null, 200240, 387918, 9408778, 2, 'text/html; charset=\"Windows-1252\"', 'sdafhsadkfh', 0, 0, 1, 0, '2009-12-22 10:42:03', '2009-12-22 10:42:03');")
    test("""INSERT INTO email_track (email_code,sent_count,open_count,email_date,ctime) VALUES ('ms1',1,0,'2009-12-22','2009-12-22 10:41:22') ON DUPLICATE KEY UPDATE sent_count = sent_count + 1, mtime = '2009-12-22 10:41:22';""")
    test(r"""select * from chapter c""", sql_select)
    test(r"""select * from chapter c, member b, order as order""", sql_select)
    test(r"""select member_id, org_id, status from chapter c, member b, order as order""", sql_select)
    test(r"""select member_id, org_id, status from chapter c, member b, order as order group by something""", sql_select)
    test(r"""select member_id, org_id, status from chapter c, member b, order as order group by something, else""", sql_select)
    test(r"""select member_id, org_id, status from chapter c, member b, order as order group by something, else having 1>0""", sql_select)
    test(r"""select * from a, (select * from b) as b, c""", sql_select)
    test(r"""select * from a, (select * from b) as b, c""", sql_select)
    test(r"SELECT /*!40000 SQL_CACHE */ devid, hostid, mb_total, mb_used, mb_asof, status, weight FROM device;", sql_select)
    test(r"SELECT COUNT(*) FROM event WHERE chapter_id = '1332599' AND status = '-3' AND rsvp = '1';", sql_select)
    test(r"SELECT DISTINCT(chapter.chapter_id),chapter.parent_chapter_id,chapter.transitional_venue_id,chapter.relocated_zip,chapter.name,chapter.short_desc,chapter.logo,chapter.status,chapter.coordinator_member_id,chapter.org_starttime,chapter.org_deadbeat_time,chapter.founder_member_id,chapter.who,chapter.event_photo_id,chapter.rating,chapter.rating_count,chapter.fee_currency,chapter.fee_label,chapter.fee,chapter.fee_desc,chapter.topic_id,chapter.board_id,chapter.primary_meetupday_id,chapter.member_count,chapter.country,chapter.state,chapter.city,chapter.zip,chapter.lat,chapter.lon,chapter.timezone,chapter.meetupday_opt_out,chapter.mtime,chapter.ctime,chapter.number,chapter.potential_coordinator_member_id,chapter.notes,chapter.notify_new_member,chapter.join_mode,chapter.inv_code,chapter.content_private,chapter.welcome_blurb,chapter.grace_start,chapter.frozen_start,chapter.create_pages,chapter.boards_mode,chapter.mailing_list_mode,chapter.mailing_list_accepted,chapter.tip_display_status,chapter.rc_mask,chapter.mp_notify,chapter.headline,chapter.urlname,chapter.domain,chapter.listaddress, (3963 * acos(sin('43.13'/57.2958) * sin(chapter.lat/57.2958) + cos('43.13'/57.2958) * cos(chapter.lat/57.2958) * cos(chapter.lon/57.2958 - ('-77.6'/57.2958)))) AS distance FROM chapter JOIN chapter_topic ct ON (chapter.chapter_id = ct.chapter_id) WHERE chapter.status IN ('1','2') AND chapter.lat >= '42.40641' AND chapter.lon >= '-78.59143' AND chapter.lat <= '43.853592' AND chapter.lon <= '-76.60857' AND ct.topic_id in ('1091')  AND chapter.ctime >= '2010-01-01 11:00:00.0'  AND chapter.ctime < '2010-01-08 11:00:00.0'  AND chapter.content_private != '2'  HAVING distance <= '50.0';", sql_select)

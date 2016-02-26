#! /usr/bin/env python3
import cgi
import cgitb
import os
import sys
import glob
import urllib
import edn_format

cpath = os.getenv("SCRIPT_NAME")

cgitb.enable()

form = cgi.FieldStorage()

base = '/home/kena/jepsen/cockroachdb/store'
os.chdir(base)
path = ''
if 'path' in form:
    path = form.getvalue('path')

if len(path) == 0 or path[0] == '/':
    path = '.'

def sorted_ls(path):
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    return list(sorted(os.listdir(path), key=mtime))  

self_ext = ['edn', 'stderr']

if path.split('.')[-1] in self_ext:
    print("Content-Type: text/plain;charset=utf-8\n")

    with open(path) as f:
        contents = f.read().replace(r'\n','\n')
        print(contents)


elif 'grep-err' in form:
    print("Content-Type: text/plain;charset=utf-8\n")

    with open(path) as f:
        for l in f:
            if 'ERROR' in l:
                print(l)


elif 'version-details' in form:
    import re
    urlver = re.compile(r'^\s*(\S+)\s+(\S+)\s*$')
    print("Content-Type: text/html;charset=utf-8\n")
    print("""<!DOCTYPE html><html lang=en><body><pre>""")
    with open(path) as f:
        for l in f:
            l = l.rstrip()
            m = urlver.match(l)
            if m is None or "/" not in l:
                print(l)
            else:
                print("<a target='_blank' href='http://" + m.group(1) + "/tree/" + m.group(2) + "'>" + l + "</a>", end='')
                if 'cockroachdb/cockroach' in l: print("<strong>&lt;--- here</strong>", end='')
                print()
    print("""</pre></body></html>""")

elif path == '.':
    
    print("Content-Type: text/html;charset=utf-8\n")
    print("""<!DOCTYPE html>
    <html lang=en>
    <head>
    <title>CockroachDB Jepsen test results</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!-- Latest compiled and minified CSS -->
    <link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css">
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.0/jquery.min.js"></script>
    <script src="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js"></script></head>
    <body><div class="container-fluid">
    <h1>CockroachDB Jepsen test results</h1>
    <p class=lead>Welcome</p>
    <p class='alert alert-info'>For test descriptions, refer to <a href="https://github.com/cockroachdb/jepsen/blob/master/cockroachdb/README.rst">the README file</a>.
    </br>
    <strong>Note</strong><br/>These tests are run irregularly.<br/>
    They may not use the latest code from the main CockroachDB repository.</br>
    Some tests are run using unmerged change sets.<br/>
    <strong>Always refer to the Version column to place test results in context.</strong>
    </p>
    <style type=text/css>
    .table > tbody > tr > td { vertical-align: middle !important; }
    :target > * { background-color: yellow !important; }
    </style>
    <table class="table table-striped table-hover table-responsive"><thead><tr>
    <th></th>
    <th>Timestamp</th>
    <th>Type</th>
    <th>Events</th>
    <th>Errors</th>
    <th>Latencies</th>
    <th>Rates</th>
    <th>Version</th>
    <th>Details</th>
    <th>Node logs</th>
    </tr></thead><tbody>""")
    rl = sorted((x.split('/') for x in glob.glob('*/20*/results.edn')), key=lambda r:r[1],reverse=True)
    lastver = None
    first = True
    for d in rl:
        dpath = os.path.join(d[0],d[1])
        ednpath = os.path.join(dpath, 'results.edn')
        with open(ednpath) as f:
            r = edn_format.loads(f.read())
            if r is not None:
                ok = r[edn_format.Keyword('valid?')]
                status = {True:'success',False:'danger'}[ok]

                thisver = None
                dv = sorted(glob.glob(os.path.join(dpath, '*/version.txt')))
                if len(dv) > 0:
                    v = dv[0]
                    n = v.split('/')[-2]
                    with open(v) as vf:
                        thisver = vf.read().split('\n')[1].split(':')[1].strip()
                ts = d[1][:-5]

                if not first and thisver != lastver:
                    print("<tr class='info'><td colspan=10 class='text-center small'><strong>New CockroachDB version</strong></td></tr>")
                first=False
                lastver = thisver
                print("<tr class='%s' id='%s'>" % (status, ts))
                # Anchor
                print("<td>")
                print("<a href='#" + ts + "' class='btn btn-info'>#</a></td>")
                # Timestamp
                print("<td><a href='" + cpath + "?path=" + urllib.parse.quote_plus(dpath) + "' class='btn btn-%s'>" % status + ts +
                      ' <span class="glyphicon glyphicon-info-sign"></span>'
                      "</a></td>")
                # Type
                print("<td>" + d[0].split('-', 1)[1] + "</td>")
                # History
                print("<td>")
                hfile = os.path.join(dpath, 'history.txt')
                errs = 0
                if os.path.exists(hfile):
                    print("<a href='/" + hfile + "' class='btn btn-%s'>" % status)
                    with open(hfile) as h:
                        lines = h.read().split('\n')
                        print(len(lines), '<span class="glyphicon glyphicon-info-sign"></span>')
                        errs = sum((1 for x in lines if 'ERROR' in x))
                    print("</a>")
                print("</td>")
                # Errors
                print("<td>")
                if errs != 0:
                    print("<a href='" + cpath + "?grep-err=1&path=" + urllib.parse.quote_plus(hfile) + "' class='btn btn-%s'>" % status +
                          str(errs) + ' <span class="glyphicon glyphicon-info-sign"></span></a>')
                print("</td>")
                # Latencies
                print("<td>")
                lfile = os.path.join(dpath, "latency-raw.png")
                if os.path.exists(lfile):
                    print("<a href='/" + lfile + "'>" 
                          "<img height=60px src='/" + lfile + "' />"
                          "</a>")
                print("</td>")
                # Rates
                print("<td>")
                rfile = os.path.join(dpath, "rate.png")
                if os.path.exists(rfile):
                    print("<a href='/" + rfile + "'>" 
                          "<img height=60px src='/" + rfile + "' />"
                          "</a>")
                print("</td>")
                # Version
                print("<td>")
                dv = sorted(glob.glob(os.path.join(dpath, '*/version.txt')))
                if len(dv) > 0:
                    v = dv[0]
                    n = v.split('/')[-2]
                    with open(v) as vf:
                        vn = vf.read().split('\n')[1].split(':')[1].strip()
                        print("<a href='" + cpath + "?version-details=1&path=" + urllib.parse.quote_plus(v) + "' class='btn btn-%s btn-xs'>" % status + vn + 
                              " <span class='glyphicon glyphicon-info-sign'></span></a>")
                print("</td>")
                # Details
                print("<td>")
                dtk = edn_format.Keyword('details')
                if dtk not in r:
                    dtk = edn_format.Keyword('error')
                if dtk in r:
                    dstr = edn_format.dumps(r[dtk])
                    if len(dstr) > 60:
                        dstr = dstr[:60] + "... <a href='" + cpath + "?path=" + urllib.parse.quote_plus(ednpath) + "'>(more)</a>"
                    print("<tt class='small'>" + dstr + "</tt>")
                print("</td>")

                # Node logs
                print("<td>")
                logs = sorted(glob.glob(os.path.join(dpath, "*/cockroach.stderr")))
                if len(logs) > 0:
                    for log in logs:
                        print("<a href='" + cpath + "?path=" + urllib.parse.quote_plus(log) + "' class='btn btn-%s btn-xs'>" % status +
                              "<span class='glyphicon glyphicon-info-sign'></span></a>")
                print("</td>")
                
                print("</tr>")
    print("</tbody></table>")
    
    print("</div></body></html>")

elif os.path.isdir(path):
    print("Content-Type: text/html;charset=utf-8\n")
    print("""<!DOCTYPE html><html lang=en><body><ul>""")
    print("<ul>")
    print("<li><a href='" + cpath + "?path=" + urllib.parse.quote_plus('/'.join(path.split('/')[:-1])) + "'>Up one level</a>")
    for d in sorted_ls(path):
        if d == "cgi-bin":
            continue
        
        ditem = os.path.join(path, d)
        if os.path.isdir(ditem) or d.split('.')[-1] in self_ext:
            dst = cpath + "?path=" + urllib.parse.quote_plus(ditem)
        else:
            dst = "/" + ditem
        print("<li><a href='" + dst + "'>" + d + "</a></li>")
    print("</ul></body></html>")

else:
    print("Content-Type: text/plain;charset=utf-8\n")
    print("You shouldn't be here.")

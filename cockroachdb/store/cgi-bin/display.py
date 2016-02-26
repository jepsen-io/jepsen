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

base = os.getcwd()
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
        print(f.read().replace(r'\n','\n'))

    sys.exit(0)

print("Content-Type: text/html;charset=utf-8\n")
print("""<!DOCTYPE html>
<html>
<head>
<title>CockroachDB Jepsen test results</title>
</head>
<body>""")

if path == '.':
    print("""
    <style type='text/css'>
    .success { background-color: green; }
    .failure { background-color: red; }
    th { text-align: left; }
    </style>
    """)
    
    print("""<table><thead><tr>
    <th>Timestamp</th>
    <th>Type</th>
    <th>Status</th>
    <th>History</th>
    <th>Latencies</th>
    <th>Rates</th>
    <th>Version</th>
    <th>Details</th>
    </tr></thead><tbody>""")
    rl = sorted((x.split('/') for x in glob.glob('*/20*/results.edn')), key=lambda r:r[1],reverse=True)
    for d in rl:
        dpath = os.path.join(d[0],d[1])
        ednpath = os.path.join(dpath, 'results.edn')
        with open(ednpath) as f:
            r = edn_format.loads(f.read())
            if r is not None:
                print("<tr>")
                # Timestamp
                print("<td><a href='" + cpath + "?path=" + urllib.parse.quote_plus(dpath) + "'>" + d[1][:-5] + "</a></td>")
                # Type
                print("<td>" + d[0].split('-', 1)[1] + "</td>")
                # Status
                ok = r[edn_format.Keyword('valid?')]
                status = {True:'success',False:'failure'}[ok]
                print("<td class=" + status + ">" + status + "</td>")
                # History
                print("<td>")
                hfile = os.path.join(dpath, 'history.txt')
                if os.path.exists(hfile):
                    print("<a href='/" + hfile + "'>")
                    with open(hfile) as h:
                        print(len(h.read().split('\n')), " events")
                    print("</a>")
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
                        print("<a href='/" + v + "'>" + vn + "</a>")
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
                    print("<tt>" + dstr + "</tt>")
                print("</td>")
                print("</tr>")
    print("</tbody></table>")
    
elif os.path.isdir(path):
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
    print("</ul>")
    
print("</body></html>")

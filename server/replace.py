import functools

style='<link rel="stylesheet" type="text/css" href="/static/style.min.css">'
css='<style type="text/css"> \n' + open("static/style.css").read() + '\n </style>'
custom='<script type="text/javascript" src="/static/custom.js"></script>'
js='<script type="text/javascript"> \n' + open("static/custom.js").read() + '\n </script>'
file='templates/index.html'
newfile='templates/indexfull.html'

with open("indexfull.html", 'w+') as new_file:
        with open("index.html") as old_file:
            for line in old_file:
                repls = (style, css), (custom, js)
                new_file.write(functools.reduce(lambda a, kv: a.replace(*kv), repls, line))
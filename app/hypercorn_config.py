# hypercorn_config.py
bind = "0.0.0.0:5000"
workers = 1
accesslog = "-"
errorlog = "-"
keep_alive_timeout = 300
graceful_timeout = 300

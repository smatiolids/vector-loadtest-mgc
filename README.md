# Loader

Copy `_env_sample` file to `.env`. Edit the variables as:

```
MODE="dse"
DSE_NODES="<Comma separated ips>"
DSE_USER=""
DSE_PASS=""
```

Edit load.py file. 

Change the line 53 with the appropriate file path.

```
python3 loader.py
```

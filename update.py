from datetime import datetime, timezone


if __name__ == "__main__":
    lines = open("version.py", "r").readlines()
    lines[0] = "GENERATED_AT = \"" + str(datetime.now(timezone.utc).isoformat()) + "\""
    version_last = str(".".join(lines[1].split(" = \"")[1].split("\"")[0].split(".")[:-1]))
    lines[1] = "VERSION = \"" + version_last + "." + str((int(lines[1].split(" = \"")[1].split("\"")[0].split(".")[-1]) + 1)) + '"'
    open("version.py", "w").write(lines[0] + "\n" + lines[1] + "\n\n\n" + lines[4] + lines[5])
